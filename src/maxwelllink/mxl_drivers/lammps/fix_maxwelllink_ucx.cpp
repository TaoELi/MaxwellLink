/* ----------------------------------------------------------------------
   FixMaxwellLinkUCX: UCXX-backed client that:
   - modifies dt based on MaxwellLink INIT message,
   - receives E-field [Ex,Ey,Ez] in atomic units from a MaxwellLink SocketHubUCX,
   - applies F = qE to atoms, and
   - sends back d(mu)/dt = sum_i q_i v_i and mu (all atomic units) each step.
   --------------------------------------------------------------------- */

#include "fix_maxwelllink_ucx.h"

#include "atom.h"
#include "comm.h"
#include "compute.h"
#include "domain.h"
#include "error.h"
#include "force.h"
#include "group.h"
#include "input.h"
#include "integrate.h"
#include "memory.h"
#include "modify.h"
#include "neighbor.h"
#include "output.h"
#include "pair.h"
#include "respa.h"
#include "update.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <thread>

#ifndef _WIN32
  #include <sys/types.h>
  #include <unistd.h>
#else
  #error "FixMaxwellLinkUCX requires a POSIX-like (UNIX) environment"
#endif

#if __has_include(<ucxx/api.h>)
  #include <ucxx/api.h>
  #include <ucxx/context.h>
  #include <ucxx/endpoint.h>
  #include <ucxx/request.h>
  #include <ucxx/worker.h>
  #define MAXWELLLINK_HAVE_UCXX 1
#else
  #define MAXWELLLINK_HAVE_UCXX 0
#endif

using namespace LAMMPS_NS;
using namespace FixConst;

static constexpr char MXLU_MAGIC[4] = {'M', 'X', 'L', 'U'};
static constexpr unsigned short MXLU_VERSION = 1;

enum MXLUCXOpcode : unsigned short {
  OP_HELLO = 1,
  OP_INIT = 2,
  OP_STEP_REQUEST = 3,
  OP_STEP_RESPONSE = 4,
  OP_STOP = 5,
  OP_BYE = 6,
};

static void append_u16_le(std::vector<char> &out, unsigned short value)
{
  out.push_back(static_cast<char>(value & 0xffu));
  out.push_back(static_cast<char>((value >> 8) & 0xffu));
}

static void append_u32_le(std::vector<char> &out, unsigned int value)
{
  out.push_back(static_cast<char>(value & 0xffu));
  out.push_back(static_cast<char>((value >> 8) & 0xffu));
  out.push_back(static_cast<char>((value >> 16) & 0xffu));
  out.push_back(static_cast<char>((value >> 24) & 0xffu));
}

static void append_i32_le(std::vector<char> &out, int value)
{
  append_u32_le(out, static_cast<unsigned int>(value));
}

static void append_f64_le(std::vector<char> &out, double value)
{
  const auto *ptr = reinterpret_cast<const char *>(&value);
  out.insert(out.end(), ptr, ptr + sizeof(double));
}

static unsigned short read_u16_le(const char *ptr)
{
  return static_cast<unsigned short>(
      static_cast<unsigned char>(ptr[0]) |
      (static_cast<unsigned short>(static_cast<unsigned char>(ptr[1])) << 8));
}

static unsigned int read_u32_le(const char *ptr)
{
  return static_cast<unsigned int>(static_cast<unsigned char>(ptr[0])) |
         (static_cast<unsigned int>(static_cast<unsigned char>(ptr[1])) << 8) |
         (static_cast<unsigned int>(static_cast<unsigned char>(ptr[2])) << 16) |
         (static_cast<unsigned int>(static_cast<unsigned char>(ptr[3])) << 24);
}

static int read_i32_le(const char *ptr)
{
  return static_cast<int>(read_u32_le(ptr));
}

static double read_f64_le(const char *ptr)
{
  double value = 0.0;
  std::memcpy(&value, ptr, sizeof(double));
  return value;
}

static std::string escape_json_string(const std::string &in)
{
  std::string out;
  out.reserve(in.size() + 8);
  for (char c : in) {
    switch (c) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}

static std::vector<char> pack_ucx_message(unsigned short opcode,
                                          const std::vector<char> &payload)
{
  std::vector<char> out;
  out.reserve(12 + payload.size());
  out.insert(out.end(), MXLU_MAGIC, MXLU_MAGIC + 4);
  append_u16_le(out, MXLU_VERSION);
  append_u16_le(out, opcode);
  append_u32_le(out, static_cast<unsigned int>(payload.size()));
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

static bool unpack_ucx_message(const std::vector<char> &blob, unsigned short &opcode,
                               std::vector<char> &payload, std::string &err)
{
  if (blob.size() < 12) {
    err = "UCX message too short";
    return false;
  }
  if (std::memcmp(blob.data(), MXLU_MAGIC, 4) != 0) {
    err = "Invalid UCX message magic";
    return false;
  }
  const auto version = read_u16_le(blob.data() + 4);
  if (version != MXLU_VERSION) {
    err = "Unsupported UCX message version";
    return false;
  }
  opcode = read_u16_le(blob.data() + 6);
  const auto payload_len = read_u32_le(blob.data() + 8);
  if (blob.size() != 12 + payload_len) {
    err = "UCX message payload length mismatch";
    return false;
  }
  payload.assign(blob.begin() + 12, blob.end());
  return true;
}

static bool parse_dt_au_from_json(const char *s, int n, double &out_dt_au)
{
  const char *p = s;
  const char *end = s + n;
  while (p < end) {
    if (*p == 'd' && (end - p) >= 5 && std::strncmp(p, "dt_au", 5) == 0) {
      const char *q = p + 5;
      while (q < end && *q != ':')
        ++q;
      if (q == end)
        break;
      ++q;
      while (q < end && (std::isspace((unsigned char)*q) || *q == '"'))
        ++q;
      char *r = nullptr;
      const double v = std::strtod(q, &r);
      if (r && r != q) {
        out_dt_au = v;
        return true;
      }
    }
    ++p;
  }
  return false;
}

/* ---------------------------------------------------------------------- */

FixMaxwellLinkUCX::FixMaxwellLinkUCX(LAMMPS *lmp, int narg, char **arg)
    : Fix(lmp, narg, arg), host(nullptr), port(0), master(0), initialized(0),
      have_field(0), hello_sent(0), molid(-1), bsize(0), ex_fac(0.0),
      ey_fac(0.0), ez_fac(0.0), Eau_x(0.0), Eau_y(0.0), Eau_z(0.0), qe2f(0.0),
      v_to_au(0.0), x_to_au(0.0), efield_au_native(0.0)
{
  if (narg < 5)
    utils::missing_cmd_args(FLERR, "fix MaxwellLinkUCX", error);

  if (atom->tag_enable == 0)
    error->all(FLERR, "Fix MaxwellLinkUCX requires atom IDs");
  if (strcmp(update->unit_style, "lj") == 0)
    error->all(FLERR, "Fix MaxwellLinkUCX does not support 'units lj'");

  host = utils::strdup(arg[3]);
  port = utils::inumeric(FLERR, arg[4], false, lmp);
  if ((port <= 0) || (port > 65535))
    error->all(FLERR, "Invalid port for fix MaxwellLinkUCX: {}", port);

  int iarg = 5;
  while (iarg < narg) {
    if (strcmp(arg[iarg], "reset_dipole") == 0) {
      reset_dipole = 1;
      ++iarg;
      printf("[MaxwellLink UCX] Will reset initial permanent dipole to zero.\n");
    } else {
      error->all(FLERR, "Unknown fix MaxwellLinkUCX keyword: {}", arg[iarg]);
    }
  }

  master = (comm->me == 0) ? 1 : 0;
  bsize = 0;

  respa_level_support = 1;
  ilevel_respa = 0;
  last_field_timestep = -1;

  neighbor->delay = 0;
  neighbor->every = 1;
}

/* ---------------------------------------------------------------------- */

FixMaxwellLinkUCX::~FixMaxwellLinkUCX()
{
  if (host)
    free(host);
  close_ucx();
}

/* ---------------------------------------------------------------------- */

int FixMaxwellLinkUCX::setmask()
{
  int mask = 0;
  mask |= INITIAL_INTEGRATE;
  mask |= POST_FORCE;
  mask |= POST_FORCE_RESPA;
  mask |= MIN_POST_FORCE;
  mask |= END_OF_STEP;
  return mask;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::init()
{
  qflag = atom->q_flag ? 1 : 0;
  if (!qflag)
    error->all(FLERR,
               "Fix MaxwellLinkUCX requires per-atom charge 'q' (no q_flag)");

  qe2f = force->qe2f;
  a0_native = 0.529177210544 * force->angstrom;
  timeau_native = 0.024188843265864 * force->femtosecond;
  Eh_native = force->qqr2e * force->qelectron * force->qelectron / a0_native;
  v_to_au = 1.0 / (a0_native / timeau_native);
  x_to_au = 1.0 / a0_native;
  efield_au_native = Eh_native / (force->qelectron * a0_native);

  if (master)
    open_ucx();

  if (utils::strmatch(update->integrate_style, "^respa")) {
    auto *r = dynamic_cast<Respa *>(update->integrate);
    if (r) {
      ilevel_respa = r->nlevels - 1;
      if (respa_level >= 0)
        ilevel_respa = MIN(respa_level, ilevel_respa);
    }
  }

  modify->compute[modify->find_compute("thermo_pe")]->invoked_scalar = -1;
  modify->addstep_compute_all(update->ntimestep + 1);
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::wait_request(const std::shared_ptr<ucxx::Request> &request)
{
#if MAXWELLLINK_HAVE_UCXX
  if (!request)
    return;
  while (!request->isCompleted()) {
    ucxx_worker->progress();
  }
  request->checkError();
#else
  (void)request;
#endif
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::open_ucx()
{
  if (!master || ucxx_endpoint)
    return;

#if !MAXWELLLINK_HAVE_UCXX
  error->all(
      FLERR,
      "Fix MaxwellLinkUCX requires libucxx headers/libraries at build time");
#else
  try {
    ucxx_context =
        ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);
    ucxx_worker = ucxx_context->createWorker(false, false);
    ucxx_endpoint =
        ucxx_worker->createEndpointFromHostname(std::string(host), port, true);
    hello_sent = 0;
    initialized = 0;
    send_hello();
  } catch (const std::exception &e) {
    close_ucx();
    error->one(FLERR, "Fix MaxwellLinkUCX: failed to connect to {}:{} ({})",
               host, port, e.what());
  }
#endif
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::close_ucx()
{
#if MAXWELLLINK_HAVE_UCXX
  if (ucxx_endpoint) {
    try {
      ucxx_endpoint->closeBlocking();
    } catch (...) {
    }
    ucxx_endpoint.reset();
  }
  ucxx_worker.reset();
  ucxx_context.reset();
#endif
  initialized = 0;
  hello_sent = 0;
}

/* ---------------------------------------------------------------------- */

bool FixMaxwellLinkUCX::send_ucx_message(unsigned short opcode,
                                         const std::vector<char> &payload)
{
#if !MAXWELLLINK_HAVE_UCXX
  (void)opcode;
  (void)payload;
  return false;
#else
  if (!ucxx_endpoint)
    return false;

  try {
    const auto message = pack_ucx_message(opcode, payload);
    auto request =
        ucxx_endpoint->amSend(message.data(), message.size(), UCS_MEMORY_TYPE_HOST);
    wait_request(request);
    return true;
  } catch (...) {
    close_ucx();
    return false;
  }
#endif
}

/* ---------------------------------------------------------------------- */

bool FixMaxwellLinkUCX::recv_ucx_message(unsigned short &opcode,
                                         std::vector<char> &payload)
{
#if !MAXWELLLINK_HAVE_UCXX
  (void)opcode;
  (void)payload;
  return false;
#else
  if (!ucxx_endpoint)
    return false;

  try {
    auto request = ucxx_endpoint->amRecv(false, nullptr, nullptr);
    wait_request(request);
    auto buffer = request->getRecvBuffer();
    if (!buffer)
      error->one(FLERR, "Fix MaxwellLinkUCX: received empty UCX payload");

    std::vector<char> blob(buffer->getSize());
    std::memcpy(blob.data(), buffer->data(), blob.size());

    std::string err;
    if (!unpack_ucx_message(blob, opcode, payload, err))
      error->one(FLERR, "Fix MaxwellLinkUCX: {}", err);
    return true;
  } catch (...) {
    close_ucx();
    return false;
  }
#endif
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::send_hello()
{
  if (!master || hello_sent || !ucxx_endpoint)
    return;

  char hostname_buf[256];
  hostname_buf[0] = '\0';
  gethostname(hostname_buf, sizeof(hostname_buf) - 1);
  hostname_buf[sizeof(hostname_buf) - 1] = '\0';

  std::ostringstream ss;
  ss << '{'
     << "\"driver\":\"lammps\","
     << "\"hostname\":\"" << escape_json_string(std::string(hostname_buf)) << "\","
     << "\"pid\":" << static_cast<long long>(getpid()) << ','
     << "\"transport\":\"ucx\""
     << '}';

  const auto hello = ss.str();
  std::vector<char> payload(hello.begin(), hello.end());
  if (!send_ucx_message(OP_HELLO, payload))
    error->one(FLERR, "Fix MaxwellLinkUCX: failed to send HELLO");
  hello_sent = 1;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::send_bye()
{
  std::vector<char> payload;
  send_ucx_message(OP_BYE, payload);
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::handshake_if_needed()
{
  if (!master || initialized)
    return;

  while (!initialized) {
    unsigned short opcode = 0;
    std::vector<char> payload;
    if (!recv_ucx_message(opcode, payload))
      error->one(FLERR,
                 "Fix MaxwellLinkUCX: connection closed during INIT handshake");

    if (opcode == OP_INIT) {
      if (payload.size() < 4)
        error->one(FLERR, "Fix MaxwellLinkUCX: INIT payload too short");

      molid = read_i32_le(payload.data());
      if (comm->me == 0)
        printf("[MaxwellLink UCX] Assigned a molecular ID: %d\n", molid);

      dt_au_recv = 0.0;
      if (payload.size() > 4)
        parse_dt_au_from_json(payload.data() + 4, payload.size() - 4, dt_au_recv);

      if (dt_au_recv > 0.0)
        dt_native_recv = dt_au_recv * timeau_native;

      initialized = 1;
      return;
    }

    if (opcode == OP_STOP) {
      send_bye();
      stop_requested = true;
      close_ucx();
      return;
    }

    error->one(FLERR,
               "Fix MaxwellLinkUCX: unexpected opcode {} during INIT handshake",
               static_cast<int>(opcode));
  }
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::broadcast_dt()
{
  double dtbuf = dt_native_recv;
  MPI_Bcast(&dtbuf, 1, MPI_DOUBLE, 0, world);
  dt_native_recv = dtbuf;

  const double prior = update->dt;
  if (dt_native_recv > 0.0 && fabs(update->dt - dt_native_recv) > 1e-10) {
    update->update_time();
    update->dt = dt_native_recv;
    update->dt_default = 0;
    update->integrate->reset_dt();
    if (force->pair)
      force->pair->reset_dt();
    for (auto &ifix : modify->get_fix_list())
      ifix->reset_dt();
    output->reset_dt();

    if (comm->me == 0) {
      printf(
          "[MaxwellLink UCX] 1 atomic units time in LAMMPS native time units = %.15g\n",
          timeau_native);
      printf(
          "[MaxwellLink UCX] MaxwellLink uses time step: dt_au = %.15g -> dt_native = %.15g\n",
          dt_au_recv, dt_native_recv);
      printf(
          "[MaxwellLink UCX] Modified LAMMPS time step from %.15g to %.15g.\n",
          prior, update->dt);
    }
  }

  dt_synced = 1;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::recv_efield_from_payload(const std::vector<char> &payload)
{
  if (payload.size() != 3 * sizeof(double))
    error->one(FLERR,
               "Fix MaxwellLinkUCX: STEP_REQUEST payload must be 24 bytes");

  Eau_x = read_f64_le(payload.data());
  Eau_y = read_f64_le(payload.data() + sizeof(double));
  Eau_z = read_f64_le(payload.data() + 2 * sizeof(double));
  have_field = 1;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::send_amp_vector(const std::string &extra)
{
  std::vector<char> payload;
  payload.reserve(3 * sizeof(double) + 4 + extra.size());
  append_f64_le(payload, dmu_dt_global[0]);
  append_f64_le(payload, dmu_dt_global[1]);
  append_f64_le(payload, dmu_dt_global[2]);
  append_i32_le(payload, static_cast<int>(extra.size()));
  payload.insert(payload.end(), extra.begin(), extra.end());

  if (!send_ucx_message(OP_STEP_RESPONSE, payload))
    error->one(
        FLERR,
        "Fix MaxwellLinkUCX: failed to send STEP_RESPONSE after propagation");
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::initial_integrate(int /*vflag*/)
{
  if (master) {
    if (!ucxx_endpoint)
      open_ucx();
    handshake_if_needed();
  }

  int stop_flag = stop_requested ? 1 : 0;
  MPI_Bcast(&stop_flag, 1, MPI_INT, 0, world);
  stop_requested = (stop_flag != 0);
  if (stop_requested) {
    if (comm->me == 0)
      printf("[MaxwellLink UCX] Server requested stop, exiting gracefully...\n");
    lmp->input->one("quit 0");
    return;
  }

  if (dt_synced == 0)
    broadcast_dt();

  if (prcompute_dipole == 0) {
    double ke_au = 0.0;
    double tempK = -1.0;
    calc_initial_dipole_info(mu_global, dmu_dt_global, ke_au, tempK);

    for (size_t i = 0; i < 3; ++i) {
      if (reset_dipole) {
        mu_global_initial[i] = mu_global[i];
        mu_global[i] -= mu_global_initial[i];
      }
      mu_global_prev[i] = mu_global[i];
      dmu_dt_global_prev[i] = dmu_dt_global[i];
    }
    prcompute_dipole = 1;
  }

  if (master) {
    while (true) {
      unsigned short opcode = 0;
      std::vector<char> payload;
      if (!recv_ucx_message(opcode, payload))
        error->one(FLERR,
                   "Fix MaxwellLinkUCX: connection closed while waiting for STEP_REQUEST");

      if (opcode == OP_STEP_REQUEST) {
        recv_efield_from_payload(payload);
        break;
      }
      if (opcode == OP_STOP) {
        send_bye();
        stop_requested = true;
        close_ucx();
        break;
      }
      if (opcode == OP_INIT) {
        initialized = 0;
        dt_synced = 0;
        error->one(FLERR,
                   "Fix MaxwellLinkUCX: unexpected INIT during step loop");
      }
    }
  }

  stop_flag = stop_requested ? 1 : 0;
  MPI_Bcast(&stop_flag, 1, MPI_INT, 0, world);
  stop_requested = (stop_flag != 0);
  if (stop_requested) {
    if (comm->me == 0)
      printf("[MaxwellLink UCX] Server requested stop, exiting gracefully...\n");
    lmp->input->one("quit 0");
    return;
  }

  double ebuf[3] = {0.0, 0.0, 0.0};
  if (master) {
    ebuf[0] = Eau_x;
    ebuf[1] = Eau_y;
    ebuf[2] = Eau_z;
  }
  MPI_Bcast(ebuf, 3, MPI_DOUBLE, 0, world);

  Eau_x = ebuf[0];
  Eau_y = ebuf[1];
  Eau_z = ebuf[2];

  ex_fac = efield_au_native * Eau_x;
  ey_fac = efield_au_native * Eau_y;
  ez_fac = efield_au_native * Eau_z;

  modify->compute[modify->find_compute("thermo_pe")]->invoked_scalar = -1;
  modify->addstep_compute_all(update->ntimestep + 1);

  have_field = 1;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::post_force(int vflag)
{
  if (!have_field)
    return;
  (void)vflag;

  double **f = atom->f;
  double *q = atom->q;
  int *mask = atom->mask;
  int nlocal = atom->nlocal;
  if (igroup == atom->firstgroup)
    nlocal = atom->nfirst;

  if (!atom->q_flag)
    error->all(FLERR,
               "Fix MaxwellLinkUCX requires per-atom charge 'q' (no q_flag)");

  for (int i = 0; i < nlocal; i++) {
    if (mask[i] & groupbit) {
      f[i][0] += q[i] * ex_fac;
      f[i][1] += q[i] * ey_fac;
      f[i][2] += q[i] * ez_fac;
    }
  }
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::setup(int vflag)
{
  if (utils::strmatch(update->integrate_style, "^respa")) {
    auto respa = dynamic_cast<Respa *>(update->integrate);
    respa->copy_flevel_f(ilevel_respa);
    post_force_respa(vflag, ilevel_respa, 0);
    respa->copy_f_flevel(ilevel_respa);
  } else {
    post_force(vflag);
  }
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::min_setup(int vflag) { post_force(vflag); }

void FixMaxwellLinkUCX::post_force_respa(int vflag, int ilevel, int /*iloop*/)
{
  if (ilevel == ilevel_respa)
    post_force(vflag);
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::min_post_force(int vflag) { post_force(vflag); }

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::calc_dipole_info(double *mu, double *dmu_dt,
                                         double &ke_au, double &tempK)
{
  double dmu_dt_local[3];
  double mu_local[3];
  double dmu_dt_global_tmp[3];
  double mu_global_tmp[3];

  dmu_dt_local[0] = dmu_dt_local[1] = dmu_dt_local[2] = 0.0;
  dmu_dt_global_tmp[0] = dmu_dt_global_tmp[1] = dmu_dt_global_tmp[2] = 0.0;
  mu_local[0] = mu_local[1] = mu_local[2] = 0.0;
  mu_global_tmp[0] = mu_global_tmp[1] = mu_global_tmp[2] = 0.0;

  double **v = atom->v;
  double **x = atom->x;
  double *q = atom->q;
  double **f = atom->f;
  int *mask = atom->mask;
  int nlocal = atom->nlocal;
  int *type = atom->type;
  double *rmass = atom->rmass;

  if (igroup == atom->firstgroup)
    nlocal = atom->nfirst;

  imageint *image = atom->image;
  double unwrap[3];

  double ke_local = 0.0, ke_global = 0.0;
  long ngrp_local = 0, ngrp_global = 0;
  double m, vv;

  double vhalf[3] = {0.0, 0.0, 0.0};
  double xhalf[3] = {0.0, 0.0, 0.0};

  const double dtv = 0.5 * update->dt;
  const double dtf = 0.5 * update->dt * force->ftm2v;

  for (int i = 0; i < nlocal; i++) {
    if (!(mask[i] & groupbit))
      continue;
    m = rmass ? rmass[i] : atom->mass[type[i]];
    const double dtfm = dtf / m;
    vhalf[0] = v[i][0] + dtfm * f[i][0];
    vhalf[1] = v[i][1] + dtfm * f[i][1];
    vhalf[2] = v[i][2] + dtfm * f[i][2];
    dmu_dt_local[0] += q[i] * (vhalf[0] * v_to_au);
    dmu_dt_local[1] += q[i] * (vhalf[1] * v_to_au);
    dmu_dt_local[2] += q[i] * (vhalf[2] * v_to_au);
    xhalf[0] = x[i][0] + dtv * vhalf[0];
    xhalf[1] = x[i][1] + dtv * vhalf[1];
    xhalf[2] = x[i][2] + dtv * vhalf[2];
    domain->unmap(xhalf, image[i], unwrap);
    mu_local[0] += q[i] * (unwrap[0] * x_to_au);
    mu_local[1] += q[i] * (unwrap[1] * x_to_au);
    mu_local[2] += q[i] * (unwrap[2] * x_to_au);

    vv = v[i][0] * v[i][0] + v[i][1] * v[i][1] + v[i][2] * v[i][2];
    ke_local += 0.5 * m * vv;
    ngrp_local++;
  }

  double allbuf_global[8];
  double allbuf[8] = {dmu_dt_local[0], dmu_dt_local[1], dmu_dt_local[2],
                      mu_local[0],     mu_local[1],     mu_local[2],
                      ke_local,        static_cast<double>(ngrp_local)};
  MPI_Allreduce(allbuf, allbuf_global, 8, MPI_DOUBLE, MPI_SUM, world);
  for (size_t i = 0; i < 3; ++i) {
    dmu_dt_global_tmp[i] = allbuf_global[i];
    mu_global_tmp[i] = allbuf_global[i + 3];
  }
  ke_global = allbuf_global[6];
  ngrp_global = static_cast<long>(allbuf_global[7]);

  for (size_t i = 0; i < 3; ++i) {
    dmu_dt[i] = dmu_dt_global_tmp[i];
    mu[i] = mu_global_tmp[i];
  }
  ke_au = ke_global / Eh_native;

  const double dof = 3.0 * static_cast<double>(ngrp_global);
  tempK = (2.0 * ke_global) / (force->boltz * dof);
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::calc_initial_dipole_info(double *mu, double *dmu_dt,
                                                 double &ke_au, double &tempK)
{
  double dmu_dt_local[3];
  double mu_local[3];
  double dmu_dt_global_tmp[3];
  double mu_global_tmp[3];

  dmu_dt_local[0] = dmu_dt_local[1] = dmu_dt_local[2] = 0.0;
  dmu_dt_global_tmp[0] = dmu_dt_global_tmp[1] = dmu_dt_global_tmp[2] = 0.0;
  mu_local[0] = mu_local[1] = mu_local[2] = 0.0;
  mu_global_tmp[0] = mu_global_tmp[1] = mu_global_tmp[2] = 0.0;

  double **v = atom->v;
  double **x = atom->x;
  double *q = atom->q;
  int *mask = atom->mask;
  int nlocal = atom->nlocal;
  int *type = atom->type;
  double *rmass = atom->rmass;

  if (igroup == atom->firstgroup)
    nlocal = atom->nfirst;

  imageint *image = atom->image;
  double unwrap[3];

  double ke_local = 0.0, ke_global = 0.0;
  long ngrp_local = 0, ngrp_global = 0;

  for (int i = 0; i < nlocal; i++) {
    if (!(mask[i] & groupbit))
      continue;
    dmu_dt_local[0] += q[i] * (v[i][0] * v_to_au);
    dmu_dt_local[1] += q[i] * (v[i][1] * v_to_au);
    dmu_dt_local[2] += q[i] * (v[i][2] * v_to_au);

    domain->unmap(x[i], image[i], unwrap);
    mu_local[0] += q[i] * (unwrap[0] * x_to_au);
    mu_local[1] += q[i] * (unwrap[1] * x_to_au);
    mu_local[2] += q[i] * (unwrap[2] * x_to_au);

    const double m = rmass ? rmass[i] : atom->mass[type[i]];
    const double vv = v[i][0] * v[i][0] + v[i][1] * v[i][1] + v[i][2] * v[i][2];
    ke_local += 0.5 * m * vv;
    ngrp_local++;
  }

  double allbuf_global[8];
  double allbuf[8] = {dmu_dt_local[0], dmu_dt_local[1], dmu_dt_local[2],
                      mu_local[0],     mu_local[1],     mu_local[2],
                      ke_local,        static_cast<double>(ngrp_local)};
  MPI_Allreduce(allbuf, allbuf_global, 8, MPI_DOUBLE, MPI_SUM, world);
  for (size_t i = 0; i < 3; ++i) {
    dmu_dt_global_tmp[i] = allbuf_global[i];
    mu_global_tmp[i] = allbuf_global[i + 3];
  }
  ke_global = allbuf_global[6];
  ngrp_global = static_cast<long>(allbuf_global[7]);

  for (size_t i = 0; i < 3; ++i) {
    dmu_dt[i] = dmu_dt_global_tmp[i];
    mu[i] = mu_global_tmp[i];
  }
  ke_au = ke_global / Eh_native;

  const double dof = 3.0 * static_cast<double>(ngrp_global);
  tempK = (2.0 * ke_global) / (force->boltz * dof);
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::build_additional_json(std::string &out, double t_fs,
                                              double tempK, double pe_au,
                                              double ke_au,
                                              const double dmudt_au[3]) const
{
  (void)dmudt_au;
  std::ostringstream ss;
  ss.setf(std::ios::fixed);
  ss << std::setprecision(15);
  ss << '{'
     << "\"time_au\":" << t_fs * 41.3413745758
     << ",\"mux_au\":" << mu_global[0]
     << ",\"muy_au\":" << mu_global[1]
     << ",\"muz_au\":" << mu_global[2]
     << ",\"mux_m_au\":" << mu_global_midpoint[0]
     << ",\"muy_m_au\":" << mu_global_midpoint[1]
     << ",\"muz_m_au\":" << mu_global_midpoint[2]
     << ",\"energy_au\":" << ke_au + pe_au
     << ",\"temp_K\":" << tempK
     << ",\"pe_au\":" << pe_au
     << ",\"ke_au\":" << ke_au
     << '}';
  out = ss.str();
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLinkUCX::end_of_step()
{
  for (size_t i = 0; i < 3; ++i) {
    dmu_dt_global_prev[i] = dmu_dt_global[i];
    mu_global_prev[i] = mu_global[i];
  }

  double ke_au = 0.0;
  double tempK = -1.0;

  calc_dipole_info(mu_global_midpoint, dmu_dt_global_midpoint, ke_au, tempK);
  if (reset_dipole) {
    for (size_t i = 0; i < 3; ++i)
      mu_global_midpoint[i] -= mu_global_initial[i];
  }

  for (size_t i = 0; i < 3; ++i) {
    dmu_dt_global[i] = dmu_dt_global_midpoint[i];
    mu_global[i] = mu_global_midpoint[i];
  }

  const double pe_native =
      modify->compute[modify->find_compute("thermo_pe")]->compute_scalar();
  const double pe_au = pe_native / Eh_native;
  const double t_native = update->ntimestep * update->dt;
  const double t_fs = t_native / force->femtosecond;

  if (master)
    build_additional_json(extra_json, t_fs, tempK, pe_au, ke_au, dmu_dt_global);

  if (master)
    send_amp_vector(extra_json);

  have_field = 0;
}
