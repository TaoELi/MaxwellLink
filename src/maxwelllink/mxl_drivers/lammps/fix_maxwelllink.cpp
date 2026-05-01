/* ----------------------------------------------------------------------
   FixMaxwellLink: i-PI–compatible client that:
   - modify dt based on MaxwellLink INIT message,
   - receives E-field [Ex,Ey,Ez] in atomic units from a MaxwellLink SocketHub,
   - applies F = qE to atoms, and
   - sends back d(mu)/dt = sum_i q_i v_i and mu (all atomic units) each step.
   Contributing author: Tao E. Li (University of Delaware)
   --------------------------------------------------------------------- */

#include "fix_maxwelllink.h"

#include "atom.h"
#include "comm.h"
#include "domain.h"
#include "error.h"
#include "force.h"
#include "group.h"
#include "memory.h"
#include "modify.h"
#include "update.h"
#include "integrate.h"
#include "compute.h"
#include "neighbor.h"
#include "input.h" 
#include "pair.h"
#include "output.h"
#include "variable.h"
#include "respa.h"
#include <sstream>
#include <iomanip>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <cmath>

#ifndef _WIN32
  #include <netdb.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <sys/un.h>
  #include <unistd.h>
#else
  #error "FixMaxwellLink requires a POSIX-like (UNIX) environment"
#endif

using namespace LAMMPS_NS;
using namespace FixConst;

#define MSGLEN 12

// --- protocol tags (i-PI compatible) ---
static const char HDR_STATUS[MSGLEN+1]     = "STATUS      ";
static const char HDR_READY [MSGLEN+1]     = "READY       ";
static const char HDR_HAVEDATA[MSGLEN+1]   = "HAVEDATA    ";
static const char HDR_NEEDINIT[MSGLEN+1]   = "NEEDINIT    ";
static const char HDR_INIT  [MSGLEN+1]     = "INIT        ";
static const char HDR_POSDATA[MSGLEN+1]    = "POSDATA     "; // aliased FIELDDATA
static const char HDR_GETFORCE[MSGLEN+1]   = "GETFORCE    "; // aliased GETSOURCE
static const char HDR_FORCEREADY[MSGLEN+1] = "FORCEREADY  "; // aliased SOURCEREADY
static const char HDR_STOP   [MSGLEN+1]    = "STOP        ";
static const char HDR_BYE    [MSGLEN+1]    = "BYE         ";
static const char HDR_EXIT   [MSGLEN+1]    = "EXIT        ";

/* ---------------------------------------------------------------------- */

FixMaxwellLink::FixMaxwellLink(LAMMPS *lmp, int narg, char **arg)
: Fix(lmp, narg, arg),
  host(nullptr), port(0), inet(1), master(0),
  sockfd(-1), initialized(0), have_field(0),
  bsize(0), ex_fac(0.0), ey_fac(0.0), ez_fac(0.0),
  Eau_x(0.0), Eau_y(0.0), Eau_z(0.0),
  qe2f(0.0), v_to_au(0.0), x_to_au(0.0), efield_au_native(0.0)
{
  if (narg < 5) utils::missing_cmd_args(FLERR, "fix MaxwellLink", error);

  if (atom->tag_enable == 0) error->all(FLERR, "Fix MaxwellLink requires atom IDs");
  if (strcmp(update->unit_style, "lj") == 0)
    error->all(FLERR, "Fix MaxwellLink does not support 'units lj'");

  host = utils::strdup(arg[3]);
  port = utils::inumeric(FLERR, arg[4], false, lmp);

  // optional: "unix"
  inet = 1;
  int iarg = 5;
  while (iarg < narg) {
    if (strcmp(arg[iarg], "unix") == 0) {
      inet = 0; ++iarg;
    } else if (strcmp(arg[iarg], "reset_dipole") == 0) {
      reset_dipole = 1; ++iarg;
      utils::logmesg(lmp, "[MaxwellLink] Will reset initial permanent dipole to zero.\n");
    } else {
      error->all(FLERR, "Unknown fix MaxwellLink keyword: {}", arg[iarg]);
    }
  }

  if (inet && ((port <= 1024) || (port > 65536)))
    error->all(FLERR, "Invalid port for fix MaxwellLink: {}", port);

  master = (comm->me == 0) ? 1 : 0;
  bsize = 0;

  respa_level_support = 1;
  ilevel_respa = 0;
  last_field_timestep = -1;

  // makes sure that neighbor lists are re-built at each step
  neighbor->delay = 0;
  neighbor->every = 1;

}

/* ---------------------------------------------------------------------- */

FixMaxwellLink::~FixMaxwellLink()
{
  if (host) free(host);
  close_socket();
}

/* ---------------------------------------------------------------------- */

int FixMaxwellLink::setmask()
{
  int mask = 0;
  mask |= INITIAL_INTEGRATE; // receive E-field for this step
  mask |= POST_FORCE;        // apply F = qE
  mask |= POST_FORCE_RESPA;    // NEW: ensure application at correct RESPA level
  mask |= MIN_POST_FORCE;    // NEW: ensure application during minimization
  mask |= END_OF_STEP;   // compute/send d(mu)/dt
  return mask;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::init()
{
   qflag = 0;
   if (atom->q_flag) qflag = 1;
   if (!qflag) error->all(FLERR, "Fix MaxwellLink requires per-atom charge 'q' (no q_flag)");

  // units conversion parameters
  // prefactor to convert lammps charge * E-field (native units) to force (native units)
  qe2f = force->qe2f;
  // 1 Bohr in native length
  a0_native   = 0.529177210544 * force->angstrom; 
  // 1 a.u. time in native time
  timeau_native = 0.024188843265864 * force->femtosecond;
  // 1 Hartree energy in native energy
  Eh_native = force->qqr2e * force->qelectron * force->qelectron / a0_native;
  // assuming lammps is operated under 'si' units
  // force->angstrom = 1.0 Angstrom
  // force->femtosecond = 1.0 fs
  // force->qelectron = 1.6021765e-19 C
  // force->qqr2e = 8.9876e9 (1/(4*pi*epsilon0)) in m/F
  // force->qe2f = 1.0
  // Eh_native = 8.9876e9 * (1.6021765e-19)^2 / (0.529177210544 * 1e-10) = 4.359744650e-18 J

  // velocity back conversion definition:
  // converting LAMMPS native velocity to a.u. should apply the inverse
  // idea is for 1 Angstrom/fs velocity in LAMMPS to become ~0.0457 a.u. velocity
  v_to_au = 1.0 / (a0_native / timeau_native);
  x_to_au = 1.0 / a0_native;

  // E-field conversion definition:
  // converting E-field in atomic units to LAMMPS native units
  // idea is for 1 Hartree/e/Bohr E-field in atomic units to become ~51.422 V/Angstrom in LAMMPS
  efield_au_native = Eh_native / (force->qelectron * a0_native);

  if (master) {
    open_socket();
  }

  // select RESPA level similar to fix efield (apply at outermost by default)
  if (utils::strmatch(update->integrate_style, "^respa")) {
    auto *r = dynamic_cast<Respa *>(update->integrate);
    if (r) {
      ilevel_respa = r->nlevels - 1;
      if (respa_level >= 0) ilevel_respa = MIN(respa_level, ilevel_respa);
    }
  }

  // compute PE. makes sure that it will be evaluated at next step
  modify->compute[modify->find_compute("thermo_pe")]->invoked_scalar = -1;
  modify->addstep_compute_all(update->ntimestep+1);

}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::open_socket()
{
  if (!master) return;

  if (!inet) {
    // UNIX domain socket 
    struct sockaddr_un serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    std::string path = "/tmp/socketmxl_";
    path += host;
    strncpy(serv_addr.sun_path, path.c_str(), sizeof(serv_addr.sun_path)-1);

    sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) error->one(FLERR, "Error creating UNIX socket in fix MaxwellLink");
    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    {
      if (!stop_requested)
        error->one(FLERR, "Error connecting UNIX socket in fix MaxwellLink");
      else 
      {
        close_socket();
        // end LAMMPS simulation 
        lmp->input->one("quit 0");   // graceful shutdown, exit status 0
      }
    }
    return;
  }

  // TCP/IP: try all records (IPv6 and IPv4) until one connects
  struct addrinfo hints, *res = nullptr, *rp = nullptr;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family   = AF_UNSPEC;   // try v6 and v4

  int gai = getaddrinfo(host, std::to_string(port).c_str(), &hints, &res);
  if (gai != 0) error->one(FLERR, "Fix MaxwellLink: getaddrinfo() failed for host {}", host);

  int last_errno = 0;
  for (rp = res; rp != nullptr; rp = rp->ai_next) {
    int fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd < 0) { last_errno = errno; continue; }
    sockfd = fd;
    set_nodelay_keepalive();
    if (connect(sockfd, rp->ai_addr, rp->ai_addrlen) == 0) break;  // success
    last_errno = errno;
    close(sockfd);
    sockfd = -1;
  }
  freeaddrinfo(res);

  if (sockfd < 0) {
    error->one(FLERR, "Fix MaxwellLink: connect() failed for {}:{} (errno {})",
               host, port, last_errno);
  }
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::close_socket()
{
  if (sockfd >= 0) {
    close(sockfd);
    sockfd = -1;
    initialized = 0;
  }
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::set_nodelay_keepalive()
{
  if (sockfd < 0) return;
  int one = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (char*)&one, sizeof(int));
  // may be AF_UNIX, TCP_NODELAY invalid there -> ignore errors
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(int));
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::writebuffer(const char *data, int len)
{
  int n = ::write(sockfd, data, len);
  if (n < 0) error->one(FLERR, "Fix MaxwellLink: write() failed (broken connection)");
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::readbuffer(char *data, int len)
{
  int n = ::read(sockfd, data, len), nr = n;
  while (nr > 0 && n < len) {
    nr = ::read(sockfd, &data[n], len - n);
    n += nr;
  }
  if (n == 0) error->one(FLERR, "Fix MaxwellLink: read() got EOF (broken connection)");
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::read_header(char *hdr12)
{
  readbuffer(hdr12, MSGLEN);
  hdr12[MSGLEN] = 0;
}

/* ---------------------------------------------------------------------- */

int FixMaxwellLink::read_int32()
{
  int val = 0;
  readbuffer((char*)&val, 4);
  return val;
}

/* ---------------------------------------------------------------------- */

int FixMaxwellLink::read_bytes(char *&buf)
{
  int n = read_int32();
  if (n <= 0) { buf = nullptr; return 0; }
  buf = (char*) malloc(n);
  readbuffer(buf, n);
  return n;
}


bool FixMaxwellLink::try_read_header(char *hdr12)
{
  // Non-fatal read of 12-byte header. Returns false on clean EOF.
  int n = ::read(sockfd, hdr12, MSGLEN);
  if (n == 0) return false;
  if (n < 0) error->one(FLERR, "Fix MaxwellLink: read() failed while reading header");

  int got = n;
  while (got < MSGLEN) {
    int nr = ::read(sockfd, hdr12 + got, MSGLEN - got);
    if (nr == 0) return false;
    if (nr < 0) error->one(FLERR, "Fix MaxwellLink: read() failed while reading header");
    got += nr;
  }
  hdr12[MSGLEN] = 0;
  return true;
}

/* ---------------------------------------------------------------------- */

static bool parse_dt_au_from_json(const char* s, int n, double& out_dt_au)
{
  // looks for "dt_au": <number>
  const char* p = s;
  const char* end = s + n;
  while (p < end) {
    // match "dt_au" literally (allow spaces before/after)
    if (*p == 'd' && (end - p) >= 5 && std::strncmp(p, "dt_au", 5) == 0) {
      const char* q = p + 5;
      while (q < end && *q != ':') ++q;
      if (q == end) break;
      ++q; // past ':'
      while (q < end && (std::isspace((unsigned char)*q) || *q=='"')) ++q;
      char* r = nullptr;
      double v = std::strtod(q, &r);
      if (r && r != q) { out_dt_au = v; return true; }
    }
    ++p;
  }
  return false;
}

void FixMaxwellLink::build_additional_json(std::string& out,
                                          double t_fs, double tempK, double pe_au, double ke_au,
                                          const double dmudt_au[3]) const
{
  std::ostringstream ss;
  ss.setf(std::ios::fixed);
  ss << std::setprecision(15);
  /*
  We recommend including "time_au", "energy_au", and dipole
        components "mux_au", "muy_au", "muz_au" in the returned dictionary. This format 
        would allow for easy energy analysis. Dipole information is useful for debugging
        and also computing dipole self-energy term if needed.
  */
  ss << '{'
     << "\"time_au\":"<< t_fs * 41.3413745758
     << ",\"mux_au\":"<< mu_global[0]
     << ",\"muy_au\":"<< mu_global[1]
     << ",\"muz_au\":"<< mu_global[2]
     << ",\"mux_m_au\":"<< mu_global_force[0]
     << ",\"muy_m_au\":"<< mu_global_force[1]
     << ",\"muz_m_au\":"<< mu_global_force[2]
     << ",\"energy_au\":"<< ke_au + pe_au
     << ",\"temp_K\":"<< tempK
     << ",\"pe_au\":" << pe_au
     << ",\"ke_au\":" << ke_au
     << '}';
  out = ss.str();
}


void FixMaxwellLink::handshake_if_needed()
{
  if (!master || initialized) return;

  // Drive the minimal INIT dance:
  // STATUS -> send NEEDINIT
  // expect INIT: int32 molid + int32 len + JSON bytes (ignored or parsed)
  char header[MSGLEN+1];
  while (!initialized) {
    read_header(header);

    if (!strncmp(header, HDR_STATUS, MSGLEN)) {
      writebuffer(HDR_NEEDINIT, MSGLEN);
      continue;
    }
    if (!strncmp(header, HDR_INIT, MSGLEN)) {
      molid = read_int32();
      if (comm->me == 0)
        // printf("[MaxwellLink] Assigned a molecular ID: %d\n", molid);
        utils::logmesg(lmp, "[MaxwellLink] Assigned a molecular ID: {}\n", molid);
      char *blob = nullptr;
      int blen = read_bytes(blob);

      // Parse "dt_au" from JSON payload
      double parsed_dt_au = 0.0;
      if (blob && blen > 0) {
        parse_dt_au_from_json(blob, blen, parsed_dt_au);
      }
      if (blob) free(blob);
      dt_au_recv = parsed_dt_au;

      // Convert to native LAMMPS time units: 1 a.u. time = 0.024188843265864 fs
      const double timeau_native = 0.024188843265864 * force->femtosecond;
      if (dt_au_recv > 0.0) {
        dt_native_recv = dt_au_recv * timeau_native;
      }

      initialized = 1;

      break;
    }
    if (!strncmp(header, HDR_STOP, MSGLEN) || !strncmp(header, HDR_EXIT, MSGLEN)) {
      writebuffer(HDR_BYE, MSGLEN);
      //error->one(FLERR, "Fix MaxwellLink: server requested stop");
      //printf("[MaxwellLink] Server requested stop/exit during handshake, exiting gracefully...\n");
      utils::logmesg(lmp, "[MaxwellLink] Server requested stop/exit during handshake, exiting gracefully...\n");
      stop_requested = true;
      error->one(FLERR, "[MaxwellLink] Exit requested by MaxwellLink server...");
      return;
    }
  }
}

void FixMaxwellLink::broadcast_dt()
{
  // broadcast dt_native_recv to all ranks
  double dtbuf = dt_native_recv;
  MPI_Bcast(&dtbuf, 1, MPI_DOUBLE, 0, world);
  if (true) {
    dt_native_recv = dtbuf;
    const double prior = update->dt;
    if (dt_native_recv > 0.0 && fabs(update->dt - dt_native_recv) > 1e-10) {
      // Force LAMMPS to use this dt on non-master ranks now
      update->update_time();
      update->dt = dt_native_recv;
      update->dt_default = 0;
      if (true) update->integrate->reset_dt();
      if (force->pair) force->pair->reset_dt();
      for (auto &ifix : modify->get_fix_list()) ifix->reset_dt();
      output->reset_dt();

      if (comm->me == 0) {
          utils::logmesg(lmp, "[MaxwellLink] 1 atomic units time in LAMMPS native time units = {}\n", timeau_native);
          utils::logmesg(lmp, "[MaxwellLink] MaxwellLink uses time step: dt_au = {} ->  dt_native (LAMMPS units) = {}\n",
                 dt_au_recv, dt_native_recv);
          utils::logmesg(lmp, "[MaxwellLink] Modified LAMMPS time step from {} to {} to match MaxwellLink dt!\n",
                 prior, update->dt);
          utils::logmesg(lmp, "[MaxwellLink] Make sure all coupled simulations use this time step, after the modification, LAMMPS dt = {}\!\n", update->dt);
      }

      if (comm->me == 1) { // rank 1 only
        utils::logmesg(lmp, "[MaxwellLink] Non-master rank modified LAMMPS time step from {} to {} to match MaxwellLink dt!\n",
                       prior, update->dt);
      }
    }
  }
  dt_synced = 1;
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::recv_efield()
{
  // Read a POSDATA/FIELDDATA block:
  //  - 3x3 cell (float64), 3x3 invcell (float64), nat (int32), positions (nat x 3 float64)
  // For EM alias, nat=1 and positions[0] = [Ex,Ey,Ez] in atomic units.
  double cell[9], icell[9];
  int nat = 0;

  readbuffer((char*)cell,  9*8);
  readbuffer((char*)icell, 9*8);
  readbuffer((char*)&nat,  4);

  if (nat < 1) error->one(FLERR, "Fix MaxwellLink: invalid nat in FIELDDATA/POSDATA");

  double evec[3] = {0,0,0};
  readbuffer((char*)evec, nat*3*8);
  // ignore additional positions if nat>1 (shouldn't happen)

  Eau_x = evec[0];
  Eau_y = evec[1];
  Eau_z = evec[2];

  have_field = 1;
}


void FixMaxwellLink::send_amp_vector(const std::string& extra)
{
  // Reply FORCEREADY:
  // energy (1 float64), natoms (int32=1), forces (1x3 float64 -> our amp vector),
  // virial (3x3 float64), extra blob length (int32=0) + 0 bytes
  double energy = 0.0;
  int nat = 1;
  double vir[9];
  for (int i=0;i<9;++i) vir[i]=0.0;

  writebuffer(HDR_FORCEREADY, MSGLEN);
  writebuffer((char*)&energy, 8);
  writebuffer((char*)&nat,    4);
  writebuffer((char*)dmu_dt_global, 3*8);
  writebuffer((char*)vir,     9*8);
  // send extra JSON blob
  int len = (int)extra.size();
  writebuffer((char*)&len, 4);
  if (len > 0) writebuffer(extra.data(), len);

}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::initial_integrate(int /*vflag*/)
{

  // Master: drive handshake (if needed), then wait for FIELDDATA for this step
  // Non-master: will get E broadcast from master
  char header[MSGLEN+1];

  if (master) {
    if (sockfd < 0) {
      open_socket();
      if (sockfd < 0) return; // open_socket failed but stop_requested
    }
    handshake_if_needed();
  }

  // broadcast dt if needed
  if (dt_synced == 0) {
    broadcast_dt();
  }

  if (prcompute_dipole == 0) {
      double ke_au = 0.0;
      double tempK = -1.0;
      calc_initial_dipole_info(mu_global, dmu_dt_global, ke_au, tempK);

      for (size_t i =0; i < 3; ++i) {
        if (reset_dipole) {
          mu_global_initial[i] = mu_global[i];
          mu_global[i] -= mu_global_initial[i];
        }

        mu_global_force[i] = mu_global[i];
        mu_global_prev[i] = mu_global[i];
        dmu_dt_global_prev[i] = dmu_dt_global[i];
      }

      prcompute_dipole = 1;
  }

  // MPI_Barrier(world);

  if (master) {

    // Consume STATUS loops until we get POSDATA/FIELDDATA
    while (true) {
      read_header(header);

      if (!strncmp(header, HDR_STATUS, MSGLEN)) {
        // Before we have a result, we are READY
        writebuffer(HDR_READY, MSGLEN);
        continue;
      }
      if (!strncmp(header, HDR_POSDATA, MSGLEN)) {
        recv_efield();
        break;
      }
      if (!strncmp(header, HDR_STOP, MSGLEN) || !strncmp(header, HDR_EXIT, MSGLEN)) {
        writebuffer(HDR_BYE, MSGLEN);
        //error->one(FLERR, "Fix MaxwellLink: server requested stop");
        utils::logmesg(lmp, "[MaxwellLink] Server requested stop/exit, exiting gracefully...\n");
        stop_requested = true;
        break;
      }
    }
  }

  // MPI_Barrier(world);

  // --- after rank 0 has received POSDATA and set Eau_x/y/z ---
  double ebuf[3];

  // fill ebuf on master only
  if (master) {
    ebuf[0] = Eau_x; ebuf[1] = Eau_y; ebuf[2] = Eau_z;
  }

  // EVERY rank must call MPI_Bcast
  MPI_Bcast(ebuf, 3, MPI_DOUBLE, 0, world);

  // now set the E-field on all ranks
  Eau_x = ebuf[0];
  Eau_y = ebuf[1];
  Eau_z = ebuf[2];

  // compute force factors locally (identical on all ranks)
  ex_fac = efield_au_native * Eau_x;
  ey_fac = efield_au_native * Eau_y;
  ez_fac = efield_au_native * Eau_z;

  // MPI_Barrier(world);

  // compute PE. makes sure that it will be evaluated at next step
  modify->compute[modify->find_compute("thermo_pe")]->invoked_scalar = -1;
  modify->addstep_compute_all(update->ntimestep+1);

  have_field = 1;
}

void FixMaxwellLink::post_force(int vflag)
{
  if (!have_field) return; 

  // Apply F = qE (constant field this step)
  double **f = atom->f;
  double *q   = atom->q;
  int *mask   = atom->mask;
  int nlocal  = atom->nlocal;
  if (igroup == atom->firstgroup) nlocal = atom->nfirst;

  if (!atom->q_flag)
    error->all(FLERR, "Fix MaxwellLink requires per-atom charge 'q' (no q_flag)");
  for (int i = 0; i < nlocal; i++) {
    if (mask[i] & groupbit) {
      f[i][0] += q[i] * ex_fac;
      f[i][1] += q[i] * ey_fac;
      f[i][2] += q[i] * ez_fac;
    }
  }
}


void FixMaxwellLink::setup(int vflag)
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

void FixMaxwellLink::min_setup(int vflag)
{
  post_force(vflag);
}

void FixMaxwellLink::post_force_respa(int vflag, int ilevel, int /*iloop*/)
{
  if (ilevel == ilevel_respa) post_force(vflag);
}

/* ---------------------------------------------------------------------- */

void FixMaxwellLink::min_post_force(int vflag)
{
  post_force(vflag);
}


/* ---------------------------------------------------------------------- */

void FixMaxwellLink::calc_dipole_info(double *mu, double *dmu_dt, double &ke_au, double &tempK) 
{
  double dmu_dt_local[3];
  double mu_local[3];
  double dmu_dt_global_tmp[3];
  double mu_global_tmp[3];

  dmu_dt_local[0]=dmu_dt_local[1]=dmu_dt_local[2]=0.0;
  dmu_dt_global_tmp[0]=dmu_dt_global_tmp[1]=dmu_dt_global_tmp[2]=0.0;
  mu_local[0]=mu_local[1]=mu_local[2]=0.0;
  mu_global_tmp[0]=mu_global_tmp[1]=mu_global_tmp[2]=0.0;

  double **v = atom->v;
  double **x = atom->x;
  double *q  = atom->q;
  double **f = atom->f;
  int *mask  = atom->mask;
  int nlocal = atom->nlocal;
  int *type  = atom->type;
  double *rmass = atom->rmass;

  if (igroup == atom->firstgroup) nlocal = atom->nfirst;

  imageint *image = atom->image;
  double unwrap[3];

  double ke_local = 0.0, ke_global = 0.0;
  long   ngrp_local = 0, ngrp_global = 0;
  double m, vv;

  double vhalf[3] = {0.0, 0.0, 0.0};
  double xhalf[3] = {0.0, 0.0, 0.0};

  double dtfm;
  double dtv = 0.5 * update->dt;
  double dtf = 0.5 * update->dt * force->ftm2v;

  for (int i=0;i<nlocal;i++) {
    /*
    if (!(mask[i] & groupbit)) continue;
    dmu_dt_local[0] += q[i] * (v[i][0] * v_to_au);
    dmu_dt_local[1] += q[i] * (v[i][1] * v_to_au);
    dmu_dt_local[2] += q[i] * (v[i][2] * v_to_au);

    domain->unmap(x[i], image[i], unwrap);
    mu_local[0] += q[i] * (unwrap[0] * x_to_au);
    mu_local[1] += q[i] * (unwrap[1] * x_to_au);
    mu_local[2] += q[i] * (unwrap[2] * x_to_au);
    */
    
    // let's do something crazy here: we compute dmudt and mu at half step after this simulation step
    // to make this happen, we need to propagate v and x by half a step here following standard Velocity-Verlet
    if (!(mask[i] & groupbit)) continue;
    m = rmass ? rmass[i] : atom->mass[type[i]];
    dtfm = dtf / m;
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
    
    //m = rmass ? rmass[i] : atom->mass[type[i]];
    vv = v[i][0]*v[i][0] + v[i][1]*v[i][1] + v[i][2]*v[i][2];
    ke_local += 0.5 * m * vv;
    ngrp_local++;
  }

  //MPI_Allreduce(dmu_dt_local, dmu_dt_global_tmp, 3, MPI_DOUBLE, MPI_SUM, world);
  //MPI_Allreduce(mu_local, mu_global_tmp, 3, MPI_DOUBLE, MPI_SUM, world);
  //MPI_Allreduce(&ke_local, &ke_global, 1, MPI_DOUBLE, MPI_SUM, world);
  //MPI_Allreduce(&ngrp_local, &ngrp_global, 1, MPI_LONG,   MPI_SUM, world);

  // replace the above four allreduce with a single allreduce
  double allbuf_global[8];
  double allbuf[8] = {dmu_dt_local[0], dmu_dt_local[1], dmu_dt_local[2],
                        mu_local[0],    mu_local[1],    mu_local[2],
                        ke_local,      (double)ngrp_local};
  MPI_Allreduce(allbuf, allbuf_global, 8, MPI_DOUBLE, MPI_SUM, world);
  for (size_t i = 0; i <3; ++i) {
    dmu_dt_global_tmp[i] = allbuf_global[i];
    mu_global_tmp[i]     = allbuf_global[i+3];
  }
  ke_global = allbuf_global[6];
  ngrp_global = (long)(allbuf_global[7]);
  
  // copy info to output
  for (size_t i = 0; i < 3; ++i) {
    dmu_dt[i] = dmu_dt_global_tmp[i];
    mu[i] = mu_global_tmp[i];
  }
  ke_au = ke_global / Eh_native; // convert to a.u.

  const double dof = 3.0 * (double)ngrp_global;
  tempK = (2.0 * ke_global) / (force->boltz * dof);
}


void FixMaxwellLink::calc_initial_dipole_info(double *mu, double *dmu_dt, double &ke_au, double &tempK) 
{
  double dmu_dt_local[3];
  double mu_local[3];
  double dmu_dt_global_tmp[3];
  double mu_global_tmp[3];

  dmu_dt_local[0]=dmu_dt_local[1]=dmu_dt_local[2]=0.0;
  dmu_dt_global_tmp[0]=dmu_dt_global_tmp[1]=dmu_dt_global_tmp[2]=0.0;
  mu_local[0]=mu_local[1]=mu_local[2]=0.0;
  mu_global_tmp[0]=mu_global_tmp[1]=mu_global_tmp[2]=0.0;

  double **v = atom->v;
  double **x = atom->x;
  double *q  = atom->q;
  double **f = atom->f;
  int *mask  = atom->mask;
  int nlocal = atom->nlocal;
  int *type  = atom->type;
  double *rmass = atom->rmass;

  if (igroup == atom->firstgroup) nlocal = atom->nfirst;

  imageint *image = atom->image;
  double unwrap[3];

  double ke_local = 0.0, ke_global = 0.0;
  long   ngrp_local = 0, ngrp_global = 0;
  double m, vv;

  double vhalf[3] = {0.0, 0.0, 0.0};
  double xhalf[3] = {0.0, 0.0, 0.0};

  double dtfm;
  double dtv = 0.5 * update->dt;
  double dtf = 0.5 * update->dt * force->ftm2v;

  for (int i=0;i<nlocal;i++) {
    if (!(mask[i] & groupbit)) continue;
    dmu_dt_local[0] += q[i] * (v[i][0] * v_to_au);
    dmu_dt_local[1] += q[i] * (v[i][1] * v_to_au);
    dmu_dt_local[2] += q[i] * (v[i][2] * v_to_au);

    domain->unmap(x[i], image[i], unwrap);
    mu_local[0] += q[i] * (unwrap[0] * x_to_au);
    mu_local[1] += q[i] * (unwrap[1] * x_to_au);
    mu_local[2] += q[i] * (unwrap[2] * x_to_au);

    m = rmass ? rmass[i] : atom->mass[type[i]];
    vv = v[i][0]*v[i][0] + v[i][1]*v[i][1] + v[i][2]*v[i][2];
    ke_local += 0.5 * m * vv;
    ngrp_local++;
  }

  //MPI_Allreduce(dmu_dt_local, dmu_dt_global_tmp, 3, MPI_DOUBLE, MPI_SUM, world);
  //MPI_Allreduce(mu_local, mu_global_tmp, 3, MPI_DOUBLE, MPI_SUM, world);
  //MPI_Allreduce(&ke_local, &ke_global, 1, MPI_DOUBLE, MPI_SUM, world);
  //MPI_Allreduce(&ngrp_local, &ngrp_global, 1, MPI_LONG,   MPI_SUM, world);

  // replace the above four allreduce with a single allreduce
  double allbuf_global[8];
  double allbuf[8] = {dmu_dt_local[0], dmu_dt_local[1], dmu_dt_local[2],
                        mu_local[0],    mu_local[1],    mu_local[2],
                        ke_local,      (double)ngrp_local};
  MPI_Allreduce(allbuf, allbuf_global, 8, MPI_DOUBLE, MPI_SUM, world);
  for (size_t i = 0; i <3; ++i) {
    dmu_dt_global_tmp[i] = allbuf_global[i];
    mu_global_tmp[i]     = allbuf_global[i+3];
  }
  ke_global = allbuf_global[6];
  ngrp_global = (long)(allbuf_global[7]);
  
  // copy info to output
  for (size_t i = 0; i < 3; ++i) {
    dmu_dt[i] = dmu_dt_global_tmp[i];
    mu[i] = mu_global_tmp[i];
  }
  ke_au = ke_global / Eh_native; // convert to a.u.

  const double dof = 3.0 * (double)ngrp_global;
  tempK = (2.0 * ke_global) / (force->boltz * dof);
}

void FixMaxwellLink::end_of_step()
{

  for (size_t i = 0; i < 3; ++i) {
    dmu_dt_global_prev[i] = dmu_dt_global[i];
    mu_global_prev[i] = mu_global[i];
  }

  double ke_au = 0.0;
  double tempK = -1.0;
  double dmu_dt_global_force[3] = {0.0, 0.0, 0.0};

  calc_initial_dipole_info(mu_global_force, dmu_dt_global_force, ke_au, tempK);
  if (reset_dipole) {
    for (size_t i=0; i < 3; ++i) {
      mu_global_force[i] -= mu_global_initial[i];
    }
  }

  calc_dipole_info(mu_global_midpoint, dmu_dt_global_midpoint, ke_au, tempK);
  if (reset_dipole) {
    for (size_t i=0; i < 3; ++i) {
      mu_global_midpoint[i] -= mu_global_initial[i];
    }
  }

  // MaxwellLink expects d(mu)/dt and mu at half a time step after E-field (force) evaluation
  for (size_t i = 0; i < 3; ++i) {
    dmu_dt_global[i] = dmu_dt_global_midpoint[i];
    mu_global[i] = mu_global_midpoint[i];
  }


  double pe_native = modify->compute[modify->find_compute("thermo_pe")]->compute_scalar();

  // --- Convert requested fields to required units
  const double pe_au = pe_native / Eh_native;

  const double t_native = update->ntimestep * update->dt;
  const double t_fs     = t_native / force->femtosecond; // native -> femtoseconds

  // 4) Build JSON on master
  if (master) {
    build_additional_json(extra_json, t_fs, tempK, pe_au, ke_au, dmu_dt_global);
  }

  // Master: answer STATUS/GETFORCE with HAVEDATA/FORCEREADY
  if (master) {
    char header[MSGLEN+1];

    while (true) {
      if (!try_read_header(header)) {
        // server closed gracefully
        close_socket();
        have_field = 0;
        return;
      }

      if (!strncmp(header, HDR_STATUS, MSGLEN)) {
        writebuffer(HDR_HAVEDATA, MSGLEN);
        continue;
      }
      if (!strncmp(header, HDR_GETFORCE, MSGLEN)) {
        send_amp_vector(extra_json);
        break;
      }
      if (!strncmp(header, HDR_STOP, MSGLEN) || !strncmp(header, HDR_EXIT, MSGLEN)) {
        writebuffer(HDR_BYE, MSGLEN);
        //error->one(FLERR, "Fix MaxwellLink: server requested stop");
        utils::logmesg(lmp, "[MaxwellLink] Server requested stop/exit, exiting gracefully...\n");
        stop_requested = true;
        break;
      }
    }
  }

  // All ranks wait for master to finish socket ops
  //MPI_Barrier(world);

  have_field = 0;
}
