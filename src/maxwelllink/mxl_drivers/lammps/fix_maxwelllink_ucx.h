#ifdef FIX_CLASS
// clang-format off
FixStyle(mxl/ucx,FixMaxwellLinkUCX);
// clang-format on
#else

#ifndef LMP_FIX_MAXWELL_LINK_UCX_H
#define LMP_FIX_MAXWELL_LINK_UCX_H

#include "fix.h"
#include <cctype>
#include <memory>
#include <string>
#include <vector>

namespace ucxx {
class Context;
class Worker;
class Endpoint;
class Request;
}

namespace LAMMPS_NS {

class FixMaxwellLinkUCX : public Fix {
 public:
  FixMaxwellLinkUCX(class LAMMPS *, int, char **);
  ~FixMaxwellLinkUCX() override;
  int setmask() override;
  void init() override;

  void initial_integrate(int) override;
  void post_force(int) override;
  void setup(int) override;
  void min_setup(int) override;
  void post_force_respa(int, int, int) override;
  void min_post_force(int) override;
  void end_of_step() override;

 private:
  char *host;
  int port;
  int master;
  int initialized;
  int have_field;
  int hello_sent;
  int molid;
  int qflag = 0;

  bool stop_requested = false;

  long bsize;
  double ex_fac, ey_fac, ez_fac;
  double Eau_x, Eau_y, Eau_z;

  double qe2f;
  double v_to_au;
  double x_to_au;
  double efield_au_native;

  double dmu_dt_local[3];
  double dmu_dt_global[3];
  double dmu_dt_global_midpoint[3];
  double dmu_dt_global_prev[3];
  double mu_local[3];
  double mu_global[3];
  double mu_global_midpoint[3];
  double mu_global_prev[3];

  double dt_au_recv = 0.0;
  double dt_native_recv = 0.0;
  int dt_synced = 0;
  int prcompute_dipole = 0;
  int reset_dipole = 0;
  double mu_global_initial[3] = {0.0, 0.0, 0.0};

  std::string extra_json;

  double a0_native;
  double timeau_native;
  double Eh_native;

  int ilevel_respa;
  long last_field_timestep = -1;

  std::shared_ptr<ucxx::Context> ucxx_context;
  std::shared_ptr<ucxx::Worker> ucxx_worker;
  std::shared_ptr<ucxx::Endpoint> ucxx_endpoint;

  void open_ucx();
  void close_ucx();
  void handshake_if_needed();
  bool recv_ucx_message(unsigned short &opcode, std::vector<char> &payload);
  bool send_ucx_message(unsigned short opcode, const std::vector<char> &payload);
  void send_hello();
  void send_bye();
  void recv_efield_from_payload(const std::vector<char> &payload);
  void send_amp_vector(const std::string &extra_json);
  void calc_dipole_info(double *mu, double *dmu_dt, double &ke_au, double &tempK);
  void calc_initial_dipole_info(double *mu, double *dmu_dt, double &ke_au,
                                double &tempK);
  void broadcast_dt();
  void build_additional_json(std::string &out_json, double t_fs, double tempK,
                             double pe_au, double ke_au,
                             const double dmudt_au[3]) const;
  void wait_request(const std::shared_ptr<ucxx::Request> &request);

  FixMaxwellLinkUCX(const FixMaxwellLinkUCX &) = delete;
  FixMaxwellLinkUCX &operator=(const FixMaxwellLinkUCX &) = delete;
};

} // namespace LAMMPS_NS

#endif
#endif
