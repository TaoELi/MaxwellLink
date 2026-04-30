#ifdef FIX_CLASS
// clang-format off
FixStyle(mxl,FixMaxwellLink);
// clang-format on
#else

#ifndef LMP_FIX_MAXWELL_LINK_H
#define LMP_FIX_MAXWELL_LINK_H

#include "fix.h"
#include <cctype>

namespace LAMMPS_NS {

class FixMaxwellLink : public Fix {
 public:
  FixMaxwellLink(class LAMMPS *, int, char **);
  ~FixMaxwellLink() override;
  int setmask() override;
  void init() override;

  void initial_integrate(int) override;  // receive E-field from MaxwellLink
  void post_force(int) override;         // apply F = qE
  void setup(int) override;
  void min_setup(int) override;
  void post_force_respa(int, int, int) override;
  void min_post_force(int) override;
  void end_of_step() override;       // send d(mu)/dt back to MaxwellLink

 private:
  // connection/config
  char *host;
  int port;
  int inet;        // 1=INET (TCP), 0=UNIX
  int master;      // rank 0
  int sockfd;      // socket fd
  int initialized; // after INIT handshake
  int have_field;  // E-field received this step
  int molid;       // molecule ID
  int qflag = 0;   // NEW: track presence of per-atom charges (like fix efield)

  bool stop_requested = false;

  // buffers / sizes
  long bsize;
  double ex_fac, ey_fac, ez_fac; // q * qe2f * (E_au -> field units) pre-multiplied factors
  double Eau_x, Eau_y, Eau_z;    // last E-field in atomic units (for diagnostics)

  // conversion helpers
  double qe2f;         // taken from force->qe2f
  double v_to_au;      // velocity conversion factor (LAMMPS vel -> bohr/au_time)
  double x_to_au;      // position conversion factor (LAMMPS length -> bohr)
  double efield_au_native; // E-field atomic units in native LAMMPS field units

  // accumulated dipole-rate this step (atomic units)
  double dmu_dt_local[3];
  double dmu_dt_global[3];
  double dmu_dt_global_midpoint[3];
  double dmu_dt_global_prev[3];
  double mu_local[3];
  double mu_global[3];
  double mu_global_force[3];
  double mu_global_midpoint[3];
  double mu_global_prev[3];

   // timestep from MaxwellLink
   double dt_au_recv = 0.0;        // dt in atomic units received from INIT
   double dt_native_recv = 0.0;    // dt converted to LAMMPS native time units
   int dt_synced = 0;              // 0 until we've applied/broadcast dt
   int prcompute_dipole = 0;       // 1 if we should compute dipole at initial_integrate
   int reset_dipole = 0;               // 1 if we should reset initial permanent dipole to zero
   double mu_global_initial[3] = {0.0, 0.0, 0.0}; // initial permanent dipole to subtract if reset_dipole=1

   // additional-data in JSON (master only)
   std::string extra_json;

   // conversion constants retained for reuse
  double a0_native;         // 1 bohr in LAMMPS native length units
  double timeau_native;     // 1 a.u. time in LAMMPS native time units
  double Eh_native;         // 1 Hartree in LAMMPS native energy units

  int ilevel_respa;  // respa level to apply fix at
  long last_field_timestep = -1; // NEW: ensure we fetch E-field once per LAMMPS timestep
 

  // helpers
  void open_socket();
  void close_socket();
  void handshake_if_needed();  // STATUS->NEEDINIT->INIT handling
  void recv_efield();          // read FIELDDATA/POSDATA and set ex_fac,ey_fac,ez_fac
  // void send_amp_vector();      // reply to GETSOURCE with dmu_dt_global (FORCEREADY)
  void send_amp_vector(const std::string& extra_json); // reply with FORCEREADY (+ extra)
  void calc_dipole_info(double *mu, double *dmu_dt, double &ke_au, double &tempK);
  void calc_initial_dipole_info(double *mu, double *dmu_dt, double &ke_au, double &tempK);

  // socket I/O
  void writebuffer(const char *data, int len);
  void readbuffer(char *data, int len);
  void read_header(char *hdr12); // reads 12-byte header
  int  read_int32();             // host-order int32
  int  read_bytes(char *&buf);   // malloc+read blob, returns length
  void set_nodelay_keepalive();  // TCP_NODELAY + SO_KEEPALIVE
  void broadcast_dt();        // broadcast dt_native_recv to all ranks

  // disallow copy
  FixMaxwellLink(const FixMaxwellLink&) = delete;
  FixMaxwellLink& operator=(const FixMaxwellLink&) = delete;

  // end of simulation usage
  bool try_read_header(char *hdr12);    // NEW: returns false on EOF (clean shutdown)

  // sending additional data to MaxwellLink at each time step
  void build_additional_json(std::string& out_json,
                             double t_fs, double tempK, double pe_au, double ke_au, const double dmudt_au[3]) const;

};

} // namespace LAMMPS_NS

#endif
#endif
