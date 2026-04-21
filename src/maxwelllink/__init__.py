from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("maxwelllink")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "TLSMolecule",
    "SocketMolecule",
    "update_molecules",
    "update_molecules_no_mpi",
    "update_molecules_no_socket",
    "SocketHub",
    "get_available_host_port",
    "mxl_driver_main",
    "launch_driver",
    "terminate_driver",
    "mxl_lammps_main",
    "RTTDDFTModel",
    "RTEhrenfestModel",
    "QuTiPModel",
    "ASEModel",
    "TLSModel",
    # v2 features
    "MeepSimulation",
    "meep_units_helper",
    "Molecule",
    "SingleModeSimulation",
    "MultiModeSimulation",
    "Vector3",
    "LaserDrivenSimulation",
]


# Lazy attribute loader: import submodules *only when accessed*.
def __getattr__(name):
    """
    Lazy attribute loader that imports and returns model classes on demand.

    Parameters
    ----------
    name : str
        The attribute name requested (e.g., ``"Molecule"``, ``"SocketHub"``).

    Returns
    -------
    type
        The requested class object.

    Raises
    ------
    AttributeError
        If the requested attribute is not a known model class.
    """
    # Legacy code path (molecule_fast) kept for reference; prefer molecule_abstract + em_solvers/meep now.
    if name in {
        "TLSMolecule",
        "SocketMolecule",
        "update_molecules",
        "update_molecules_no_mpi",
        "update_molecules_no_socket",
    }:
        from .molecule import (
            TLSMolecule,
            SocketMolecule,
            update_molecules,
            update_molecules_no_mpi,
            update_molecules_no_socket,
        )

        return locals()[name]

    if name in {
        "Molecule",
        "Vector3",
    }:
        from .molecule import (
            Molecule,
            Vector3,
        )

        return locals()[name]

    if name in {
        "MeepSimulation",
        "meep_units_helper",
    }:
        from .em_solvers.meep import (
            MeepSimulation,
            meep_units_helper,
        )

        return locals()[name]
    if name in {
        "SingleModeSimulation",
    }:
        from .em_solvers.single_mode_cavity import (
            SingleModeSimulation,
        )
    
        return locals()[name]   
    if name in {
        "MultiModeSimulation",
    }:
        from .em_solvers.multimode_cavity import (
            MultiModeSimulation,
        )

        return locals()[name]

    if name in {
        "LaserDrivenSimulation",
    }:
        from .em_solvers.laser_driven import (
            LaserDrivenSimulation,
        )

        return locals()[name]

    if name in {"SocketHub", "get_available_host_port"}:
        from .sockets import SocketHub, get_available_host_port

        return locals()[name]
    if name in {"mxl_driver_main", "launch_driver", "terminate_driver"}:
        from .mxl_drivers.python.mxl_driver import (
            mxl_driver_main,
            launch_driver,
            terminate_driver,
        )

        return locals()[name]
    if name == "mxl_lammps_main":
        from .mxl_drivers.lammps.install import mxl_lammps_main

        return mxl_lammps_main
    if name == "RTTDDFTModel":
        from .mxl_drivers.python.models.rttddft_model import RTTDDFTModel

        return RTTDDFTModel
    if name == "RTEhrenfestModel":
        from .mxl_drivers.python.models.rt_ehrenfest_model import RTEhrenfestModel

        return RTEhrenfestModel
    if name == "QuTiPModel":
        from .mxl_drivers.python.models.qutip_model import QuTiPModel

        return QuTiPModel
    if name == "ASEModel":
        from .mxl_drivers.python.models.ase_model import ASEModel

        return ASEModel
    if name == "TLSModel":
        from .mxl_drivers.python.models.tls_model import TLSModel

        return TLSModel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
