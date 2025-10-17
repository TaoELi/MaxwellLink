#!/usr/bin/env python3

from __future__ import annotations
import argparse
import subprocess
import shlex
import time
import shutil
import os
import socket, json
import numpy as np
import struct

try:
    from .models import __drivers__
    from .models.dummy_model import DummyModel
except ImportError:
    from models import __drivers__
    from models.dummy_model import DummyModel

description = """
A Python driver connecting to MaxwellLink, receiving E-field data and returning
the source amplitude vector for a quantum dynamics model.
"""


# helper function to determine whether this processor is the MPI master using mpi4py
def am_master():
    """
    Return True if this process is the MPI master rank (rank 0), otherwise False.

    Notes
    -----
    Attempts to import ``mpi4py`` and query ``COMM_WORLD``. If ``mpi4py`` is not
    available, returns ``True`` by treating the single process as rank 0.
    """

    try:
        from mpi4py import MPI as _MPI

        _COMM = _MPI.COMM_WORLD
        _RANK = _COMM.Get_rank()
    except Exception:
        _COMM = None
        _RANK = 0
    return _RANK == 0


_INT32 = struct.Struct("<i")
_FLOAT64 = struct.Struct("<d")
# numpy dtypes on the wire
DT_FLOAT = np.float64
DT_INT = np.int32

# header width (ASCII, space-padded)
HEADER_LEN = 12

# Message codes similar to i-PI's socket interface
STATUS = b"STATUS"
READY = b"READY"
HAVEDATA = b"HAVEDATA"
NEEDINIT = b"NEEDINIT"
INIT = b"INIT"
POSDATA = b"POSDATA"
GETFORCE = b"GETFORCE"
FORCEREADY = b"FORCEREADY"
STOP = b"STOP"
BYE = b"BYE"

# EM aliases for readability (same wire format)
FIELDDATA = POSDATA
GETSOURCE = GETFORCE
SOURCEREADY = FORCEREADY


class SocketClosed(OSError):
    pass


def _pad12(msg: bytes) -> bytes:
    """
    Left-pad or right-pad a message to a fixed 12-byte ASCII header.

    Parameters
    ----------
    msg : bytes
        Message bytes to send in the fixed-width header.

    Returns
    -------
    bytes
        The message padded with spaces to 12 bytes.

    Raises
    ------
    ValueError
        If the message is longer than the 12-byte header.
    """

    if len(msg) > HEADER_LEN:
        raise ValueError("Header too long")
    return msg.ljust(HEADER_LEN, b" ")


def send_msg(sock: socket.socket, msg: bytes) -> None:
    """
    Send a 12-byte ASCII header (space-padded).

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    msg : bytes
        Message tag to send (e.g., ``b"STATUS"``).
    """

    sock.sendall(_pad12(msg))


def _recvall(sock: socket.socket, n: int) -> bytes:
    """
    Read exactly ``n`` bytes from a socket.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    n : int
        Number of bytes to read.

    Returns
    -------
    bytes
        The data read.

    Raises
    ------
    SocketClosed
        If the peer closes the connection before all bytes are received.
    """

    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise SocketClosed("Peer closed")
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket) -> bytes:
    """
    Receive a 12-byte ASCII header and strip trailing spaces.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    bytes
        The received header (without trailing spaces).
    """

    hdr = _recvall(sock, HEADER_LEN)
    return hdr.rstrip()


def send_array(sock: socket.socket, arr, dtype) -> None:
    """
    Send a NumPy array over a socket using a contiguous C-order memory view.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    arr : array-like
        Array data to send.
    dtype : numpy.dtype
        Data type to cast and send as (e.g., ``np.float64``).
    """

    a = np.asarray(arr, dtype=dtype, order="C")
    sock.sendall(memoryview(a).cast("B"))


def recv_array(sock: socket.socket, shape, dtype):
    """
    Receive a NumPy array of a given shape and dtype from a socket.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    shape : tuple of int
        Expected array shape.
    dtype : numpy.dtype
        Expected dtype (e.g., ``np.float64``).

    Returns
    -------
    numpy.ndarray
        The received array with the given shape and dtype.

    Raises
    ------
    SocketClosed
        If the peer closes the connection during the transfer.
    """

    out = np.empty(shape, dtype=dtype, order="C")
    mv = memoryview(out).cast("B")
    need = mv.nbytes
    got = 0
    while got < need:
        r = sock.recv_into(mv[got:], need - got)
        if r == 0:
            raise SocketClosed("Peer closed")
        got += r
    return out


def send_int(sock: socket.socket, x: int) -> None:
    """
    Send a 32-bit little-endian integer.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    x : int
        Integer value to send.
    """

    sock.sendall(_INT32.pack(int(x)))


def recv_int(sock: socket.socket) -> int:
    """
    Receive a 32-bit little-endian integer.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    int
        The received integer.

    Raises
    ------
    SocketClosed
        If the peer closes the connection during the transfer.
    """

    buf = bytearray(_INT32.size)
    mv = memoryview(buf)
    got = 0
    while got < _INT32.size:
        r = sock.recv_into(mv[got:], _INT32.size - got)
        if r == 0:
            raise SocketClosed("Peer closed")
        got += r
    return _INT32.unpack(buf)[0]


def send_bytes(sock: socket.socket, b: bytes) -> None:
    """
    Send a length-prefixed byte string.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    b : bytes
        Byte string to send. The length is sent first as a 32-bit integer.
    """

    send_int(sock, len(b))
    if len(b):
        sock.sendall(b)


def recv_bytes(sock: socket.socket) -> bytes:
    """
    Receive a length-prefixed byte string.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    bytes
        The received byte string (may be empty).
    """

    n = recv_int(sock)
    return _recvall(sock, n) if n else b""


def recv_posdata(sock: socket.socket):
    """
    Read a POSDATA / FIELDDATA block from the socket.

    Returns
    -------
    tuple
        ``(cell, icell, xyz)`` where:
        - ``cell`` : ``(3, 3)`` ndarray (row-major), simulation cell.
        - ``icell`` : ``(3, 3)`` ndarray (row-major), inverse cell.
        - ``xyz`` : ``(nat, 3)`` ndarray of positions (or effective field payload).
    """

    cell = recv_array(sock, (3, 3), DT_FLOAT).T.copy()
    icell = recv_array(sock, (3, 3), DT_FLOAT).T.copy()
    nat = recv_int(sock)
    xyz = recv_array(sock, (nat, 3), DT_FLOAT)
    return cell, icell, xyz


def send_force_ready(
    sock: socket.socket,
    energy_ha: float,
    forces_Nx3_ha_per_bohr,
    virial_3x3_ha,
    more: bytes = b"",
):
    """
    Send a FORCEREADY/SOURCEREADY message with energy, forces, virial, and extras.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    energy_ha : float
        Total energy (Hartree).
    forces_Nx3_ha_per_bohr : array-like
        Forces as an ``(N, 3)`` array (Hartree/Bohr).
    virial_3x3_ha : array-like
        Virial tensor as a ``(3, 3)`` array (Hartree).
    more : bytes, optional
        Extra payload as bytes (length-prefixed), e.g., JSON metadata.
    """

    send_msg(sock, FORCEREADY)
    send_array(sock, np.array([energy_ha], dtype=DT_FLOAT), DT_FLOAT)
    forces = np.asarray(forces_Nx3_ha_per_bohr, dtype=DT_FLOAT)
    assert forces.ndim == 2 and forces.shape[1] == 3
    send_int(sock, forces.shape[0])
    send_array(sock, forces, DT_FLOAT)
    send_array(sock, np.asarray(virial_3x3_ha, dtype=DT_FLOAT).T, DT_FLOAT)
    send_bytes(sock, more)


# the above functions can be also obtained from maxwelllink.sockets,
# but we copy them here to avoid circular imports.


def read_value(s):
    """
    Attempt to parse a string as ``int`` or ``float``; fall back to string/boolean.

    Parameters
    ----------
    s : str
        Input token.

    Returns
    -------
    int or float or bool or str
        Parsed value.
    """

    s = s.strip()
    for cast in (int, float):
        try:
            return cast(s)
        except ValueError:
            continue
    if s.lower() == "false":
        return False
    if s.lower() == "true":
        return True
    return s


def read_args_kwargs(input_str):
    """
    Parse a comma-separated string into positional and keyword arguments.

    Parameters
    ----------
    input_str : str
        Comma-separated tokens. Positional values are bare; keyword values use
        ``key=value``. Booleans accept ``true``/``false`` (case-insensitive).

    Returns
    -------
    tuple
        ``(args, kwargs)`` where ``args`` is a list and ``kwargs`` is a dict.
    """

    args = []
    kwargs = {}
    tokens = input_str.split(",")
    for token in tokens:
        token = token.strip()
        if "=" in token:
            key, value = token.split("=", 1)
            kwargs[key.strip()] = read_value(value)
        elif len(token) > 0:
            args.append(read_value(token))
    return args, kwargs


def run_driver(
    unix=False,
    address="localhost",
    port: int = 31415,
    timeout: float = 600.0,
    driver=DummyModel(),
    sockets_prefix="/tmp/socketmxl_",
):
    """
    Run the socket driver loop to communicate with MaxwellLink.

    Parameters
    ----------
    unix : bool, default: False
        Use a UNIX domain socket if ``True``; otherwise use TCP/IP.
    address : str, default: "localhost"
        Hostname (TCP/IP) or UNIX socket name (when ``unix=True``).
    port : int, default: 31415
        TCP/IP port (ignored for UNIX sockets).
    timeout : float, default: 600.0
        Socket timeout in seconds.
    driver : DummyModel, default: DummyModel()
        Quantum dynamics model implementing the driver interface.
    sockets_prefix : str, default: "/tmp/socketmxl_"
        Prefix for UNIX domain socket paths (ignored for TCP/IP).

    Notes
    -----
    Implements a simple message protocol with headers such as ``STATUS``,
    ``INIT``, ``POSDATA``/``FIELDDATA``, ``GETFORCE``/``GETSOURCE``, and ``STOP``.
    """

    if unix:
        sock = socket.socket(socket.AF_UNIX)
        sock.connect(sockets_prefix + address)
    else:
        sock = socket.socket(socket.AF_INET)
        # NEW: keepalive + nodelay for low-latency, long-lived tiny messages
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, AttributeError):
            pass

        sock.connect((address, port))
        sock.settimeout(timeout)

    initialized = False
    have_result = False
    pending_amp = None
    additional_data = {}
    dt_au = 0.0
    molid = None

    while True:
        try:
            msg = recv_msg(sock)
        except Exception:
            # Treat EOF/timeouts during normal shutdown as clean exit
            break

        if msg == STATUS:
            # Server is polling; we must reply with our state.
            if not initialized:
                send_msg(sock, NEEDINIT)
            elif have_result:
                send_msg(sock, HAVEDATA)
            else:
                send_msg(sock, READY)

        elif msg == INIT:
            # Server sends INIT after we replied NEEDINIT
            molid = recv_int(sock)
            init_json = json.loads(recv_bytes(sock).decode("utf-8") or "{}")
            dt_au = float(init_json.get("dt_au", 0.0))
            print("[initialization] Time step in atomic units:", dt_au)
            print("[initialization] Assigned a molecular ID:", molid)

            driver.initialize(dt_au, molid)

            initialized = True
            print("[initialization] Finished initialization for molecular ID:", molid)

        elif msg == FIELDDATA or msg == b"POSDATA":
            # One step of data from server: treat "positions" as the E-field vector in a.u.
            # This is to mirror i-pi's existing socket interface.
            cell, icell, xyz = recv_posdata(sock)
            # effective [Ex, Ey, Ez] (a.u.) for this molecule
            E = xyz[0]
            # Stage the step (no commit)
            driver.stage_step(E)

            have_result = True

        elif msg == GETSOURCE or msg == b"GETFORCE":
            # Server asks us to return the result for this step
            if not driver.have_result():
                # it means the driver code was terminated during driver.propagate() and driver.calc_amp_vector()
                # one way is to be defensive: return zero if we somehow got here without a computed result
                pending_amp = np.zeros(3, float)
            else:
                pending_amp = driver.commit_step()
                additional_data = driver.append_additional_data()
            send_force_ready(
                sock,
                energy_ha=0.0,
                forces_Nx3_ha_per_bohr=pending_amp.reshape(1, 3),
                virial_3x3_ha=np.zeros((3, 3)),
                more=json.dumps(
                    additional_data,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"),
            )
            have_result = False
            pending_amp = None

        elif msg == STOP:
            # Acknowledge and leave gracefully
            try:
                send_msg(sock, BYE)
            finally:
                print("Received STOP, exiting")
                break
        else:
            raise RuntimeError(f"Unexpected header: {msg!r}")


def mxl_driver_main():
    """
    Parse CLI arguments and start the MaxwellLink socket driver.

    Notes
    -----
    Constructs the selected model via ``__drivers__`` using the ``--model`` and
    ``--param`` options, then calls ``run_driver(...)``.
    """

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "-u",
        "--unix",
        action="store_true",
        default=False,
        help="Use a UNIX domain socket.",
    )
    parser.add_argument(
        "-a",
        "--address",
        type=str,
        default="localhost",
        help="Host name (for INET sockets) or name of the UNIX domain socket to connect to.",
    )
    parser.add_argument(
        "-S",
        "--sockets_prefix",
        type=str,
        default="/tmp/socketmxl_",
        help="Prefix used for the unix domain sockets. Ignored when using TCP/IP sockets.",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=31415,
        help="TCP/IP port number. Ignored when using UNIX domain sockets.",
    )
    parser.add_argument(
        "-m",
        "--model",
        type=str,
        default="dummy",
        choices=list(__drivers__.keys()),
        help="""Type of molecular / material model for computing dipole moments under EM field.
        """,
    )
    parser.add_argument(
        "-o",
        "--param",
        type=str,
        default="",
        help="""Parameters required to run the driver. Comma-separated list of values
        """,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Verbose output.",
    )

    args = parser.parse_args()

    driver_args, driver_kwargs = read_args_kwargs(args.param)

    if args.model in __drivers__:
        try:
            d_f = __drivers__[args.model](
                *driver_args, verbose=args.verbose, **driver_kwargs
            )
        except ImportError:
            # specific errors have already been triggered
            raise
        except Exception as err:
            print(f"Error setting up molecular dynamics model {args.model}")
            print(__drivers__[args.model].__doc__)
            print("Error trace: ")
            raise err
    elif args.model == "dummy":
        d_f = DummyModel(verbose=args.verbose)
    else:
        raise ValueError("Unsupported driver model ", args.model)

    run_driver(
        unix=args.unix,
        address=args.address,
        port=args.port,
        driver=d_f,
        sockets_prefix=args.sockets_prefix,
    )


def _clean_env_for_subprocess():
    """
    Return a copy of the environment with MPI-related variables removed.

    Returns
    -------
    dict
        Sanitized environment dictionary suitable for launching child processes.
    """

    env = os.environ.copy()
    # Nuke anything that makes a child think it's an MPI rank
    prefixes = (
        "PMI_",
        "PMIX_",
        "OMPI_",
        "MPI_",
        "MPICH_",
        "I_MPI_",
        "HYDRA_",
        "SLURM_",
        "FI_",
        "UCX_",
        "PSM2_",
        "PMI",
    )
    for k in list(env.keys()):
        for p in prefixes:
            if k.startswith(p):
                env.pop(k, None)
                break
    # Some MPIs set these exact names without a prefix
    for k in ("PMI_FD", "PMI_PORT", "PMI_ID", "PMI_RANK", "PMI_SIZE"):
        env.pop(k, None)
    return env


def launch_driver(
    command='--model tls --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4" --verbose',
    sleep_time=0.5,
):
    """
    Launch the driver as a background subprocess for local testing.

    Parameters
    ----------
    command : str, default: '--model tls --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4" --verbose'
        Command-line arguments passed to ``mxl_driver.py``.
    sleep_time : float, default: 0.5
        Time to sleep (seconds) after launch to allow initialization.

    Returns
    -------
    subprocess.Popen or None
        The process handle on the master rank, otherwise ``None``.
    """

    if am_master():
        print(f"Launching driver with command: mxl_driver.py {command}")
        # launch the external driver (client)
        driver_argv = shlex.split(shutil.which("mxl_driver.py") + " " + command)
        # Use a fresh, non-blocking subprocess; inherit env/stdio for easy debugging
        proc = subprocess.Popen(driver_argv, env=_clean_env_for_subprocess())
        time.sleep(sleep_time)
        return proc
    else:
        return None


def terminate_driver(proc, timeout=2.0):
    """
    Terminate a driver process launched by ``launch_driver``.

    Parameters
    ----------
    proc : subprocess.Popen or None
        Process handle to terminate.
    timeout : float, default: 2.0
        Seconds to wait for graceful shutdown before escalating.
    """

    if proc is not None and am_master():
        # Give it a moment to shut down naturally after the sim closes the socket
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.terminate()
            print("Driver did not exit cleanly, sent terminate signal")
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                print("Driver did not terminate, sent kill signal")


if __name__ == "__main__":
    mxl_driver_main()
