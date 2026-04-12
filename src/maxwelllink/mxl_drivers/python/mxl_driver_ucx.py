#!/usr/bin/env python3

# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import shlex
import shutil
import socket
import subprocess
import time

import numpy as np

try:
    from .models import __drivers__
    from .models.dummy_model import DummyModel
except ImportError:
    from models import __drivers__
    from models.dummy_model import DummyModel

from ...sockets.ucx_protocol import (
    BYE,
    INIT,
    STEP_REQUEST,
    STOP,
    load_ucx_module,
    pack_bye,
    pack_hello,
    pack_step_response,
    unpack_init,
    unpack_message,
    unpack_step_request,
    init_ucx_module,
)

description = """
A Python driver connecting to MaxwellLink over UCX active messages, receiving
E-field data and returning the source amplitude vector for a quantum dynamics
model.
"""


def _am_master():
    """
    Return True if this process is the MPI master rank (rank 0), otherwise False.
    """

    try:
        from mpi4py import MPI as _MPI

        _COMM = _MPI.COMM_WORLD
        _RANK = _COMM.Get_rank()
    except Exception:
        _COMM = None
        _RANK = 0
    return _RANK == 0


def _read_value(s):
    """
    Attempt to parse a string as ``int`` or ``float``; fall back to string/boolean.
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


def _read_args_kwargs(input_str):
    """
    Parse a comma-separated string into positional and keyword arguments.
    """

    args = []
    kwargs = {}
    tokens = input_str.split(",")
    for token in tokens:
        token = token.strip()
        if "=" in token:
            key, value = token.split("=", 1)
            kwargs[key.strip()] = _read_value(value)
        elif len(token) > 0:
            args.append(_read_value(token))
    return args, kwargs


async def _call_endpoint_method(endpoint, name: str, *args):
    """
    Call one endpoint method, awaiting it when needed.
    """

    method = getattr(endpoint, name, None)
    if method is None:
        raise AttributeError(f"UCX endpoint has no method {name!r}")
    result = method(*args)
    if inspect.isawaitable(result):
        return await result
    return result


async def _am_send(endpoint, payload: bytes):
    """
    Send one active-message payload.
    """

    await _call_endpoint_method(endpoint, "am_send", memoryview(payload))


async def _am_recv(endpoint) -> bytes:
    """
    Receive one active-message payload.
    """

    blob = await _call_endpoint_method(endpoint, "am_recv")
    return bytes(blob)


async def _close_endpoint(endpoint):
    """
    Close or abort one endpoint.
    """

    for method_name in ("close", "abort"):
        method = getattr(endpoint, method_name, None)
        if method is None:
            continue
        try:
            result = method()
            if inspect.isawaitable(result):
                await result
            return
        except Exception:
            continue


async def run_driver_ucx_async(
    address="127.0.0.1",
    port: int = 31415,
    timeout: float = 600.0,
    driver=DummyModel(),
    ucx_module=None,
    ucx_options=None,
):
    """
    Run the UCX driver loop to communicate with MaxwellLink.

    Parameters
    ----------
    address : str, default: "127.0.0.1"
        Hub address to connect to.
    port : int, default: 31415
        Hub port.
    timeout : float, default: 600.0
        Reserved timeout value for parity with the socket driver.
    driver : DummyModel, default: DummyModel()
        Quantum dynamics model implementing the driver interface.
    ucx_module : module or None, optional
        Private dependency-injection hook used by tests.
    ucx_options : dict or None, optional
        Optional UCX runtime options.
    """

    del timeout
    _scrub_inherited_transport_env()
    ucx = ucx_module or load_ucx_module()
    init_ucx_module(ucx, ucx_options)

    create_endpoint = getattr(ucx, "create_endpoint")
    try:
        endpoint = await create_endpoint(address, int(port))
    except TypeError:
        endpoint = await create_endpoint(ip_address=address, port=int(port))

    hello = {
        "driver": type(driver).__name__,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "transport": "ucx",
    }

    try:
        await _am_send(endpoint, pack_hello(hello))

        while True:
            blob = await _am_recv(endpoint)
            message = unpack_message(blob)

            if message.opcode == INIT:
                init_payload = unpack_init(message.payload)
                dt_au = float(init_payload.get("dt_au", 0.0))
                molecule_id = int(init_payload.get("molecule_id", -1))
                print("[initialization] Time step in atomic units:", dt_au)
                print("[initialization] Assigned a molecular ID:", molecule_id)
                driver.initialize(dt_au, molecule_id)
                print(
                    "[initialization] Finished initialization for molecular ID:",
                    molecule_id,
                )
                continue

            if message.opcode == STEP_REQUEST:
                efield = unpack_step_request(message.payload)
                driver.stage_step(efield)
                if not driver.have_result():
                    amp = np.zeros(3, float)
                    additional_data = {}
                else:
                    amp = np.asarray(driver.commit_step(), dtype=float).reshape(3)
                    additional_data = driver.append_additional_data()
                extra = b""
                if additional_data:
                    extra = json.dumps(
                        additional_data,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    extra = extra.encode("utf-8")
                await _am_send(endpoint, pack_step_response(amp, extra=extra))
                continue

            if message.opcode == STOP:
                try:
                    await _am_send(endpoint, pack_bye())
                finally:
                    print("Received STOP, exiting")
                    break

            if message.opcode == BYE:
                break

            raise RuntimeError(f"Unexpected UCX opcode: {message.opcode!r}")
    finally:
        await _close_endpoint(endpoint)


def run_driver_ucx(
    address="127.0.0.1",
    port: int = 31415,
    timeout: float = 600.0,
    driver=DummyModel(),
    ucx_module=None,
    ucx_options=None,
):
    """
    Synchronous wrapper around :func:`run_driver_ucx_async`.
    """

    asyncio.run(
        run_driver_ucx_async(
            address=address,
            port=port,
            timeout=timeout,
            driver=driver,
            ucx_module=ucx_module,
            ucx_options=ucx_options,
        )
    )


def mxl_driver_ucx_main():
    """
    Parse CLI arguments and start the MaxwellLink UCX driver.
    """

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-a",
        "--address",
        type=str,
        default="127.0.0.1",
        help="Hub IP address or hostname.",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=31415,
        help="UCX listener port number.",
    )
    parser.add_argument(
        "-m",
        "--model",
        type=str,
        default="dummy",
        choices=list(__drivers__.keys()),
        help="Type of molecular/material model for computing dipole moments under EM field.",
    )
    parser.add_argument(
        "-o",
        "--param",
        type=str,
        default="",
        help="Parameters required to run the driver. Comma-separated list of values.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Verbose output.",
    )

    args = parser.parse_args()
    driver_args, driver_kwargs = _read_args_kwargs(args.param)

    if args.model in __drivers__:
        try:
            d_f = __drivers__[args.model](
                *driver_args, verbose=args.verbose, **driver_kwargs
            )
        except ImportError:
            raise
        except Exception as err:
            print(f"Error setting up molecular dynamics model {args.model}")
            print(__drivers__[args.model].__doc__)
            print("Error trace: ")
            raise err
    else:
        d_f = DummyModel(verbose=args.verbose)

    run_driver_ucx(address=args.address, port=args.port, driver=d_f)


def _clean_env_for_ucx_subprocess():
    """
    Return a copy of the environment with inherited launcher variables removed.

    The UCX driver is usually spawned as a plain local subprocess rather than
    under an MPI launcher. Inheriting the parent shell's ``MPI_*``/``OMPI_*``
    metadata or ``UCX_*``/``FI_*`` transport pins can force UCX onto a device
    that is not present on the current host, which breaks endpoint creation
    before the MaxwellLink protocol starts.

    Set ``MXL_UCX_KEEP_TRANSPORT_ENV=1`` to preserve ``UCX_*`` and ``FI_*``
    variables for advanced tuning. The older
    ``MXL_DRIVER_UCX_KEEP_TRANSPORT_ENV=1`` alias is also recognized.
    """

    env = os.environ.copy()
    keep_transport_env = (
        env.get(
            "MXL_UCX_KEEP_TRANSPORT_ENV",
            env.get("MXL_DRIVER_UCX_KEEP_TRANSPORT_ENV", ""),
        )
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )
    prefixes = (
        "PMI_",
        "PMIX_",
        "OMPI_",
        "MPI_",
        "MPICH_",
        "I_MPI_",
        "HYDRA_",
        "SLURM_",
        "PMI",
    )
    if not keep_transport_env:
        prefixes = prefixes + ("FI_", "UCX_")
    for k in list(env.keys()):
        for p in prefixes:
            if k.startswith(p):
                env.pop(k, None)
                break
    for k in ("PMI_FD", "PMI_PORT", "PMI_ID", "PMI_RANK", "PMI_SIZE"):
        env.pop(k, None)
    return env


def _scrub_inherited_transport_env() -> None:
    """
    Remove inherited launcher/transport variables from the current process.

    This mirrors :func:`_clean_env_for_ucx_subprocess` so direct invocations of
    ``mxl_driver_ucx`` behave like the launcher helper before UCX initializes.
    """

    cleaned_env = _clean_env_for_ucx_subprocess()
    for key in list(os.environ.keys()):
        if key not in cleaned_env:
            os.environ.pop(key, None)


def launch_driver_ucx(
    command='--model tls --address 127.0.0.1 --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4" --verbose',
    sleep_time=0.5,
):
    """
    Launch the UCX driver as a background subprocess for local testing.

    Parameters
    ----------
    command : str, default: '--model tls --address 127.0.0.1 --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4" --verbose'
        Command-line arguments passed to ``mxl_driver_ucx.py``.
    sleep_time : float, default: 0.5
        Time to sleep after launch so the process can connect.

    Returns
    -------
    subprocess.Popen or None
        Process handle on the MPI master rank, otherwise ``None``.
    """

    if not _am_master():
        return None

    print(f"Launching driver with command: mxl_driver_ucx.py {command}")
    driver_exe = shutil.which("mxl_driver_ucx.py") or shutil.which("mxl_driver_ucx")
    if driver_exe is None:
        raise RuntimeError("Could not find `mxl_driver_ucx.py` on PATH.")
    driver_argv = shlex.split(driver_exe + " " + command)
    proc = subprocess.Popen(driver_argv, env=_clean_env_for_ucx_subprocess())
    time.sleep(sleep_time)
    return proc


def terminate_driver_ucx(proc, timeout=2.0):
    """
    Terminate a driver process launched by :func:`launch_driver_ucx`.

    Parameters
    ----------
    proc : subprocess.Popen or None
        Process handle to terminate.
    timeout : float, default: 2.0
        Seconds to wait for graceful shutdown before escalating.
    """

    if proc is not None and _am_master():
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
    mxl_driver_ucx_main()
