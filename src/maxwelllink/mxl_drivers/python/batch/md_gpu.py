# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched classical molecular-dynamics (MD) driver for the aggregated socket hub,
with thousands of independent MD systems (same force field and topology) advance together.
"""

import os

import numpy as np

from maxwelllink.matter.helpers import resolve_backend, to_host
from maxwelllink.matter.md.md_engine import MDEngine

from .dummy_gpu import DummyBatchModel

# Additional-data field names, in the component order the dipole arrays hold.
_HALF_DIPOLE_KEYS = ("mux_au", "muy_au", "muz_au")
_FORCE_DIPOLE_KEYS = ("mux_m_au", "muy_m_au", "muz_m_au")


class MDGPUBatchModel(DummyBatchModel):
    """
    Vectorized classical-MD batch model: the aggregated-hub front end of the
    shared :class:`~maxwelllink.matter.md.md_engine.MDEngine`.
    """

    def __init__(
        self,
        *,
        num,
        driver_kwargs,
        backend=None,
        driver_args=None,
        store_additional_data=False,
    ):
        """
        Initialize a batch of identical MD systems.

        Parameters
        ----------
        num : int
            Number of MD systems in the batch; must be positive.
        driver_kwargs : mapping
            Keyword arguments of the scalar ``MDModel`` driver, e.g.
            ``{"ff": "co2jcp2021", "thermostat": "nve", "pre_nvt": True}``; they
            are forwarded to the shared engine unchanged.
        backend : {'cpu', 'gpu', None}, default: None
            The backend choice, matching ``matter.MD`` and the scalar
            ``MDModel``: ``'gpu'`` runs the CUDA kernels through CuPy, ``'cpu'``
            runs the same compiled physics through numba on the host, and
            ``None`` (auto) picks the GPU when CuPy reaches a CUDA device.
        driver_args : sequence, optional
            Positional arguments of the scalar ``MDModel`` driver.
        store_additional_data : bool, default: False
            Accepted for parity with the other batch models. The extras are always
            built, because the cavity solvers read the dipole from them.

        Raises
        ------
        ValueError
            If ``num`` is not positive, or ``verbose`` is enabled.
        """

        driver_kwargs = dict(driver_kwargs or {})
        if driver_kwargs.get("verbose"):
            raise ValueError(
                "the MD batch model does not support verbose=True; per-system "
                "log streams defeat the purpose of batching."
            )
        self.store_additional_data = bool(store_additional_data)
        self.backend, xp = resolve_backend(backend)
        self.engine = MDEngine(
            *tuple(driver_args or ()),
            **driver_kwargs,
            num=num,
            xp=xp,
            label="MDGPUBatchModel",
        )
        self.num = self.engine.num

    # ----------------------- heavy-load initialization ------------------------------

    def initialize(self, dt_au, molecule_ids):
        """
        Allocate the batch state and optionally restore a checkpoint or
        pre-equilibrate.

        Parameters
        ----------
        dt_au : float
            The shared time step in atomic units (a.u.).
        molecule_ids : array-like of int
            Molecule IDs assigned by the hub; must number exactly ``num``.
        """

        restart_state = None
        if self.engine.restart and self.engine.checkpoint:
            ids = [int(mid) for mid in molecule_ids]
            first = ids[0] if ids else 0
            restart_state = self._read_checkpoint(
                "md_batch_checkpoint_id_%d.npz" % first
            )
        self.engine.initialize(dt_au, molecule_ids, restart_state=restart_state)

    # ----------------------- one FDTD step under E-field ----------------------------

    def step(self, efield_au):
        """
        Advance every system by one OBABO velocity-Verlet step under the field.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Contiguous host array of effective electric field vectors in a.u.

        Returns
        -------
        maxwelllink.matter.md.md_engine.MDStepResult
            Columnar amplitude (``dmu/dt``), half-step dipole, force-time dipole,
            and energy for every system -- the same four fields as
            ``BatchStepResult``, which is all the bridge reads.
        """

        return self.engine.step(efield_au)

    # ------------ optional data / checkpoint --------------

    def append_additional_data(self):
        """
        Return per-system dicts matching ``MDModel.append_additional_data``.

        Returns
        -------
        list of dict
            One dictionary per system with ``time_au``, ``energy_au``,
            ``potential_au``, the half-step dipole (``mux_au``/``muy_au``/``muz_au``)
            and the force-time dipole (``mux_m_au``/``muy_m_au``/``muz_m_au``).

        Raises
        ------
        RuntimeError
            If called before :meth:`initialize`.
        """

        engine = self.engine
        if engine.x is None:
            raise RuntimeError("append_additional_data() called before initialize().")
        mu_half = to_host(engine._dipole_half)
        mu_force = to_host(engine._dipole_force)
        energy = to_host(engine._energy)
        potential = to_host(engine.potential)
        return [
            {
                "time_au": engine.t,
                "energy_au": float(energy[i]),
                "potential_au": float(potential[i]),
                "mux_au": float(mu_half[i, 0]),
                "muy_au": float(mu_half[i, 1]),
                "muz_au": float(mu_half[i, 2]),
                "mux_m_au": float(mu_force[i, 0]),
                "muy_m_au": float(mu_force[i, 1]),
                "muz_m_au": float(mu_force[i, 2]),
            }
            for i in range(self.num)
        ]

    def append_additional_data_columns(self, keys):
        """
        Return the requested additional-data fields as one contiguous block.

        The engine already holds every field as an array, so this copies only the
        columns the hub asked for and never builds a dictionary per system.

        Parameters
        ----------
        keys : sequence of str
            Field names to return, in column order.

        Returns
        -------
        numpy.ndarray of float, shape (num, len(keys))
            The requested fields, one row per system.

        Raises
        ------
        KeyError
            If a requested field is not one this driver reports.
        """

        engine = self.engine
        columns = {}
        if any(key in _HALF_DIPOLE_KEYS for key in keys):
            columns.update(zip(_HALF_DIPOLE_KEYS, to_host(engine._dipole_half).T))
        if any(key in _FORCE_DIPOLE_KEYS for key in keys):
            columns.update(zip(_FORCE_DIPOLE_KEYS, to_host(engine._dipole_force).T))
        if "energy_au" in keys:
            columns["energy_au"] = to_host(engine._energy)
        if "potential_au" in keys:
            columns["potential_au"] = to_host(engine.potential)
        if "time_au" in keys:
            columns["time_au"] = np.full(self.num, engine.t)
        return np.ascontiguousarray(np.column_stack([columns[key] for key in keys]))

    def save_checkpoint(self, filename=None):
        """
        Write the whole batch state to one ``.npz`` file.

        Parameters
        ----------
        filename : str, optional
            Target path. Defaults to a name built from the first molecule ID.
        """

        np.savez(filename or self._checkpoint_filename(), **self.engine.state_dict())

    def load_checkpoint(self, filename=None):
        """
        Restore the batch state written by :meth:`save_checkpoint`.

        Parameters
        ----------
        filename : str, optional
            Source path. Defaults to a name built from the first molecule ID.

        Raises
        ------
        ValueError
            If the checkpoint holds a differently shaped batch.
        """

        state = self._read_checkpoint(filename or self._checkpoint_filename())
        if state is not None:
            self.engine.load_state_dict(state)

    def _read_checkpoint(self, path):
        """
        Read a batch checkpoint file into an engine ``restart_state`` dict.
        """

        if not os.path.exists(path):
            print(
                "[checkpoint] No batch checkpoint file %r found, starting fresh." % path
            )
            return None
        data = np.load(path)
        if data["x"].shape != (self.num, self.engine.n_atoms, 3):
            raise ValueError(
                f"Checkpoint {path!r} holds {data['x'].shape} state, expected "
                f"{(self.num, self.engine.n_atoms, 3)}."
            )
        state = {
            "time": float(data["time"]),
            "x": data["x"],
            "p": data["p"],
            "F": data["F"],
        }
        if "mu_initial" in data:  # keep reporting against the original baseline
            state["mu_initial"] = data["mu_initial"]
        return state

    def _checkpoint_filename(self):
        """Default checkpoint filename, built from the first molecule ID."""

        ids = self.engine.molecule_ids
        first = ids[0] if ids else 0
        return "md_batch_checkpoint_id_%d.npz" % first

    def close(self):
        """
        Drop the batch state, writing a checkpoint first when one was requested.
        """

        if self.engine.x is not None and self.engine.checkpoint:
            self.save_checkpoint()
        self.engine.close()
