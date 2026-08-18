# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Run-time trajectory output of the classical-MD drivers, one file per driver process.
"""

import os

import numpy as np

#: The per-system quantities every MD driver records, before the force field's own.
BASE_RECORD_NAMES = ("temperature_K", "energy_au")


def trajectory_filename(filename, first_molecule_id):
    """
    Return ``filename`` with ``_id_<first molecule ID>`` inserted before its extension.

    Every driver process -- a scalar driver or one batch bridge -- writes its own file,
    and several of them run from one command line, so the name carries the first
    molecule ID the process owns, as the checkpoints do.
    """

    stem, extension = os.path.splitext(str(filename))
    return f"{stem}_id_{int(first_molecule_id)}{extension}"


def _resolve_format(path):
    """Return ``(path, use_h5)``: HDF5 when possible, else the ``.npz`` fallback."""

    if path.lower().endswith(".npz"):
        return path, False
    try:
        import h5py  # noqa: F401
    except ImportError:
        fallback = os.path.splitext(path)[0] + ".npz"
        print(f"[MD trajectory] h5py is not installed; writing {fallback} instead.")
        return fallback, False
    return path, True


class TrajectoryRecorder:
    """
    Per-system scalars of an MD run, streamed to an HDF5 file or buffered for an NPZ.

    Every ``record_every_steps`` steps :meth:`record` takes one value per system and
    per name. HDF5 files grow as the run goes (each record is written and flushed at
    once, so a run that stops early still leaves a readable file); NPZ files are held
    in memory and written by :meth:`close`.

    Parameters
    ----------
    path : str
        Output file, HDF5 unless it ends in ``.npz`` or ``h5py`` is not installed, in
        which case an ``.npz`` file of that name is written instead. Missing folders
        are created.
    names : sequence of str
        The recorded quantities; each becomes a dataset of shape ``(n_records, num)``.
    molecule_ids : sequence of int
        The systems in column order, stored as ``molecule_ids``.
    dt_au : float
        The time step, stored as ``dt_au``.
    record_every_steps : int, default: 1
        Record every this many steps.
    record_max_steps : int, optional
        Stop recording after this many records; ``None`` for no cap.
    append : bool, default: False
        Continue an existing file of the same layout (a restart) instead of
        overwriting it.
    """

    def __init__(
        self,
        path,
        names,
        molecule_ids,
        dt_au,
        record_every_steps=1,
        record_max_steps=None,
        append=False,
    ):
        self.path, use_h5 = _resolve_format(str(path))
        self.names = tuple(str(name) for name in names)
        self.molecule_ids = np.asarray(molecule_ids, dtype=int)
        self.dt_au = float(dt_au)
        self.record_every_steps = int(record_every_steps)
        if self.record_every_steps < 1:
            raise ValueError("record_every_steps must be a positive integer.")
        self.record_max_steps = (
            None if record_max_steps is None else int(record_max_steps)
        )
        self.n_records = 0
        self._h5 = None
        self._buffer = None

        num = self.molecule_ids.size
        exists = append and os.path.exists(self.path)
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        if use_h5:
            import h5py

            self._h5 = h5py.File(self.path, "a" if exists else "w")
            if exists and "time_au" in self._h5:
                self.n_records = int(self._h5["time_au"].shape[0])
            else:
                chunk = max(1, min(64, (1 << 20) // (8 * max(num, 1))))
                self._h5.create_dataset(
                    "time_au",
                    shape=(0,),
                    maxshape=(None,),
                    dtype=np.float64,
                    chunks=(64,),
                )
                for name in self.names:
                    self._h5.create_dataset(
                        name,
                        shape=(0, num),
                        maxshape=(None, num),
                        dtype=np.float64,
                        chunks=(chunk, num),
                    )
                self._h5.create_dataset("molecule_ids", data=self.molecule_ids)
                self._h5.attrs["dt_au"] = self.dt_au
                self._h5.attrs["record_every_steps"] = self.record_every_steps
        else:
            self._buffer = {"time_au": [], **{name: [] for name in self.names}}
            if exists:
                with np.load(self.path) as old:
                    for key, rows in self._buffer.items():
                        if key in old:
                            rows.extend(np.asarray(old[key]))
                self.n_records = len(self._buffer["time_au"])

    def record(self, step_index, time_au, values):
        """
        Take one record if ``step_index`` falls on the recording stride.

        Parameters
        ----------
        step_index : int
            Number of production steps taken so far, counting from one.
        time_au : float
            The time after that step, in atomic units.
        values : array-like of float, shape (num, len(names))
            One value per system and per name, in the order of ``names``.
        """

        if int(step_index) % self.record_every_steps != 0:
            return
        if (
            self.record_max_steps is not None
            and self.n_records >= self.record_max_steps
        ):
            return
        values = np.asarray(values, dtype=np.float64).reshape(
            self.molecule_ids.size, -1
        )
        n = self.n_records
        if self._h5 is not None:
            self._h5["time_au"].resize(n + 1, axis=0)
            self._h5["time_au"][n] = float(time_au)
            for column, name in enumerate(self.names):
                dataset = self._h5[name]
                dataset.resize(n + 1, axis=0)
                dataset[n] = values[:, column]
            self._h5.flush()
        else:
            self._buffer["time_au"].append(float(time_au))
            for column, name in enumerate(self.names):
                self._buffer[name].append(values[:, column].copy())
        self.n_records = n + 1

    def close(self):
        """Close the HDF5 file, or write the buffered NPZ file. Safe to call twice."""

        if self._h5 is not None:
            self._h5.close()
            self._h5 = None
        elif self._buffer is not None:
            np.savez(
                self.path,
                time_au=np.asarray(self._buffer["time_au"]),
                molecule_ids=self.molecule_ids,
                dt_au=self.dt_au,
                record_every_steps=self.record_every_steps,
                **{
                    name: np.asarray(self._buffer[name]).reshape(
                        -1, self.molecule_ids.size
                    )
                    for name in self.names
                },
            )
            self._buffer = None
