# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Run-time output of the molecular drivers, one file per driver process: per-molecule
properties to HDF5 or NPZ, and positions and/or velocities to extended XYZ.
"""

import os

import numpy as np

from maxwelllink.units import BOHR_PER_ANG

__all__ = ["output_filename", "PropertyRecorder", "XYZTrajectoryWriter"]


def output_filename(filename, first_molecule_id):
    """
    Return ``filename`` with ``_id_<first molecule ID>`` inserted before its extension.

    Every driver process -- a scalar driver or one batch bridge -- writes its own file,
    and several of them run from one command line, so the name carries the first
    molecule ID the process owns, as the checkpoints do.
    """

    stem, extension = os.path.splitext(str(filename))
    return f"{stem}_id_{int(first_molecule_id)}{extension}"


def _resolve_property_format(path):
    """Return ``(path, use_h5)``: HDF5 when possible, else the ``.npz`` fallback."""

    if path.lower().endswith(".npz"):
        return path, False
    try:
        import h5py  # noqa: F401
    except ImportError:
        fallback = os.path.splitext(path)[0] + ".npz"
        print(f"[record] h5py is not installed; writing {fallback} instead.")
        return fallback, False
    return path, True


class PropertyRecorder:
    """
    Per-molecule scalars of a run, streamed to an HDF5 file or buffered for an NPZ.

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
        self.path, use_h5 = _resolve_property_format(str(path))
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


class XYZTrajectoryWriter:
    """
    Positions, atomic velocities, and/or body-frame angular velocities with
    principal moments of inertia streamed to one extended-XYZ file, with one
    frame per independent system. Each system may contain multiple physical molecules.

    Every ``record_every_steps`` steps :meth:`write` appends one frame per system,
    in ``molecule_ids`` order. The file is time-major: with ``num`` systems, system
    ``k`` occupies every ``num``-th frame starting at frame ``k``.

    For files containing positions, those frames can be read using
    ``ase.io.read(path, index=slice(k, None, num))`` or
    :func:`maxwelllink.tools.read_xyz_trajectory`. The latter returns positions
    only, including for ``traj_content="all"``, and must not be used for files
    containing only velocities or angular velocities.

    The comment line carries ``time_au``, ``step`` and ``molecule_id``. Extra
    per-atom scalar columns (e.g. Mulliken charges) are declared in ``per_atom``;
    all selected columns appear in the extended-XYZ ``Properties`` string.
    Positions are written in Angstrom; atomic velocities are in Bohr per atomic
    time. Body-frame angular velocities are in inverse atomic time and always
    accompanied by the corresponding principal moments of inertia in electron-mass
    times Bohr-squared units. The angular block has six columns: three angular
    velocities followed by three moments, repeated for every atom of the molecule.
    They are declared as ``angular_velocity_body_au:R:3`` and
    ``principal_moments_au:R:3`` in ``Properties``.

    Parameters
    ----------
    path : str
        Output file; missing folders are created.
    symbols : sequence of str
        Element symbol of every atom, in the order used by the selected vectors.
    molecule_ids : sequence of int
        The independent system IDs, in frame order.
    dt_au : float
        The time step in atomic units, written into every frame's comment line.
    record_every_steps : int, default: 1
        Write every this many steps.
    record_max_steps : int, optional
        Stop after this many records; ``None`` for no cap.
    append : bool, default: False
        Continue an existing file (a restart) instead of overwriting it.
        The existing file must use the same ``traj_content`` and ``per_atom``
        layout; its columns are not validated before appending.
    per_atom : sequence of str, default: ()
        Names of extra per-atom scalar columns :meth:`write` receives.
    traj_content : str, default: "positions"
        Content to record: "positions", "velocities", "angular_velocity_body",
        or "all". Angular output always includes the three corresponding principal
        moments of inertia. "all" records positions, atomic velocities, and this
        six-column angular block.
    """

    def __init__(
        self,
        path,
        symbols,
        molecule_ids,
        dt_au,
        record_every_steps=1,
        record_max_steps=None,
        append=False,
        per_atom=(),
        traj_content="positions",
    ):
        self.path = str(path)
        self.symbols = [str(symbol) for symbol in symbols]
        self.molecule_ids = [int(mid) for mid in molecule_ids]
        self.dt_au = float(dt_au)
        self.record_every_steps = int(record_every_steps)
        if self.record_every_steps < 1:
            raise ValueError("record_every_steps must be a positive integer.")
        self.record_max_steps = (
            None if record_max_steps is None else int(record_max_steps)
        )
        self.per_atom = tuple(str(name) for name in per_atom)
        if traj_content not in ("positions", "velocities", "angular_velocity_body", "all",):
            raise ValueError(
                "traj_content must be positions, velocities, "
                "angular_velocity_body, or all."
            )

        self.save_positions = traj_content in ("positions", "all")
        self.save_velocities = traj_content in ("velocities", "all")
        self.save_angular_velocity_body = traj_content in ("angular_velocity_body", "all")
        self.n_records = 0

        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        exists = append and os.path.exists(self.path)
        if exists:  # count the frames already there, so the cap keeps its meaning
            header = str(len(self.symbols))
            with open(self.path) as handle:
                n_frames = sum(1 for line in handle if line.strip() == header)
            self.n_records = n_frames // max(len(self.molecule_ids), 1)
        self._handle = open(self.path, "a" if exists else "w")
        properties = "species:S:1"
        if self.save_positions:
            properties += ":pos:R:3"
        if self.save_velocities:
            properties += ":velocities_au:R:3"
        if self.save_angular_velocity_body:
            properties += ":angular_velocity_body_au:R:3"+":principal_moments_au:R:3"
        properties += "".join(f":{name}:R:1" for name in self.per_atom)
        self._comment = (
            f"Properties={properties} dt_au={self.dt_au:.10g} "
            "time_au={time:.10g} step={step:d} molecule_id={mid:d}"
        )

    def write(self, step_index, time_au, positions_bohr=None, per_atom=None, velocities_au=None,
        angular_velocity_body_au=None):
        """
        Append one frame per independent system on the recording stride,
        unless ``record_max_steps`` has been reached.

        Parameters
        ----------
        step_index : int
            Number of production steps taken so far, counting from one.
        time_au : float
            The time after that step, in atomic units.
        positions_bohr : array-like of float, shape (num, n_atom, 3), optional
            Every system's atomic positions in Bohr. Required when positions
            are selected and a frame is written.
        per_atom : dict of str to array-like of float, shape (num, n_atom), optional
            The extra per-atom columns, keyed by the names given at construction.
        velocities_au : array-like of float, shape (num, n_atom, 3), optional
            Every system's atomic velocities in atomic units (Bohr / atomic time).
            Required when velocities are selected and a frame is written.
        angular_velocity_body_au : array-like of float, shape (num, n_atom, 6), optional
            Three molecular body-frame angular velocities followed by their three
            principal moments of inertia, repeated for each atom of the molecule.
            Angular velocities are in inverse atomic time; moments are in
            electron-mass times Bohr-squared units. Required when angular output
            is selected.
        """

        if int(step_index) % self.record_every_steps != 0:
            return
        if (
            self.record_max_steps is not None
            and self.n_records >= self.record_max_steps
        ):
            return
        num, n_atom = len(self.molecule_ids), len(self.symbols)
        vector_columns = []

        if self.save_positions:
            if positions_bohr is None:
                raise ValueError("positions_bohr is required.")
            positions = np.asarray(positions_bohr, dtype=float).reshape(
                num, n_atom, 3
            )
            vector_columns.append(positions / BOHR_PER_ANG)

        if self.save_velocities:
            if velocities_au is None:
                raise ValueError("velocities_au is required.")
            velocities = np.asarray(velocities_au, dtype=float).reshape(
                num, n_atom, 3
            )
            vector_columns.append(velocities)

        if self.save_angular_velocity_body:
            if angular_velocity_body_au is None:
                raise ValueError("angular_velocity_body_au is required.")
            angular_velocity_body = np.asarray(
                angular_velocity_body_au, dtype=float
            ).reshape(num, n_atom, 6)  # 3 for angular velocity, 3 for principal moments
            vector_columns.append(angular_velocity_body)

        extras = [
            np.asarray(per_atom[name], dtype=float).reshape(num, n_atom)
            for name in self.per_atom
        ]
        lines = []
        for k, mid in enumerate(self.molecule_ids):
            lines.append(str(n_atom))
            lines.append(
                self._comment.format(time=float(time_au), step=int(step_index), mid=mid)
            )
            columns = [array[k] for array in vector_columns]
            columns += [extra[k][:, None] for extra in extras]
            values = np.hstack(columns)
            for symbol, row in zip(self.symbols, values):
                lines.append(symbol + " " + " ".join(f"{v:.8f}" for v in row))
        self._handle.write("\n".join(lines) + "\n")
        self._handle.flush()
        self.n_records += 1

    def close(self):
        """Close the file. Safe to call twice."""

        if self._handle is not None:
            self._handle.close()
            self._handle = None
