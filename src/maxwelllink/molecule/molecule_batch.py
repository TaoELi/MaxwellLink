# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""User-facing batches of socket-mode MaxwellLink molecules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
import json
from numbers import Integral

from maxwelllink.mxl_drivers.python.models import __drivers__
from maxwelllink.sockets import AggregatedSocketHub

from .molecule import Molecule


class MoleculeBatch(Sequence):
    """
    A homogeneous sequence of molecules served by one batched driver process.

    The first implementation supports the ``"cpu"`` backend.  It runs one
    independent instance of an existing MaxwellLink Python driver per molecule
    inside a single ``mxl_bridge`` process.  The contained objects remain
    ordinary socket-mode :class:`Molecule` instances, so existing EM solvers
    can consume a ``MoleculeBatch`` anywhere they accept a molecule sequence.

    Parameters
    ----------
    num : int
        Number of molecular drivers in the batch.
    hub : maxwelllink.AggregatedSocketHub
        Shared aggregate socket hub used by every molecule in the batch.
    group_id : str
        Non-empty aggregate group name.  One batch maps to one bridge group.
    driver : str
        Name of an existing Python driver in MaxwellLink's driver registry.
    driver_kwargs : mapping or None, optional
        Keyword arguments used to construct every driver instance.  Values
        must be JSON-serializable because ``mxl_bridge`` reads them from the
        aggregation manifest.
    backend : str, default: ``"cpu"``
        Batch execution backend.  Only ``"cpu"`` is implemented currently.
    store_additional_data : bool, default: False
        Whether each contained molecule retains its complete additional-data
        history.  ``False`` keeps only the latest five frames per molecule.
    """

    def __init__(
        self,
        num,
        hub,
        group_id,
        driver,
        driver_kwargs=None,
        backend="cpu",
        store_additional_data=False,
    ):
        if isinstance(num, bool) or not isinstance(num, Integral) or num <= 0:
            raise ValueError("num must be a positive integer.")
        if not isinstance(hub, AggregatedSocketHub):
            raise TypeError("MoleculeBatch requires an AggregatedSocketHub.")

        normalized_group = str(group_id).strip()
        if not normalized_group:
            raise ValueError("group_id must be a non-empty string.")

        normalized_backend = str(backend).strip().lower()
        if normalized_backend != "cpu":
            raise ValueError(
                f"Unsupported batch backend {backend!r}; only 'cpu' is available."
            )

        normalized_driver = str(driver).strip().lower()
        if normalized_driver not in __drivers__:
            raise ValueError(
                f"Unsupported driver {driver!r}; available Python drivers are "
                f"{list(__drivers__.keys())}."
            )

        if driver_kwargs is None:
            kwargs = {}
        elif isinstance(driver_kwargs, Mapping):
            kwargs = deepcopy(dict(driver_kwargs))
        else:
            raise TypeError("driver_kwargs must be a mapping or None.")
        try:
            json.dumps(kwargs, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                "driver_kwargs must be JSON-serializable for mxl_bridge."
            ) from exc

        self.num = int(num)
        self.hub = hub
        self.group_id = normalized_group
        self.driver = normalized_driver
        self.driver_kwargs = kwargs
        self.backend = normalized_backend
        self.store_additional_data = bool(store_additional_data)

        self._mxl_batch_config = {
            "backend": self.backend,
            "driver": self.driver,
            "driver_kwargs": deepcopy(self.driver_kwargs),
        }
        self._molecules = tuple(self._new_molecule() for _ in range(self.num))

    def _new_molecule(self) -> Molecule:
        """Construct one ordinary socket-mode molecule for this batch."""

        return Molecule(
            hub=self.hub,
            init_payload={"aggregate_group": self.group_id},
            store_additional_data=self.store_additional_data,
        )

    def __len__(self):
        """Return the number of molecules in the batch."""

        return len(self._molecules)

    def __getitem__(self, index):
        """Return one molecule, or a list of molecules for a slice."""

        if isinstance(index, slice):
            return list(self._molecules[index])
        return self._molecules[index]


__all__ = ["MoleculeBatch"]
