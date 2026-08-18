# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Non-SCC Hamiltonian and overlap tests for the direct DFTB model.
"""

import os

import numpy as np
import pytest

from slko_helpers import sk_path, sk_path_for

dftb_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models.rtdftb_model",
    reason="maxwelllink is required for this test",
)
SlaterKosterSet = dftb_mod.SlaterKosterSet
DFTBSystem = dftb_mod.DFTBSystem
build_h0_overlap = dftb_mod.build_h0_overlap

_REFERENCE = os.path.join(
    os.path.dirname(__file__), os.pardir, "data", "dftb_h0_overlap_reference.npz"
)
_TOLERANCE = 1e-10


def _reference_or_skip():
    """Return the bundled DFTB+ matrices, or skip when they are not available."""
    if not os.path.isfile(_REFERENCE):
        pytest.skip("DFTB+ H0/S reference bundle is not available.")
    return np.load(_REFERENCE, allow_pickle=True)


def _build(reference, name):
    """Build H0 and S for one bundled system with our own kernels."""
    elements = [str(s) for s in reference[f"{name}/elements"]]
    species = sorted(set(elements))
    # the angular momenta DFTB+ was given, carried in the bundle: S and Cl have d
    # shells in 3ob-3-1 while C and O do not, which no simple rule would tell us
    max_l = dict(zip(species, [str(v) for v in reference[f"{name}/max_l"]]))
    sk_set = SlaterKosterSet(
        sk_path_for(str(reference[f"{name}/sk_set"]), species, skip=pytest.skip),
        species,
        max_l,
    )
    system = DFTBSystem(elements, reference[f"{name}/positions_ang"], sk_set)
    return build_h0_overlap(system)


@pytest.mark.core
@pytest.mark.parametrize(
    "name",
    [
        "gs_h2o",
        "gs_ch2o",
        "gs_nh3",
        "gs_znh2o",
        "znoh_gen",
        "znscl_gen",
    ],
)
def test_h0_and_overlap_match_dftbplus(name):
    """Every H0 and S element reproduces the matrix DFTB+ dumped."""
    reference = _reference_or_skip()
    h0, overlap = _build(reference, name)

    assert np.max(np.abs(h0 - reference[f"{name}/h0"])) < _TOLERANCE
    assert np.max(np.abs(overlap - reference[f"{name}/overlap"])) < _TOLERANCE


@pytest.mark.core
def test_matrices_are_symmetric_with_unit_overlap_diagonal():
    """H0 and S come out symmetric, and S has an exact unit diagonal."""
    reference = _reference_or_skip()
    h0, overlap = _build(reference, "gs_znh2o")

    assert np.max(np.abs(h0 - h0.T)) == 0.0
    assert np.max(np.abs(overlap - overlap.T)) == 0.0
    assert np.max(np.abs(np.diag(overlap) - 1.0)) < 1e-14


@pytest.mark.core
def test_spectrum_is_invariant_under_rigid_rotation():
    """Rotating the system leaves the eigenvalues alone, as it must.

    This is the cheap standing guard on the d rotation: an error in the x-odd terms
    survives every planar test geometry but breaks rotational invariance at once.
    """
    reference = _reference_or_skip()
    elements = [str(s) for s in reference["gs_znh2o/elements"]]
    positions = reference["gs_znh2o/positions_ang"]
    species = sorted(set(elements))
    max_l = dict(zip(species, [str(v) for v in reference["gs_znh2o/max_l"]]))
    sk_set = SlaterKosterSet(
        sk_path_for(str(reference["gs_znh2o/sk_set"]), species, skip=pytest.skip),
        species,
        max_l,
    )

    h0, overlap = build_h0_overlap(DFTBSystem(elements, positions, sk_set))
    eig_h0 = np.linalg.eigvalsh(h0)
    eig_overlap = np.linalg.eigvalsh(overlap)

    rng = np.random.default_rng(7)
    for _ in range(5):
        # a random rotation, from the QR factor of a random matrix
        rotation, _ = np.linalg.qr(rng.normal(size=(3, 3)))
        if np.linalg.det(rotation) < 0.0:
            rotation[:, 0] *= -1.0
        turned = positions @ rotation.T + rng.normal(scale=3.0, size=3)
        h0_turned, overlap_turned = build_h0_overlap(
            DFTBSystem(elements, turned, sk_set)
        )
        assert np.max(np.abs(np.linalg.eigvalsh(h0_turned) - eig_h0)) < 1e-12
        assert np.max(np.abs(np.linalg.eigvalsh(overlap_turned) - eig_overlap)) < 1e-12


@pytest.mark.core
def test_every_bundled_parameter_file_parses():
    """The reader handles every file of both sets, in both .skf formats."""
    read_skf = dftb_mod.read_skf

    count = 0
    for parameter_set in ("3ob-3-1", "mio-1-1"):
        folder = sk_path(parameter_set, skip=pytest.skip)
        for name in sorted(os.listdir(folder)):
            if not name.endswith(".skf"):
                continue
            first, second = name[:-4].split("-")
            skf = read_skf(os.path.join(folder, name), homonuclear=first == second)
            assert np.all(np.isfinite(skf.h_tab))
            assert np.all(np.isfinite(skf.s_tab))
            count += 1
    assert count > 250  # 225 in 3ob-3-1 plus 57 in mio-1-1


if __name__ == "__main__":
    for system in ("gs_h2o", "gs_znh2o", "znscl_gen"):
        test_h0_and_overlap_match_dftbplus(system)
    test_matrices_are_symmetric_with_unit_overlap_diagonal()
    test_spectrum_is_invariant_under_rigid_rotation()
    print("H0 and S match DFTB+ on every tested system")
