import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from maxwelllink.mxl_drivers.python.mxl_driver_ucx import _clean_env_for_ucx_subprocess


def test_clean_env_for_ucx_subprocess_strips_inherited_transport_vars(monkeypatch):
    monkeypatch.setenv("UCX_NET_DEVICES", "mlx5_0:1")
    monkeypatch.setenv("UCX_PROTO_ENABLE", "n")
    monkeypatch.setenv("FI_PROVIDER", "verbs")
    monkeypatch.setenv("OMPI_MCA_pml", "ucx")
    monkeypatch.setenv("PMI_RANK", "0")
    monkeypatch.setenv("KEEP_ME", "1")

    env = _clean_env_for_ucx_subprocess()

    assert "UCX_NET_DEVICES" not in env
    assert "UCX_PROTO_ENABLE" not in env
    assert "FI_PROVIDER" not in env
    assert "OMPI_MCA_pml" not in env
    assert "PMI_RANK" not in env
    assert env["KEEP_ME"] == "1"


def test_clean_env_for_ucx_subprocess_preserves_transport_tuning_when_requested(
    monkeypatch,
):
    monkeypatch.setenv("MXL_UCX_KEEP_TRANSPORT_ENV", "1")
    monkeypatch.setenv("UCX_NET_DEVICES", "ib0")
    monkeypatch.setenv("FI_PROVIDER", "tcp")
    monkeypatch.setenv("OMPI_MCA_pml", "ucx")
    monkeypatch.setenv("PMI_RANK", "0")

    env = _clean_env_for_ucx_subprocess()

    assert env["UCX_NET_DEVICES"] == "ib0"
    assert env["FI_PROVIDER"] == "tcp"
    assert "OMPI_MCA_pml" not in env
    assert "PMI_RANK" not in env
