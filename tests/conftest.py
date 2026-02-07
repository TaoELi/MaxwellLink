# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

from __future__ import annotations

from pathlib import Path
import pytest

TARGET_TEST = "test_codex_prompt"
DEFAULT_DIR = Path("tests/test_agents")


def _prompt_set(config: pytest.Config) -> str:
    return str(config.getoption("--codex-prompts"))


def _prompts_file(config: pytest.Config) -> Path:
    override = config.getoption("--codex-prompts-file")
    if override:
        return Path(override)
    name = _prompt_set(config)  # "local" or "hpc"
    return DEFAULT_DIR / f"prompts_{name}.txt"


def _cache_key(config: pytest.Config) -> str:
    pf = _prompts_file(config)
    return f"codex_prompts/next_idx::{pf.resolve()}"


def load_prompts(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    prompts: list[str] = []
    for ln in lines:
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        prompts.append(ln)
    return prompts


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("codex-prompts")
    group.addoption(
        "--codex-prompts",
        action="store",
        default="local",
        choices=["local", "hpc"],
        help="Select which prompts file to use: prompts_local.txt or prompts_hpc.txt",
    )
    group.addoption(
        "--codex-prompts-file",
        action="store",
        default=None,
        help="Override prompt file path (takes precedence over --codex-prompts).",
    )
    group.addoption(
        "--codex-resume",
        action="store_true",
        default=False,
        help="Resume codex prompt tests from the next index stored in pytest cache.",
    )
    group.addoption(
        "--codex-reset",
        action="store_true",
        default=False,
        help="Reset the cached next index for the selected prompt set back to 1.",
    )


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("--codex-reset"):
        config.cache.set(_cache_key(config), 1)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    # Dynamically parametrize only the codex prompt test
    if metafunc.function.__name__ != TARGET_TEST:
        return
    if not {"idx", "prompt"} <= set(metafunc.fixturenames):
        return

    pf = _prompts_file(metafunc.config)
    prompts = load_prompts(pf)

    params = list(enumerate(prompts, start=1))
    ids = [f"{i:03d}" for i, _ in params]

    metafunc.parametrize(("idx", "prompt"), params, ids=ids)


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if not config.getoption("--codex-resume"):
        return

    next_idx = int(config.cache.get(_cache_key(config), 1))

    for item in items:
        if TARGET_TEST not in item.nodeid:
            continue

        callspec = getattr(item, "callspec", None)
        if not callspec:
            continue

        idx = callspec.params.get("idx")
        if isinstance(idx, int) and idx < next_idx:
            item.add_marker(
                pytest.mark.skip(
                    reason=f"Already passed earlier (resume at idx={next_idx})."
                )
            )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    outcome = yield
    report = outcome.get_result()

    if report.when != "call":
        return
    if TARGET_TEST not in item.nodeid:
        return

    callspec = getattr(item, "callspec", None)
    if not callspec:
        return

    idx = callspec.params.get("idx")
    if not isinstance(idx, int):
        return

    if report.passed:
        key = _cache_key(item.config)
        current = int(item.config.cache.get(key, 1))
        item.config.cache.set(key, max(current, idx + 1))


@pytest.fixture(scope="session")
def output_root(request: pytest.FixtureRequest) -> Path:
    """
    Output root depends on the selected prompts file, e.g.
    tests/test_agents/prompts_local.txt/
    tests/test_agents/my_custom_prompts.txt/
    """
    pf = _prompts_file(request.config)
    dir_name = pf.name.split(".")[0]
    return DEFAULT_DIR / dir_name
