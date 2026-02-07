# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

from __future__ import annotations

import shutil
import subprocess
import time
import re
from pathlib import Path

import pytest

OUTPUT_ROOT = Path("tests/test_agents") / "codex_runs"
PROJECTS_ROOT = Path("projects")

MODEL = "gpt-5.2-codex"


def load_prompts(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    prompts: list[str] = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        # Optional: allow comments in the prompt file
        if ln.startswith("#"):
            continue
        prompts.append(ln)
    return prompts


def list_project_dirs() -> set[Path]:
    if not PROJECTS_ROOT.exists():
        return set()
    return {p.resolve() for p in PROJECTS_ROOT.iterdir() if p.is_dir()}


DONE_RE = re.compile(r"status\s*:\s*done\b", re.IGNORECASE)


def wait_for_done(
    src: Path, dest: Path, timeout_s: int = 900, poll_s: float = 2.0
) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for p in (src / "summary.md", dest / "summary.md"):
            if p.exists():
                txt = p.read_text(encoding="utf-8", errors="replace")
                if DONE_RE.search(txt):
                    return True
        time.sleep(poll_s)
    return False


@pytest.mark.agent
def test_codex_prompt(idx: int, prompt: str, output_root: Path) -> None:
    run_dir = output_root / f"run_{idx}"
    run_dir.mkdir(parents=True, exist_ok=True)

    session_out = run_dir / "session_output.jsonl"
    stderr_out = run_dir / "stderr.log"

    before = list_project_dirs()

    cmd = [
        "codex",
        "exec",
        "--json",
        "--model",
        MODEL,
        prompt,
    ]

    # Run Codex; stream stdout/stderr directly to files
    try:
        with (
            session_out.open("w", encoding="utf-8") as so,
            stderr_out.open("w", encoding="utf-8") as se,
        ):
            proc = subprocess.run(cmd, stdout=so, stderr=se, text=True, timeout=60 * 60)
    except subprocess.TimeoutExpired:
        pytest.fail(
            f"codex exec timed out for run_{idx}. See {stderr_out} (if created)."
        )

    if proc.returncode != 0:
        err = (
            stderr_out.read_text(encoding="utf-8", errors="ignore")
            if stderr_out.exists()
            else ""
        )
        pytest.fail(f"codex exec failed (rc={proc.returncode}) for run_{idx}\n\n{err}")

    after = list_project_dirs()
    new_dirs = sorted(after - before, key=lambda p: p.stat().st_mtime, reverse=True)

    assert new_dirs, (
        f"No new project directories detected under {PROJECTS_ROOT}. "
        f"Check {session_out} / {stderr_out} for what happened."
    )

    moved_dirs: list[Path] = []
    for src in new_dirs:
        dest = run_dir / src.name
        shutil.move(str(src), str(dest))
        moved_dirs.append(dest)

    # “Finished” condition: summary.md exists and says status: done
    for job_dir in moved_dirs:
        summary_md = job_dir / "summary.md"
        print(f"Checking job summary at {summary_md}")
        ok = wait_for_done(job_dir, run_dir, timeout_s=900)
        assert ok, f"Job did not reach status: done. summary: {summary_md}"
