"""The build must not destroy the evidence trail it is built to produce.

Main._setup_logging writes the engine's timestamped log beside the executable —
into dist/ — and build_helper deletes dist/ wholesale on every build. So a
rebuild wiped every engine log since the last one, which is precisely when they
matter: twice a post-run review lost the engine-side log to a rebuild started
minutes afterwards (2026-08-19, and again 2026-09-01).
"""
from __future__ import annotations

import time

import build_helper as B


def _dist(root, *names):
    (root / "dist").mkdir(parents=True, exist_ok=True)
    for n in names:
        (root / "dist" / n).write_text("log body", encoding="utf-8")


def test_run_logs_survive_a_rebuild(tmp_path):
    _dist(tmp_path, "run_2026-09-01_10-20-59.log", "run_2026-08-31_10-20-11.log",
          "run.log")
    (tmp_path / "dist" / "Portfolio Optimiser.exe").write_text("bin", encoding="utf-8")

    assert B._preserve_run_logs(tmp_path) == 3
    archived = {p.name for p in (tmp_path / B.LOG_ARCHIVE_DIRNAME).glob("run*.log")}
    assert "run_2026-09-01_10-20-59.log" in archived
    # Only the logs move — the build output itself is left for the build to wipe.
    assert (tmp_path / "dist" / "Portfolio Optimiser.exe").exists()


def test_a_second_build_does_not_clobber_the_first_archive(tmp_path):
    """run.log is overwritten every run, so consecutive builds collide on the
    name. Keeping the newest under a suffix beats silently dropping either."""
    _dist(tmp_path, "run.log")
    B._preserve_run_logs(tmp_path)
    time.sleep(1.1)                       # mtime resolution for the suffix
    _dist(tmp_path, "run.log")
    B._preserve_run_logs(tmp_path)

    archived = sorted(p.name for p in (tmp_path / B.LOG_ARCHIVE_DIRNAME).glob("run*.log"))
    assert len(archived) == 2, archived


def test_the_archive_is_bounded(tmp_path, monkeypatch):
    """An archive of an archive would otherwise grow without limit."""
    monkeypatch.setattr(B, "LOG_ARCHIVE_KEEP", 3)
    for i in range(6):
        _dist(tmp_path, f"run_2026-09-{i + 1:02d}_10-00-00.log")
        B._preserve_run_logs(tmp_path)
    kept = list((tmp_path / B.LOG_ARCHIVE_DIRNAME).glob("run*.log"))
    assert len(kept) == 3, [p.name for p in kept]


def test_a_missing_dist_is_not_an_error(tmp_path):
    """First build on a clean checkout has no dist/ at all."""
    assert B._preserve_run_logs(tmp_path) == 0
