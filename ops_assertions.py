"""Assert that the live pipeline's REALITY matches its declared INTENT.

Two failure modes this exists for, both observed:

1. CONFIG DRIFT. On 2026-08-03 a fix shipped whose own docstring says
   "START TIME IS LOAD-BEARING — 10:20 AEST, not 09:30". The Windows scheduled
   task was never moved. The code said one thing, the OS did another, and for a
   week nothing compared them, so a committed fix sat inert.

2. SILENT ABSENCE. On 2026-08-07 the evening evidence run died and told nobody.
   Had the daily run never fired at all, that would have looked identical:
   no email, no error, no output. "Nothing happened" and "everything is fine"
   were the same observation.

So: expectations live in ops_expected.json, every job stamps a ledger line, and
this module compares the two and SAYS SO. Run it from the wrappers:

    ops_assertions.py --record <job> --outcome ok|fail [--detail TEXT]
    ops_assertions.py --check [--email]

Deliberately dependency-free (stdlib only) and NOT part of the frozen exe, so
assertions can be edited without a rebuild.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
EXPECTED_FILENAME = "ops_expected.json"
LEDGER_FILENAME = "run_ledger.jsonl"

_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
         "Saturday", "Sunday"]


# --- pure decision logic (unit-tested; no OS, no clock, no files) --------

def check_task_schedule(actual: dict, expected: dict) -> list:
    """Compare observed scheduled tasks against what the repo says they are.

    actual:   {task_name: {"start_time": "HH:MM", "enabled": bool,
                           "days": [names] | None}}
    expected: {task_name: {"start_time": "HH:MM", "must_be_enabled": bool,
                           "days": [names]}}
    Returns a list of human-readable findings; empty means everything agrees.
    """
    out = []
    for name, want in sorted(expected.items()):
        got = actual.get(name)
        if got is None:
            out.append(f"TASK MISSING: '{name}' is not registered at all.")
            continue
        want_t = str(want.get("start_time", "")).strip()
        got_t = str(got.get("start_time", "")).strip()
        if want_t and got_t != want_t:
            out.append(f"TASK TIME: '{name}' starts at {got_t or '?'}, "
                       f"expected {want_t}.")
        if want.get("must_be_enabled", True) and not got.get("enabled", False):
            out.append(f"TASK DISABLED: '{name}' is not enabled.")
        want_d = want.get("days")
        got_d = got.get("days")
        if want_d and got_d is not None and sorted(want_d) != sorted(got_d):
            out.append(f"TASK DAYS: '{name}' runs {sorted(got_d)}, "
                       f"expected {sorted(want_d)}.")
        # The command line matters as much as the schedule. On 2026-08-10 the US
        # Session task was registered with an UNQUOTED -File path over a home
        # directory containing a space; it fired correctly at 02:00 for two days,
        # returned 0xFFFD0000 before the script could log anything, and passed
        # every time/day check above. Declare the quoted path and the assertion
        # names the cause instead of leaving the heartbeat to imply it.
        want_a = want.get("action_contains")
        got_a = str(got.get("action", "") or "")
        if want_a and want_a not in got_a:
            out.append(f"TASK ACTION: '{name}' command line does not contain "
                       f"{want_a!r} — got {got_a!r}.")
    return out


def previous_expected_run(now: datetime, start_time: str, days: list,
                          grace_minutes: int = 90):
    """The most recent moment this job SHOULD already have finished by.

    Walks back day by day to the latest scheduled occurrence that is at least
    `grace_minutes` in the past. Returns None when the job has no scheduled day
    within the last week. Weekend-aware by construction, which matters: a
    Friday-evening job checked on Monday is 72h old and perfectly healthy, so a
    flat max-age would cry wolf every Monday and be muted within a fortnight.
    """
    try:
        hh, mm = (int(x) for x in str(start_time).split(":"))
    except (TypeError, ValueError):
        return None
    want = {str(d).lower() for d in (days or [])}
    for back in range(0, 8):
        day = now - timedelta(days=back)
        if want and _DAYS[day.weekday()].lower() not in want:
            continue
        occ = day.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if (now - occ).total_seconds() >= grace_minutes * 60:
            return occ
    return None


HEARTBEAT_LOOKBACK_DAYS = 7


def expected_occurrences(now: datetime, start_time: str, days: list,
                         grace_minutes: int = 90,
                         lookback_days: int = HEARTBEAT_LOOKBACK_DAYS) -> list:
    """Every scheduled occurrence in the lookback window that is already due.

    previous_expected_run answers "the latest one" and is kept for the
    schedule checks; this answers "all of them", which is what catching a gap
    requires.
    """
    try:
        hh, mm = (int(x) for x in str(start_time).split(":"))
    except (TypeError, ValueError):
        return []
    want = {str(d).lower() for d in (days or [])}
    out = []
    for back in range(0, max(1, int(lookback_days)) + 1):
        day = now - timedelta(days=back)
        if want and _DAYS[day.weekday()].lower() not in want:
            continue
        occ = day.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if (now - occ).total_seconds() >= grace_minutes * 60:
            out.append(occ)
    return sorted(out)


def check_heartbeat(ledger_rows: list, jobs: dict, now: datetime) -> list:
    """Did each job actually run EVERY time it was supposed to?

    Checks every due occurrence in the last week, not just the most recent
    one. The single-occurrence version could not see a job's own missed day:
    daily_auto.ps1 stamps the ledger BEFORE running this check (deliberately,
    so today's run is not reported as absent), so by check time the latest
    success is always today's and always satisfies yesterday's due time.
    2026-08-13 was missed and went unreported for exactly that reason — the
    same check caught evidence_run's miss the same morning, only because that
    job runs at 18:00 and had not yet stamped.

    Anything that ran and FAILED is still reported: the ledger records it, but
    a failure is not a success.
    """
    out = []
    ok_dates: dict = {}
    any_dates: dict = {}
    first_seen: dict = {}
    latest: dict = {}
    for r in ledger_rows:
        job = str(r.get("job", ""))
        ts = _parse_ts(r.get("finished") or r.get("started"))
        if not job or ts is None:
            continue
        outcome = str(r.get("outcome", "")).lower()
        any_dates.setdefault(job, set()).add(ts.date())
        if outcome == "ok":
            ok_dates.setdefault(job, set()).add(ts.date())
        if job not in first_seen or ts < first_seen[job]:
            first_seen[job] = ts
        prev = latest.get(job)
        if prev is None or ts > prev[0]:
            latest[job] = (ts, outcome)

    for job, cfg in sorted(jobs.items()):
        grace = int(cfg.get("grace_minutes", 90))
        due = previous_expected_run(
            now, cfg.get("start_time", ""), cfg.get("days") or [], grace)
        if due is None:
            continue
        if job not in latest:
            out.append(f"NO RUN RECORDED: '{job}' has never stamped the ledger; "
                       f"expected one by {due:%Y-%m-%d %H:%M}.")
            continue
        # A job cannot have missed a day before it existed. Without this the
        # week the ledger itself was introduced reads as seven missed runs, and
        # so does any newly added job — noise that would train the reader to
        # ignore the whole block.
        begin = first_seen[job].date()
        occs = [o for o in expected_occurrences(
            now, cfg.get("start_time", ""), cfg.get("days") or [], grace)
            if o.date() >= begin]
        seen_ok, seen_any = ok_dates.get(job, set()), any_dates.get(job, set())
        # A day that HAS a ledger row but no successful one is a failure, not
        # an absence. Different cause, different fix, different message.
        gaps = [o for o in occs if o.date() not in seen_ok and o.date() not in seen_any]
        fails = [o for o in occs if o.date() not in seen_ok and o.date() in seen_any]
        if gaps:
            shown = ", ".join(f"{g:%Y-%m-%d}" for g in gaps[-3:])
            more = f" (+{len(gaps) - 3} earlier)" if len(gaps) > 3 else ""
            out.append(f"MISSED RUN: '{job}' had no run at all on {shown}{more} "
                       f"(due {gaps[-1]:%H:%M}).")
        for f in fails[-2:]:
            _, outcome = latest[job]
            out.append(f"RUN FAILED: '{job}' ran on {f:%Y-%m-%d} but did not "
                       f"succeed (latest outcome '{outcome}').")
    return out


def check_required_files(root: Path, names: list) -> list:
    out = []
    for n in names or []:
        p = Path(root) / n
        if not p.exists():
            out.append(f"MISSING STATE FILE: {n}")
        elif p.stat().st_size == 0:
            out.append(f"EMPTY STATE FILE: {n}")
    return out


def _digest(path) -> str | None:
    """SHA-256 of a file's bytes, or None if unreadable."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fp:
            for chunk in iter(lambda: fp.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def check_exe_freshness(exe_path: Path, code_paths: list) -> list:
    """Flag engine sources whose CODE differs from what went into the binary.

    The [build] line already reports the exe's sha, but nothing compared it to
    what is on disk — working out whether the exe was stale took a manual diff
    on 2026-08-10.

    Compares CONTENT against dist/build_manifest.json, which build_helper
    writes with a SHA-256 per compiled source. The original mtime comparison
    asked "was this file written after the build", which is a different
    question: git rewrites mtimes on checkout without changing a byte, so
    merging a branch on 2026-08-24 reported 11 sources stale while the exe's
    code was identical. A false alarm on the one check that says "do not trust
    this binary" is worse than no check — it teaches you to skip it.

    Falls back to mtime when no manifest exists, so a binary built before this
    existed still gets the weaker check rather than none.
    """
    exe = Path(exe_path)
    if not exe.exists():
        return [f"EXE MISSING: {exe_path}"]

    manifest = exe.parent / "build_manifest.json"
    recorded = None
    try:
        if manifest.exists():
            recorded = (json.loads(manifest.read_text(encoding="utf-8"))
                        or {}).get("sources") or None
    except Exception:
        recorded = None

    if recorded:
        changed = []
        for p in code_paths:
            p = Path(p)
            if not p.exists():
                continue
            was = recorded.get(p.name) or recorded.get(str(p.name))
            if was is None:
                # In engine_sources but absent from the manifest: the two lists
                # have diverged, which is itself worth saying out loud.
                changed.append(f"{p.name} (not in manifest)")
            elif _digest(p) != was:
                changed.append(p.name)
        if changed:
            return [f"EXE STALE: {len(changed)} engine source(s) differ from the "
                    f"binary ({', '.join(sorted(changed)[:6])}"
                    f"{' ...' if len(changed) > 6 else ''}) — rebuild before "
                    f"relying on a production run."]
        return []

    exe_mtime = exe.stat().st_mtime
    newer = [Path(p).name for p in code_paths
             if Path(p).exists() and Path(p).stat().st_mtime > exe_mtime]
    if newer:
        return [f"EXE STALE: {len(newer)} engine source(s) newer than the "
                f"binary ({', '.join(sorted(newer)[:6])}"
                f"{' ...' if len(newer) > 6 else ''}) — rebuild before relying "
                f"on a production run (no build manifest; mtime comparison)."]
    return []


def _parse_ts(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v)).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


# --- I/O -----------------------------------------------------------------

def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def read_ledger(path) -> list:
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except OSError:
        pass
    return rows


def record(job: str, outcome: str, detail: str = "") -> int:
    """Append one ledger line. Never fatal — a wrapper must not die because its
    bookkeeping failed."""
    entry = {"job": str(job),
             "finished": datetime.now().isoformat(timespec="seconds"),
             "outcome": str(outcome or "").lower(),
             "detail": str(detail or "")[:500]}
    try:
        with open(_SCRIPT_DIR / LEDGER_FILENAME, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
        print(f"[ops] ledger: {job} -> {entry['outcome']}")
        return 0
    except OSError as e:
        print(f"[ops][WARN] could not write ledger ({e}).")
        return 0


def observe_scheduled_tasks(names: list) -> dict:
    """Read the real scheduled tasks via PowerShell. Returns {} if unavailable,
    which callers must treat as 'unknown', never as 'fine'."""
    if not names:
        return {}
    ps = (
        "$out=@{}; foreach($n in @(" +
        ",".join("'" + str(n).replace("'", "''") + "'" for n in names) +
        ")) { $t = Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue; "
        "if ($t) { $tr = $t.Triggers[0]; "
        "$sb = if ($tr) { [string]$tr.StartBoundary } else { '' }; "
        "$hm = if ($sb -match 'T(\\d{2}):(\\d{2})') { $Matches[1]+':'+$Matches[2] } else { '' }; "
        "$dow = $null; if ($tr -and $tr.DaysOfWeek) { $m=@(); "
        "$names2=@('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'); "
        "for($i=0;$i -lt 7;$i++){ if ((([int]$tr.DaysOfWeek) -band [math]::Pow(2,$i)) -ne 0){ $m+=$names2[$i] } } $dow=$m }; "
        "$ac = $t.Actions[0]; "
        "$cmd = if ($ac) { (''+$ac.Execute+' '+$ac.Arguments).Trim() } else { '' }; "
        "$out[$n]=@{ start_time=$hm; enabled=[bool]$t.Settings.Enabled; days=$dow; action=$cmd } } }; "
        "$out | ConvertTo-Json -Depth 5 -Compress"
    )
    try:
        raw = subprocess.check_output(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            stderr=subprocess.DEVNULL, timeout=60,
        ).decode("utf-8", errors="replace").strip()
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError,
            ValueError):
        return {}


def run_check(email: bool = False) -> int:
    cfg = load_json(_SCRIPT_DIR / EXPECTED_FILENAME, {})
    if not cfg:
        print(f"[ops] no {EXPECTED_FILENAME} — nothing declared, nothing to "
              f"check.")
        return 0

    findings = []
    tasks_cfg = cfg.get("scheduled_tasks", {}) or {}
    if tasks_cfg:
        actual = observe_scheduled_tasks(list(tasks_cfg))
        if not actual:
            findings.append("TASKS UNREADABLE: could not query the Windows "
                            "scheduler — schedule NOT verified this run.")
        else:
            findings += check_task_schedule(actual, tasks_cfg)

    findings += check_heartbeat(
        read_ledger(_SCRIPT_DIR / LEDGER_FILENAME),
        cfg.get("jobs", {}) or {}, datetime.now())
    findings += check_required_files(_SCRIPT_DIR,
                                     cfg.get("required_files", []) or [])
    exe_cfg = cfg.get("exe", {}) or {}
    if exe_cfg.get("path"):
        findings += check_exe_freshness(
            _SCRIPT_DIR / exe_cfg["path"],
            [_SCRIPT_DIR / p for p in exe_cfg.get("engine_sources", [])])

    print("=" * 78)
    print("OPS ASSERTIONS")
    print("=" * 78)
    if not findings:
        print("  All declared expectations hold.")
        return 0
    for f in findings:
        print(f"  [!] {f}")
    print("=" * 78)

    if email:
        try:
            from send_alert import send
            rc = send(f"[Portfolio Optimiser] OPS DRIFT: {len(findings)} "
                      f"assertion(s) failing",
                      "The live pipeline does not match its declared "
                      "configuration:\n\n" + "\n".join(f"  - {f}" for f in findings))
            print(f"[ops] --email: sent (rc={rc}).")
        except Exception as e:
            print(f"[ops][WARN] --email failed ({e}).")
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="Compare reality against ops_expected.json.")
    ap.add_argument("--email", action="store_true",
                    help="With --check: email when assertions fail.")
    ap.add_argument("--record", type=str, metavar="JOB",
                    help="Append a run-ledger entry for JOB.")
    ap.add_argument("--outcome", type=str, default="ok",
                    help="With --record: ok | fail (default ok).")
    ap.add_argument("--detail", type=str, default="",
                    help="With --record: short free-text context.")
    args = ap.parse_args()

    if args.record:
        return record(args.record, args.outcome, args.detail)
    if args.check:
        return run_check(email=bool(args.email))
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
