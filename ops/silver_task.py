"""Guard a Silver subprocess; preserve the core transformation's code identity."""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


def guarded(command, log, scratch, min_free=10 * 2**30, max_seconds=3600, poll_seconds=5):
    if shutil.disk_usage(scratch).free < min_free:
        raise RuntimeError("Insufficient scratch space before task start")
    started = time.monotonic()
    child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def terminate(signum, frame):
        raise InterruptedError(f"Task interrupted by signal {signum}")

    previous = {s: signal.signal(s, terminate) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        while child.poll() is None:
            if shutil.disk_usage(scratch).free < min_free:
                raise RuntimeError("10-GiB scratch-space guard triggered")
            if time.monotonic() - started > max_seconds:
                raise TimeoutError("Silver subprocess runtime guard triggered")
            time.sleep(poll_seconds)
        if child.returncode:
            raise RuntimeError(f"Silver subprocess exited {child.returncode}; see {log.name}")
        return time.monotonic() - started
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset")
    parser.add_argument("day", type=int)
    parser.add_argument("--root", required=True)
    parser.add_argument("--master", default="local[6]")
    parser.add_argument("--output-partitions", default="4")
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--event-log-dir")
    args = parser.parse_args()
    from pipelines.silver import Store, partition

    store = Store(args.root)
    part = partition(args.dataset, args.day)
    directory = Path(os.environ.get("TRACE_RUN_LOG_DIR", "logs/silver"))
    directory.mkdir(parents=True, exist_ok=True)
    phase = "audit" if args.audit_only else "transform"
    stamp = f"{time.time_ns()}"
    log_path = directory / f"{phase}-{args.dataset}-{args.day}-{stamp}.log"
    record_path = directory / f"{phase}-{args.dataset}-{args.day}-{stamp}.json"
    before = store.read(f"manifests/silver/{args.dataset}/{part}.current.json")
    record = {
        "dataset": args.dataset,
        "day": None if args.dataset == "execution" else args.day,
        "phase": phase,
        "started_epoch": time.time(),
        "status": "running",
        "log": log_path.name,
    }
    record_path.write_text(json.dumps(record, indent=2))
    command = [
        sys.executable,
        "-m",
        "pipelines.silver",
        args.dataset,
        str(args.day),
        "--root",
        args.root,
        "--master",
        args.master,
        "--output-partitions",
        args.output_partitions,
    ]
    if args.audit_only:
        command.append("--audit-only")
    if args.event_log_dir:
        command += ["--event-log-dir", args.event_log_dir]
    try:
        with log_path.open("w") as log:
            record["elapsed_seconds"] = guarded(
                command, log, directory, max_seconds=900 if args.audit_only else 3600
            )
        current = store.read(f"manifests/silver/{args.dataset}/{part}.current.json")
        if not current:
            raise RuntimeError("No committed Silver after successful subprocess")
        record.update(
            status="success",
            accepted=current["accepted"],
            reused=bool(before and before["accepted"] == current["accepted"]),
            source_fingerprint=current["source_fingerprint"],
            code_sha256=current["code_sha256"],
            quality=current["quality"],
        )
        if args.audit_only:
            audit = store.read(f"manifests/silver_audits/{args.dataset}/{part}.json")
            if not audit or audit.get("silver_attempt") != current["accepted"]:
                raise RuntimeError("Final audit does not bind current Silver attempt")
            record["final_audit"] = audit
    except BaseException as error:
        record.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        record["finished_epoch"] = time.time()
        record_path.write_text(json.dumps(record, indent=2))
        if store.remote:
            store.write(
                os.environ.get(
                    "TRACE_SILVER_STATUS_KEY", "run-logs/full-silver-20261119/status.json"
                ).rsplit("/", 1)[0]
                + "/tasks/"
                + record_path.name,
                record,
            )
    print(
        json.dumps(
            {k: record[k] for k in ["dataset", "day", "phase", "status", "elapsed_seconds"]},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
