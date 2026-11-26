#!/usr/bin/env python3
"""Plan campaigns and check native driver logs; neither command runs a seed."""
import argparse
import json
import os
from pathlib import Path
import re
import sys


MAX_JOBS = 256  # GitHub Actions' matrix limit.


def plan(test, first, seeds, long_seeds):
    if not re.fullmatch(r"Test[A-Za-z0-9_]+", test):
        raise ValueError(f"test {test!r} is not the name of a test")
    if first < 0 or seeds <= 0 or first + seeds > 1 << 53:
        raise ValueError("first and seeds must describe a nonempty range of exact JSON seed integers")
    end = first + seeds
    longs = sorted(set(long_seeds))
    if any(seed < first or seed >= end for seed in longs):
        raise ValueError("long seed outside the requested range")
    all_long = test in ("TestCampaignShapeSeeds", "TestKubernetesLoadLong")
    if len(longs) > MAX_JOBS or (all_long and seeds > MAX_JOBS):
        raise ValueError("too many long seeds for a 256-job matrix; use a smaller campaign")
    if all_long and len(longs) != seeds:
        raise ValueError(f"every {test} seed must be long")

    normal = seeds - len(longs)
    processes = []
    if normal:
        # Preserve the old seeds-per-process rule (at most 40 processes).
        slots = 2 * min(20, (normal + 1) // 2)
        per = (normal + slots - 1) // slots
        process, remaining = [], per
        start = first
        for stop in longs + [end]:
            while start < stop:
                n = min(remaining, stop - start)
                process.append(dict(first=start, seeds=n))
                remaining -= n
                start += n
                if not remaining:
                    processes.append(process)
                    process, remaining = [], per
            start = stop + 1
        if process:
            processes.append(process)

    jobs = [dict(processes=processes[i:i + 4]) for i in range(0, len(processes), 4)]
    jobs += [dict(processes=[[dict(first=seed, seeds=1)]]) for seed in longs]
    if len(jobs) > MAX_JOBS:
        raise ValueError("campaign exceeds the 256-job matrix limit; use a smaller campaign")
    for shard, job in enumerate(jobs):
        job["shard"] = shard
        for process in job["processes"]:
            # Divide the queue's replay budget over its spans, not each
            # span's rounded-down 1%. A 121-seed queue split 83+38 still
            # replays one seed. Cumulative ceilings put the first replay
            # in the first span, as the unsplit driver's selection does.
            total = sum(span["seeds"] for span in process)
            twice, before = total // 100, 0
            for span in process:
                after = before + span["seeds"]
                span["twice"] = ((after * twice + total - 1) // total
                                 - (before * twice + total - 1) // total)
                before = after
    return dict(test=test, first=first, seeds=seeds, jobs=jobs)


def reported(log, test):
    """Only unbuffered native seed-driver verdicts count as reported.

    Go buffers subtest PASS/FAIL until the parent ends, so a killed
    process can lose those lines for completed seeds. Numeric subtests
    that do not use simrun are not seeds. Image/fork verdicts and embedded
    gosim output also cannot stand in for the whole seed's verdict.
    """
    result = re.compile(rf"simrun: seed {re.escape(test)}/([0-9]+) (PASS|FAIL|SKIP known-failure)")
    return {int(match[1]) for line in log if (match := result.fullmatch(line.rstrip("\r\n")))}


def summary(campaign, artifacts, out):
    # The saved plan is also the ownership map. Require it to partition
    # exactly the requested range, with no gaps, duplicates or extras.
    spans = sorted((span["first"], span["seeds"])
                   for job in campaign["jobs"] for process in job["processes"] for span in process)
    cursor = campaign["first"]
    for first, seeds in spans:
        if first != cursor or seeds <= 0:
            raise ValueError("plan does not partition the requested seed range")
        cursor += seeds
    if cursor != campaign["first"] + campaign["seeds"]:
        raise ValueError("plan does not cover the requested seed range")
    missing, total = 0, 0
    for job in campaign["jobs"]:
        shard = job["shard"]
        for p, ranges in enumerate(job["processes"]):
            path = artifacts / f"sim-campaign-logs-{shard}" / f"seeds-{p}.log"
            done = set()
            if path.exists():
                with path.open(errors="replace") as log:
                    done = reported(log, campaign["test"])
            for span in ranges:
                first, end = span["first"], span["first"] + span["seeds"]
                total += span["seeds"]
                cursor = first
                for seed in sorted(seed for seed in done if first <= seed < end) + [end]:
                    if cursor < seed:
                        label = str(cursor) if cursor == seed - 1 else f"{cursor}–{seed - 1}"
                        print(f"Missing seeds {label}: seeds ({shard}), process {p}", file=out)
                        missing += seed - cursor
                    cursor = seed + 1
    if total != campaign["seeds"]:
        raise ValueError("plan does not cover the requested seed count")
    print(f"{total - missing} of {total} seeds reported; {missing} missing", file=out)
    return 1 if missing else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser("plan")
    planner.add_argument("--long-seeds", type=Path, required=True)
    planner.add_argument("--output", type=Path, required=True)
    checker = commands.add_parser("summary")
    checker.add_argument("--plan", type=Path, help="saved plan; otherwise use the workflow's planning outputs")
    checker.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "plan":
            longs = json.loads(args.long_seeds.read_text())
            campaign = plan(os.environ["TEST"], int(os.environ["FIRST"]), int(os.environ["SEEDS"]), longs)
            args.output.write_text(json.dumps(campaign) + "\n")
            with open(os.environ["GITHUB_OUTPUT"], "a") as output:
                print("matrix=" + json.dumps(dict(include=campaign["jobs"]), separators=(",", ":")), file=output)
            print(f"{len(campaign['jobs'])} jobs; {len(longs)} long seeds run alone")
            return 0
        if args.plan:
            campaign = json.loads(args.plan.read_text())
        else:
            # Job outputs survive an artifact-download failure. The
            # summary needs only logs, not the potentially huge sim-out.
            campaign = dict(test=os.environ["TEST"], first=int(os.environ["FIRST"]),
                            seeds=int(os.environ["SEEDS"]), jobs=json.loads(os.environ["MATRIX"])["include"])
        return summary(campaign, args.artifacts, sys.stdout)
    except (ValueError, OSError, KeyError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    sys.exit(main())
