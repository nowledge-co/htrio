#!/usr/bin/env python3
"""Summarize JSONL upload receipts without discarding failed measurements."""

import argparse
import collections
import json
import math
import statistics
import sys

FIELDS = ("revision", "network", "algorithm", "bytes", "os", "arch", "gso_enabled")


def summarize(paths):
    groups = collections.defaultdict(list)
    for path in paths:
        with open(path, encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                record = json.loads(line)
                if record.get("schema") != 1 or record.get("kind") != "upload":
                    raise ValueError(
                        f"{path}:{line_number}: unsupported receipt schema"
                    )
                record.setdefault("gso_enabled", True)
                key = tuple(record[field] for field in FIELDS)
                groups[key].append(record)
    for key, records in sorted(groups.items()):
        successful = [record for record in records if record["status"] == "ok"]
        result = dict(zip(FIELDS, key))
        result.update(
            attempts=len(records),
            successes=len(successful),
            failures=len(records) - len(successful),
        )
        if successful:
            rates = sorted(record["upload_mbps"] for record in successful)
            result.update(
                median_upload_mbps=statistics.median(rates),
                p10_upload_mbps=rates[max(0, math.ceil(len(rates) * 0.1) - 1)],
                median_p95_smoothed_rtt_ms=statistics.median(
                    record["p95_smoothed_rtt_ms"] for record in successful
                ),
                sent_packets=sum(record["sent_packets"] for record in successful),
                lost_packets=sum(record["lost_packets"] for record in successful),
                congestion_events=sum(
                    record["congestion_events"] for record in successful
                ),
            )
            result["reported_loss_ratio"] = result["lost_packets"] / max(
                1, result["sent_packets"]
            )
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipts", nargs="+")
    args = parser.parse_args()
    try:
        summarize(args.receipts)
    except (OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
