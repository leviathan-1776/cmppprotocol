"""分析 compare-burst.ps1 产生的配对结果；不启动或构建 Rust 程序。"""
import argparse
import csv
import json
import random
import statistics
from collections import defaultdict
from pathlib import Path


def healthy(row):
    if any(int(row.get(k) or -1) != 0 for k in ("exit_code", "retries", "loss_signals", "timeouts", "closed", "exhausted")):
        return False
    return (row.get("segments") == row.get("responses") and
            row.get("deliver_sent") == row.get("deliver_acked") == row.get("delivers"))


def interval(ratios):
    rng = random.Random(20260927)
    samples = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(20000))
    return samples[499], samples[19499]


def analyze(paths):
    groups = defaultdict(dict)
    for path in paths:
        with Path(path).open(encoding="utf-8-sig", newline="") as source:
            for row in csv.DictReader(source):
                key = (int(row["pair"]), row["variant"])
                if key in groups[row["scenario"]]:
                    raise ValueError(f"duplicate pair: {row['scenario']} {key}")
                groups[row["scenario"]][key] = row
    result = {}
    for scenario, rows in groups.items():
        for variant in ("baseline", "candidate"):
            hashes = {r["binary_sha256"] for (_, v), r in rows.items() if v == variant}
            if len(hashes) > 1:
                raise ValueError(f"mixed binary versions: {scenario} {variant}")
        if all(v == "candidate" for _, v in rows):
            good = sum(healthy(r) for r in rows.values())
            result[scenario] = {"kind": "stability", "runs": len(rows), "healthy_runs": good,
                                "decision": "pass" if good == len(rows) and good >= 10 else "not_passed"}
            continue
        paired = []
        invalid = []
        for pair in sorted({key[0] for key in rows}):
            base, candidate = rows.get((pair, "baseline")), rows.get((pair, "candidate"))
            if not base or not candidate or not healthy(base) or not healthy(candidate):
                invalid.append(pair)
            else:
                if base["arguments"] != candidate["arguments"]:
                    raise ValueError(f"mismatched parameters: {scenario} {pair}")
                paired.append((base, candidate))
        summary = {"healthy_pairs": len(paired), "invalid_or_unpaired": invalid, "metrics": {}}
        for metric in ("responses", "delivers"):
            ratios = [int(c[metric]) / int(b[metric]) for b, c in paired if int(b[metric]) > 0]
            if not ratios:
                continue
            middle = statistics.median(ratios)
            lower, upper = interval(ratios)
            decision = "pass"
            if invalid:
                decision = "invalid_run"
            elif len(ratios) < 7:
                decision = "screen_only"
            elif lower < 1 or middle < 1:
                decision = "extend_to_15" if lower <= 1 <= upper and len(ratios) < 15 else "reject"
            summary["metrics"][metric] = {"median_ratio": middle, "ci95": [lower, upper], "pairs": len(ratios), "decision": decision}
        result[scenario] = summary
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="+")
    args = parser.parse_args()
    print(json.dumps(analyze(args.csv), ensure_ascii=False, indent=2))
