#!/usr/bin/env python3
"""Compare compatible MuscleDB benchmark JSON files (Python standard library only)."""
import argparse
import json
import statistics
import sys


def compare(baseline, candidate):
    for key in ("schema_version", "suite_version", "zig_version", "optimize", "arch", "os", "cpu", "cache_mode", "allocator"):
        if baseline[key] != candidate[key]:
            raise ValueError(f"incompatible {key}: {baseline[key]!r} vs {candidate[key]!r}")
    for key in ("rows", "samples", "warmup"):
        if baseline["config"][key] != candidate["config"][key]:
            raise ValueError(f"incompatible configuration: {key}")
    old = {entry["name"]: entry for entry in baseline["benchmarks"]}
    new = {entry["name"]: entry for entry in candidate["benchmarks"]}
    if not old or old.keys() != new.keys():
        raise ValueError("benchmark names differ or suite is empty")
    lines = [f"{'benchmark':30} {'baseline us':>12} {'candidate us':>13} {'change':>9} {'spread % (old/new)':>20}"]
    for name, before in old.items():
        after = new[name]
        for key in ("iterations", "queries", "checksum"):
            if before[key] != after[key]:
                raise ValueError(f"incompatible {name}: {key}")
        medians = []
        spreads = []
        for entry in (before, after):
            samples = entry["samples_ns_per_query"]
            if len(samples) != baseline["config"]["samples"] or any(not isinstance(x, (int, float)) or not 0 < x < float('inf') for x in samples):
                raise ValueError(f"invalid samples for {name}")
            median = statistics.median(samples)
            medians.append(median)
            # Relative median absolute deviation describes sample variation.
            spreads.append(100 * statistics.median(abs(x - median) for x in samples) / median)
        change = 100 * (medians[1] / medians[0] - 1)
        lines.append(f"{name:30} {medians[0] / 1000:12.2f} {medians[1] / 1000:13.2f} {change:+8.1f}% {spreads[0]:8.1f}/{spreads[1]:.1f}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline")
    parser.add_argument("candidate")
    args = parser.parse_args()
    try:
        with open(args.baseline) as file:
            baseline = json.load(file)
        with open(args.candidate) as file:
            candidate = json.load(file)
        print(compare(baseline, candidate))
        print("\nNegative change is faster. Spread is relative median absolute deviation; small changes need repeated runs.")
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Cannot compare: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
