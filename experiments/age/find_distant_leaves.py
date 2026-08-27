#!/usr/bin/env python3
"""Find the deterministic Top-K longest leaf-to-leaf paths in a Dewey forest."""

from __future__ import annotations

import csv
import heapq
import sys
from dataclasses import dataclass, field
from pathlib import Path


TOP_K = 20


@dataclass(frozen=True)
class Endpoint:
    distance: int
    node_id: int
    dewey: str


@dataclass
class Frame:
    components: tuple[str, ...]
    node_id: int | None
    dewey: str | None
    has_child: bool = False
    endpoints: list[Endpoint] = field(default_factory=list)


def endpoint_order(endpoint: Endpoint) -> tuple[int, int, str]:
    return (-endpoint.distance, endpoint.node_id, endpoint.dewey)


def retain_best(endpoints: list[Endpoint]) -> list[Endpoint]:
    return sorted(endpoints, key=endpoint_order)[:TOP_K]


def pair_quality(
    distance: int, id1: int, id2: int
) -> tuple[int, int, int]:
    # Larger tuples are better. For equal distances, smaller IDs win.
    return distance, -id1, -id2


def add_pair(
    best_pairs: list[tuple[tuple[int, int, int], tuple]],
    left: Endpoint,
    right: Endpoint,
    lca_dewey: str | None,
) -> None:
    if left.node_id < right.node_id:
        first, second = left, right
    else:
        first, second = right, left

    distance = first.distance + second.distance
    quality = pair_quality(distance, first.node_id, second.node_id)
    record = (
        first.node_id,
        first.dewey,
        second.node_id,
        second.dewey,
        lca_dewey,
        first.distance,
        second.distance,
        distance,
    )
    item = (quality, record)

    if len(best_pairs) < TOP_K:
        heapq.heappush(best_pairs, item)
    elif quality > best_pairs[0][0]:
        heapq.heapreplace(best_pairs, item)


def close_frame(
    stack: list[Frame],
    best_pairs: list[tuple[tuple[int, int, int], tuple]],
) -> None:
    child = stack.pop()
    if not child.has_child:
        if child.node_id is None or child.dewey is None:
            return
        child.endpoints = [Endpoint(0, child.node_id, child.dewey)]

    parent = stack[-1]
    shifted = [
        Endpoint(endpoint.distance + 1, endpoint.node_id, endpoint.dewey)
        for endpoint in child.endpoints
    ]

    # The parent's existing endpoints belong to previously closed child
    # branches, so every combination has this parent as its exact LCA.
    for left in parent.endpoints:
        for right in shifted:
            add_pair(best_pairs, left, right, parent.dewey)

    parent.endpoints = retain_best(parent.endpoints + shifted)
    parent.has_child = True


def find_pairs(rows) -> list[tuple]:
    # All real roots are children of this imaginary parent.
    stack = [Frame((), None, None)]
    best_pairs: list[tuple[tuple[int, int, int], tuple]] = []

    for line_number, raw_line in enumerate(rows, start=1):
        line = raw_line.rstrip("\n")
        if not line:
            continue
        try:
            raw_id, dewey = line.split("\t", 1)
            node_id = int(raw_id)
        except ValueError as exc:
            raise ValueError(f"invalid input on line {line_number}: {line!r}") from exc

        components = tuple(dewey.split("."))
        while len(stack) > 1 and components[: len(stack[-1].components)] != stack[-1].components:
            close_frame(stack, best_pairs)

        expected_parent = components[:-1]
        if stack[-1].components != expected_parent:
            raise ValueError(
                f"missing Dewey parent for {dewey!r}: expected {'.'.join(expected_parent)!r}"
            )
        stack.append(Frame(components, node_id, dewey))

    while len(stack) > 1:
        close_frame(stack, best_pairs)

    return [
        record
        for _, record in sorted(
            best_pairs,
            key=lambda item: (-item[1][7], item[1][0], item[1][2]),
        )
    ]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"Usage: {Path(sys.argv[0]).name} OUTPUT.csv")

    pairs = find_pairs(sys.stdin)
    with Path(sys.argv[1]).open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "id1",
                "dewey1",
                "id2",
                "dewey2",
                "lca_dewey",
                "distance1",
                "distance2",
                "total_distance",
            ]
        )
        writer.writerows(pairs)


if __name__ == "__main__":
    main()
