"""Fail closed to product checks unless every changed input is prose."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import PurePosixPath


def docs_only(event: str, paths: list[str] | None) -> bool:
    if event not in {"push", "pull_request"} or not paths:
        return False
    prose = {"README.md", "README.zh-CN.md", "AGENTS.md", "NOTICE.md"}
    return all(
        path in prose or (path.startswith("docs/") and PurePosixPath(path).suffix == ".md")
        for path in paths
    )


def changed_paths(base: str, head: str) -> list[str] | None:
    if not base or set(base) == {"0"}:
        return None
    result = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", "-z", base, head],
        capture_output=True,
        check=False,
    )
    if result.returncode:
        return None
    return [
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0") if path
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", required=True)
    args = parser.parse_args()
    selected = docs_only(args.event, changed_paths(args.base, args.head))
    print(f"docs_only={str(selected).lower()}")


if __name__ == "__main__":
    main()
