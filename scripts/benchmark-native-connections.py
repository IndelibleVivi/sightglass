#!/usr/bin/env python
"""Sample repeated handle retirement with generated encrypted macOS fixtures only."""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=10)
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("this generated native fixture requires macOS and the macos-wechat extra")
    if not 1 <= args.rounds <= 100:
        parser.error("rounds must be between 1 and 100 (40 replacements each)")
    args.scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    tempfile.tempdir = str(args.scratch.resolve())
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from tests.integration.test_native_connection_lifecycle import NativeConnectionLifecycleTests

    case = NativeConnectionLifecycleTests(
        methodName="test_repeated_identity_replacement_keeps_handle_and_fd_set_bounded"
    )
    samples = []
    case.setUp()
    try:
        for ordinal in range(1, args.rounds + 1):
            case.test_repeated_identity_replacement_keeps_handle_and_fd_set_bounded()
            rss = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())])
            samples.append({
                "replacements": ordinal * 40,
                "rss_bytes": int(rss.strip()) * 1024,
                "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "open_fds": len(os.listdir("/dev/fd")),
                "cache": case.provider.connection_cache_status(),
            })
    finally:
        case.tearDown()
    print(json.dumps({
        "schema": "sightglass.synthetic-native-connections.v1",
        "scope": "generated encrypted native fixture only; no configured account",
        "samples": samples,
    }, indent=2))


if __name__ == "__main__":
    main()
