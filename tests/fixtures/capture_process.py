"""Generated-source-only stdio edge process used by relay integration tests."""

from __future__ import annotations

import sys
from pathlib import Path

from sightglass.contracts.capture import CaptureCeiling
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_relay import EdgeSession, FramedStream, RelayDisconnected
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.direct_wechat import SyntheticSourceProvider


def main() -> None:
    source, spool_path, token, initialize = sys.argv[1:]
    provider = SyntheticSourceProvider(Path(source))
    executor = CaptureExecutor(
        provider,
        CaptureCeiling("synthetic-account-demo", frozenset({"conv_group"}), "fixture-egress-1"),
        source_instance_id="fixture-capture-instance",
    )
    spool = (
        EdgeSpool.initialize(
            Path(spool_path),
            stream_epoch="fixture-stream",
            source_instance_id=executor.source_instance_id,
            account_id=executor.ceiling.account_id,
            origin_epoch=executor.origin_epoch,
        )
        if initialize == "initialize"
        else EdgeSpool(
            Path(spool_path),
            source_instance_id=executor.source_instance_id,
            account_id=executor.ceiling.account_id,
            origin_epoch=executor.origin_epoch,
        )
    )
    try:
        EdgeSession(executor, spool, token=token).run(
            FramedStream(sys.stdin.buffer, sys.stdout.buffer)
        )
    except RelayDisconnected:
        pass
    finally:
        spool.close()


if __name__ == "__main__":
    main()
