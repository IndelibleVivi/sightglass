"""Bounded child processes for the local voice pipeline.

Every voice child process runs in its own session so the parent can terminate the whole
group, and both output streams are drained with hard caps instead of being buffered
without limit.
"""

from __future__ import annotations

import os
import select
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

READ_CHUNK_BYTES = 65_536
TERMINATE_GRACE_SECONDS = 2.0
KILL_REAP_SECONDS = 5.0


class BoundedProcessError(RuntimeError):
    """The bounded child could not be started at all."""


@dataclass(frozen=True)
class ProcessOutcome:
    argv: tuple[str, ...]
    exit_code: int | None
    signal_number: int | None
    stdout: bytes
    stderr: bytes
    timed_out: bool
    limit_exceeded: str | None

    @property
    def ok(self) -> bool:
        return (
            self.exit_code == 0
            and self.signal_number is None
            and not self.timed_out
            and self.limit_exceeded is None
        )


def _terminate_group(process: subprocess.Popen[bytes]) -> None:
    try:
        group = os.getpgid(process.pid)
    except OSError:
        group = None
    for signum in (signal.SIGTERM, signal.SIGKILL):
        if process.poll() is not None:
            return
        try:
            if group is not None:
                os.killpg(group, signum)
            else:
                process.send_signal(signum)
        except (OSError, ProcessLookupError):
            return
        if signum is signal.SIGTERM:
            try:
                process.wait(timeout=TERMINATE_GRACE_SECONDS)
                return
            except subprocess.TimeoutExpired:
                continue


def run_bounded(
    argv: Sequence[str],
    *,
    timeout_seconds: float,
    max_stdout_bytes: int,
    max_stderr_bytes: int,
    stdin_data: bytes | None = None,
    pass_fds: Sequence[int] = (),
    env: Mapping[str, str] | None = None,
) -> ProcessOutcome:
    """Run one child in a new session with bounded output and a wall-clock deadline."""

    if timeout_seconds <= 0:
        raise BoundedProcessError("timeout must be positive")
    try:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            pass_fds=tuple(pass_fds),
            env=dict(env) if env is not None else None,
        )
    except OSError as exc:
        raise BoundedProcessError(str(exc)) from exc
    assert process.stdout is not None and process.stderr is not None
    if stdin_data is not None and process.stdin is not None:
        try:
            process.stdin.write(stdin_data)
        except (BrokenPipeError, OSError):
            pass
        finally:
            process.stdin.close()
    deadline = time.monotonic() + float(timeout_seconds)
    stdout_fd = process.stdout.fileno()
    stderr_fd = process.stderr.fileno()
    buffers: dict[int, list[bytes]] = {stdout_fd: [], stderr_fd: []}
    pending = {stdout_fd, stderr_fd}
    sizes = {stdout_fd: 0, stderr_fd: 0}
    caps = {stdout_fd: int(max_stdout_bytes), stderr_fd: int(max_stderr_bytes)}
    names = {stdout_fd: "stdout", stderr_fd: "stderr"}
    timed_out = False
    limit_exceeded: str | None = None
    try:
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                _terminate_group(process)
                break
            ready, _, _ = select.select(sorted(pending), [], [], min(remaining, 0.25))
            for descriptor in ready:
                try:
                    chunk = os.read(descriptor, READ_CHUNK_BYTES)
                except OSError:
                    chunk = b""
                if not chunk:
                    pending.discard(descriptor)
                    continue
                sizes[descriptor] += len(chunk)
                if sizes[descriptor] > caps[descriptor]:
                    limit_exceeded = names[descriptor]
                    _terminate_group(process)
                    break
                buffers[descriptor].append(chunk)
            if limit_exceeded is not None:
                break
        if process.poll() is None:
            try:
                process.wait(timeout=KILL_REAP_SECONDS)
            except subprocess.TimeoutExpired:
                _terminate_group(process)
                process.wait(timeout=KILL_REAP_SECONDS)
    finally:
        process.stdout.close()
        process.stderr.close()
    exit_code = process.returncode
    signal_number = -exit_code if exit_code is not None and exit_code < 0 else None
    return ProcessOutcome(
        argv=tuple(str(value) for value in argv),
        exit_code=exit_code,
        signal_number=signal_number,
        stdout=b"".join(buffers[stdout_fd]),
        stderr=b"".join(buffers[stderr_fd]),
        timed_out=timed_out,
        limit_exceeded=limit_exceeded,
    )
