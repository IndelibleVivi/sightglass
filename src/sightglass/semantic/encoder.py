"""Bounded local encoder contract; never downloads assets or calls a remote provider."""

from __future__ import annotations

import json
import math
import os
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from sightglass.contracts.errors import ErrorCode, SightglassError


@dataclass(frozen=True)
class EncodedBatch:
    model: str
    revision: int
    dimensions: int
    vectors: tuple[tuple[float, ...], ...]


class LocalEncoder(Protocol):
    def encode(self, texts: tuple[str, ...]) -> EncodedBatch: ...


class AppleSentenceEncoder:
    def __init__(self, helper: Path, *, language: str = "zh-Hans") -> None:
        self.helper = helper
        self.language = language

    def encode(self, texts: tuple[str, ...]) -> EncodedBatch:
        if (
            not 0 < len(texts) <= 128
            or any(len(value) > 16_000 for value in texts)
            or self.language not in {"en", "zh-Hans"}
        ):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        try:
            info = self.helper.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
                or info.st_nlink != 1
            ):
                raise ValueError("encoder helper must be owner-private")
            process = subprocess.run(
                [str(self.helper)],
                input=json.dumps({"language": self.language, "texts": texts}).encode(),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=60,
                check=True,
            )
            value = json.loads(process.stdout)
            vectors = tuple(
                tuple(float(element) for element in vector) for vector in value["vectors"]
            )
            dimensions = int(value["dimensions"])
            if (
                len(vectors) != len(texts)
                or not 0 < dimensions <= 4096
                or any(
                    len(vector) != dimensions
                    or not all(math.isfinite(element) for element in vector)
                    for vector in vectors
                )
            ):
                raise ValueError("invalid encoder result")
            return EncodedBatch(str(value["model"]), int(value["revision"]), dimensions, vectors)
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
            raise SightglassError(
                ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "local_encoder_unavailable"}
            ) from exc


def cosine(first: tuple[float, ...], second: tuple[float, ...]) -> float:
    denominator = math.sqrt(sum(x * x for x in first) * sum(x * x for x in second))
    return (
        sum(x * y for x, y in zip(first, second, strict=True)) / denominator if denominator else 0.0
    )
