from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.processors import (
    WXGF_MIME,
    decode_wechat_wxgf_preview,
    extract_wechat_wxgf_hevc,
    sniff_mime,
)


class WeChatWXGFTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hevc = b"\x00\x00\x00\x01synthetic-annex-b-stream"
        self.container = b"wxgf synthetic bounded header" + self.hevc

    def test_extracts_annex_b_stream_from_container(self) -> None:
        self.assertEqual(extract_wechat_wxgf_hevc(self.container), self.hevc)
        self.assertEqual(sniff_mime(self.container), WXGF_MIME)

    def test_missing_bounded_annex_b_start_code_fails_closed(self) -> None:
        with self.assertRaises(SightglassError) as caught:
            extract_wechat_wxgf_hevc(b"wxgf" + b"x" * 5000 + self.hevc)

        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

    def test_decoder_passes_only_extracted_hevc_to_bounded_processor(self) -> None:
        png = b"\x89PNG\r\n\x1a\nsynthetic decoded frame"

        def fake_run(
            command: list[str],
            output: Path,
            *,
            max_output_bytes: int,
            stdout_to_output: bool,
        ) -> bytes:
            self.assertEqual(command[0], "/synthetic/ffmpeg")
            self.assertIn("hevc", command)
            source = Path(command[command.index("-i") + 1])
            self.assertEqual(source.read_bytes(), self.hevc)
            self.assertEqual(max_output_bytes, 4096)
            self.assertFalse(stdout_to_output)
            output.write_bytes(png)
            return png

        with (
            patch(
                "sightglass.resources.processors._ffmpeg_command",
                return_value="/synthetic/ffmpeg",
            ),
            patch("sightglass.resources.processors._run_bounded_file", new=fake_run),
        ):
            decoded = decode_wechat_wxgf_preview(self.container, max_bytes=4096)

        self.assertEqual(decoded, png)


if __name__ == "__main__":
    unittest.main()
