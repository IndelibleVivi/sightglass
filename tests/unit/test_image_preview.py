from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
import wave
from io import BytesIO
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from sightglass.resources.processors import (
    ImageInfo,
    _ffmpeg_command,
    decode_text_with_encoding,
    image_preview,
    inspect_image,
    inspect_media,
    render_video_preview,
    sniff_mime,
)
from sightglass.resources.service import ResourceService
from sightglass.source.synthetic import _png_bytes


class ImagePreviewTests(unittest.TestCase):
    def preview_command(self, width: int, height: int) -> list[str]:
        source = ImageInfo(width, height, "png", False)
        rendered = ImageInfo(min(width, 2048), min(height, 2048), "png", False)
        png = b"\x89PNG\r\n\x1a\nsynthetic"
        seen: list[str] = []

        def fake_run(
            command: list[str],
            output: Path,
            *,
            max_output_bytes: int,
            stdout_to_output: bool,
        ) -> bytes:
            del output
            self.assertEqual(max_output_bytes, 4096)
            self.assertFalse(stdout_to_output)
            seen.extend(command)
            return png

        with (
            patch(
                "sightglass.resources.processors.inspect_image",
                side_effect=(source, rendered),
            ),
            patch("sightglass.resources.processors._command", return_value="/synthetic/sips"),
            patch("sightglass.resources.processors._run_bounded_file", new=fake_run),
        ):
            preview, info = image_preview(png, max_bytes=4096)

        self.assertEqual(preview, png)
        self.assertEqual(info, rendered)
        return seen

    def test_small_images_are_normalized_without_upscaling(self) -> None:
        command = self.preview_command(240, 240)

        self.assertNotIn("-Z", command)

    def test_large_images_are_bounded_to_the_preview_dimension(self) -> None:
        command = self.preview_command(4096, 3072)

        self.assertEqual(command[command.index("-Z") + 1], "2048")

    def test_sticker_descriptor_advertises_derivable_preview(self) -> None:
        class Repository:
            @staticmethod
            def resource_binding(_resource_id: str, _variant: str):
                return None

        service = object.__new__(ResourceService)
        cast(Any, service).repository = Repository()

        descriptor = service._descriptor(  # noqa: SLF001 - focused contract regression
            {
                "resource_id": "synthetic-sticker",
                "message_id": "synthetic-message",
                "conversation_id": "synthetic-conversation",
                "kind": "sticker",
                "mime_type": "image/x-wechat-wxgf",
                "original_name": None,
                "declared_size": 1024,
                "declared_hash": None,
                "availability": "local_available",
            }
        )

        self.assertTrue(descriptor["preview_available"])
        self.assertTrue(descriptor["original_available"])


class ExtendedFormatProcessorTests(unittest.TestCase):
    def test_utf16_and_gb18030_text_are_decoded_with_their_encoding(self) -> None:
        utf16 = "Synthetic UTF-16 文本\n第二行".encode("utf-16")
        gb18030 = "Synthetic GB18030 文本\n第二行".encode("gb18030")

        self.assertEqual(sniff_mime(utf16), "text/plain")
        self.assertEqual(sniff_mime(gb18030), "text/plain")
        self.assertEqual(
            decode_text_with_encoding(utf16),
            ("Synthetic UTF-16 文本\n第二行", "utf-16"),
        )
        self.assertEqual(
            decode_text_with_encoding(gb18030),
            ("Synthetic GB18030 文本\n第二行", "gb18030"),
        )

    @unittest.skipUnless(shutil.which("sips"), "macOS sips is required")
    def test_heic_tiff_and_bmp_are_sniffed_inspected_and_previewed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sightglass-formats-") as raw:
            root = Path(raw)
            source = root / "source.png"
            source.write_bytes(_png_bytes())
            expected = {
                "heic": "image/heic",
                "tiff": "image/tiff",
                "bmp": "image/bmp",
            }
            for image_format, mime_type in expected.items():
                with self.subTest(image_format=image_format):
                    output = root / f"converted.{image_format}"
                    subprocess.run(
                        [
                            "sips",
                            "-s",
                            "format",
                            image_format,
                            source.as_posix(),
                            "--out",
                            output.as_posix(),
                        ],
                        check=True,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    data = output.read_bytes()

                    self.assertEqual(sniff_mime(data), mime_type)
                    info = inspect_image(data)
                    self.assertEqual((info.width, info.height), (3, 2))
                    preview, rendered = image_preview(data, max_bytes=1024 * 1024)
                    self.assertEqual(sniff_mime(preview), "image/png")
                    self.assertEqual((rendered.width, rendered.height), (3, 2))

    def test_synthesized_video_reports_metadata_and_renders_png_preview(self) -> None:
        try:
            ffmpeg = _ffmpeg_command()
        except Exception as exc:  # pragma: no cover - optional dependency environment
            self.skipTest(f"bundled ffmpeg unavailable: {exc}")
        with tempfile.TemporaryDirectory(prefix="sightglass-video-") as raw:
            output = Path(raw) / "synthetic.mp4"
            subprocess.run(
                [
                    ffmpeg,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "color=c=0x1b2430:s=96x64:d=0.25",
                    "-an",
                    "-pix_fmt",
                    "yuv420p",
                    "-movflags",
                    "+faststart",
                    "-y",
                    output.as_posix(),
                ],
                check=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            data = output.read_bytes()

        self.assertEqual(sniff_mime(data), "video/mp4")
        info = inspect_media(data, "video/mp4")
        self.assertTrue(info.has_video)
        self.assertFalse(info.has_audio)
        self.assertEqual((info.width, info.height), (96, 64))
        self.assertIsNotNone(info.duration_seconds)
        preview, preview_info = render_video_preview(data, max_bytes=1024 * 1024)
        self.assertEqual(sniff_mime(preview), "image/png")
        self.assertTrue(preview_info.has_video)

    def test_synthesized_wav_reports_audio_metadata(self) -> None:
        output = BytesIO()
        with wave.open(output, "wb") as writer:
            writer.setnchannels(1)
            writer.setsampwidth(2)
            writer.setframerate(8_000)
            writer.writeframes(b"\x00\x00" * 800)
        data = output.getvalue()

        self.assertEqual(sniff_mime(data), "audio/wav")
        info = inspect_media(data, "audio/wav")
        self.assertTrue(info.has_audio)
        self.assertFalse(info.has_video)
        self.assertIsNone(info.width)
        self.assertIsNone(info.height)
        self.assertAlmostEqual(info.duration_seconds or 0.0, 0.1, places=2)


if __name__ == "__main__":
    unittest.main()
