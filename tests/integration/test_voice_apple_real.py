"""Real Apple SpeechAnalyzer recognition through the compiled Swift helper.

Skipped by default: it needs a compiled helper (``scripts/compile-voice-helper.sh``) and
installed macOS speech assets.  Point ``SIGHTGLASS_VOICE_REAL_HELPER`` at the binary, or
compile it to the default private location.  No speech assets and no microphone are
touched by the daemon itself; this test only exercises the helper the daemon would run.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
import wave
from pathlib import Path

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.voice.apple import DEFAULT_HELPER_TIMEOUT_SECONDS, AppleSpeechHelper

PRIVATE_HELPER_PATH = (
    Path.home()
    / "Library"
    / "Application Support"
    / "Sightglass"
    / "voice"
    / "sightglass-transcribe"
)
HELPER_PATH = Path(os.environ.get("SIGHTGLASS_VOICE_REAL_HELPER") or PRIVATE_HELPER_PATH)
SPOKEN_TEXT = "Hello, this is a local transcription check."
PCM_SAMPLE_RATE = 16_000

requires_helper = unittest.skipUnless(
    HELPER_PATH.is_file() and os.access(HELPER_PATH, os.X_OK),
    "no compiled voice helper; run scripts/compile-voice-helper.sh or set "
    "SIGHTGLASS_VOICE_REAL_HELPER",
)
requires_speech_tools = unittest.skipUnless(
    shutil.which("say") is not None, "macOS say is unavailable"
)


def synthesize_pcm(root: Path, text: str = SPOKEN_TEXT) -> Path:
    """One 16 kHz mono s16le PCM file spoken locally by ``say``."""

    wave_path = root / "speech.wav"
    subprocess.run(
        ["say", "-o", str(wave_path), f"--data-format=LEI16@{PCM_SAMPLE_RATE}", text],
        check=True,
        capture_output=True,
    )
    pcm_path = root / "speech.pcm"
    with wave.open(str(wave_path), "rb") as handle:
        assert handle.getnchannels() == 1
        assert handle.getsampwidth() == 2
        assert handle.getframerate() == PCM_SAMPLE_RATE
        pcm_path.write_bytes(handle.readframes(handle.getnframes()))
    return pcm_path


@requires_helper
class RealHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def helper(self, locale: str) -> AppleSpeechHelper:
        return AppleSpeechHelper(
            HELPER_PATH, locale=locale, timeout_seconds=DEFAULT_HELPER_TIMEOUT_SECONDS
        )

    def test_helper_reports_its_version(self) -> None:
        result = subprocess.run(
            [str(HELPER_PATH), "--version"], check=True, capture_output=True
        )
        self.assertEqual(result.stdout.decode().strip(), "sightglass-transcribe 2")

    @requires_speech_tools
    def test_installed_locale_transcribes_local_speech(self) -> None:
        transcript = self.helper("en-US").transcribe_pcm(synthesize_pcm(self.root))
        self.assertEqual(transcript.locale, "en-US")
        self.assertEqual(transcript.backend, "SpeechAnalyzer+SpeechTranscriber")
        self.assertEqual(transcript.asset_status, "installed")
        self.assertEqual(
            transcript.text.strip().lower().rstrip("."), SPOKEN_TEXT.lower().rstrip(".")
        )
        self.assertGreaterEqual(transcript.segments, 1)
        self.assertTrue(transcript.provenance()["volatile_excluded"])
        self.assertTrue(transcript.os_version)

    @requires_speech_tools
    def test_unsupported_locale_is_blocked(self) -> None:
        with self.assertRaises(SightglassError) as caught:
            self.helper("zz-ZZ").transcribe_pcm(synthesize_pcm(self.root))
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertIn(
            caught.exception.details["reason"], {"unsupported_locale", "not_installed"}
        )
        self.assertEqual(caught.exception.details["stage"], "recognize")

    def test_audio_file_path_also_works(self) -> None:
        wave_path = self.root / "speech.wav"
        subprocess.run(
            ["say", "-o", str(wave_path), f"--data-format=LEI16@{PCM_SAMPLE_RATE}", SPOKEN_TEXT],
            check=True,
            capture_output=True,
        )
        result = subprocess.run(
            [str(HELPER_PATH), "--audio", str(wave_path), "--locale", "en-US"],
            check=True,
            capture_output=True,
        )
        self.assertIn("transcription", result.stdout.decode().lower())


if __name__ == "__main__":
    unittest.main()
