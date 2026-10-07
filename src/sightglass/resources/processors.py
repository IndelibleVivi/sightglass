from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.storage import temporary_workspace

from .rich import sniff_rich_mime
from .sticker import WXGF_MAGIC

MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_SOURCE_ENVELOPE_BYTES = MAX_SOURCE_BYTES + 64 * 1024
MAX_IMAGE_PIXELS = 40_000_000
MAX_IMAGE_DIMENSION = 16_384
MAX_PDF_PAGES = 500
MAX_EXTRACTED_TEXT_BYTES = 4 * 1024 * 1024
PROCESS_TIMEOUT_SECONDS = 20.0
WXGF_MIME = "image/x-wechat-wxgf"
_MAX_WXGF_HEADER_BYTES = 4096

# Linux image processing is backed by libvips (``vipsheader`` for metadata,
# ``vipsthumbnail`` for previews).  Both are probed for their actual capabilities
# before use: libvips loads HEIF/TIFF/BMP/JPEG/PNG/WebP/GIF through installed loaders,
# so presence is necessary but not sufficient.  We never install one, fetch one, or add
# a Python image runtime that is not an existing dependency.
_VIPS_HEADER_TIMEOUT_SECONDS = 10.0
_PREVIEW_SCALE_BOUND = 2048


@dataclass(frozen=True)
class ImageInfo:
    width: int
    height: int
    format: str
    animated: bool


@dataclass(frozen=True)
class PdfInfo:
    page_count: int
    page_size: str | None
    version: str | None


@dataclass(frozen=True)
class MediaInfo:
    format: str
    duration_seconds: float | None
    width: int | None
    height: int | None
    has_audio: bool
    has_video: bool


_HEIF_BRANDS = {b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis", b"mif1"}
_MP4_VIDEO_BRANDS = {b"isom", b"iso2", b"mp41", b"mp42", b"avc1", b"dash", b"M4V "}
_MP4_AUDIO_BRANDS = {b"M4A ", b"M4B ", b"M4P "}


def sniff_mime(data: bytes) -> str:
    if data.startswith((b"#!SILK_V3", b"\x02#!SILK_V3")):
        return "audio/silk"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if data.startswith(b"BM"):
        return "image/bmp"
    if len(data) >= 12 and data[4:8] == b"ftyp":
        compatible = [
            data[index : index + 4]
            for index in range(16, min(len(data), 64), 4)
        ]
        brands = {data[8:12], *compatible}
        if brands & _HEIF_BRANDS:
            return "image/heic"
        if b"qt  " in brands:
            return "video/quicktime"
        if brands & _MP4_AUDIO_BRANDS:
            return "audio/mp4"
        if brands & _MP4_VIDEO_BRANDS:
            return "video/mp4"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        return "audio/wav"
    if data.startswith(b"fLaC"):
        return "audio/flac"
    if data.startswith(b"OggS"):
        return "audio/ogg"
    # UTF-16LE's ``FF FE`` BOM is also a syntactically valid MPEG-1 Layer I
    # frame prefix. A BOM-bearing payload is an explicit text declaration, so
    # validate it before applying the heuristic frame detector.
    if data.startswith((b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff")):
        try:
            decode_text(data)
        except SightglassError:
            pass
        else:
            return "text/plain"
    mp3_frame = bool(
        len(data) >= 4
        and data[0] == 0xFF
        and data[1] & 0xE0 == 0xE0
        and (data[1] >> 3) & 0x03 != 0x01
        and (data[1] >> 1) & 0x03 != 0
        and (data[2] >> 4) & 0x0F not in {0, 0x0F}
        and (data[2] >> 2) & 0x03 != 0x03
    )
    if data.startswith(b"ID3") or mp3_frame:
        return "audio/mpeg"
    if data.startswith(WXGF_MAGIC):
        return WXGF_MIME
    if data.startswith(b"%PDF-"):
        return "application/pdf"
    rich_mime = sniff_rich_mime(data)
    if rich_mime is not None:
        return rich_mime
    try:
        decode_text(data)
    except SightglassError:
        return "application/octet-stream"
    return "text/plain"


def decode_text(data: bytes) -> str:
    text, _encoding = decode_text_with_encoding(data)
    return text


def decode_text_with_encoding(data: bytes) -> tuple[str, str]:
    """Decode the accepted local text encodings in a deterministic strict order."""

    candidates: list[tuple[str, bytes]]
    if data.startswith(b"\xef\xbb\xbf"):
        candidates = [("utf-8-sig", data)]
    elif data.startswith((b"\xff\xfe", b"\xfe\xff")):
        candidates = [("utf-16", data)]
    else:
        candidates = [("utf-8", data), ("gb18030", data)]
    text: str | None = None
    encoding = ""
    for candidate, value in candidates:
        try:
            text = value.decode(candidate)
            encoding = candidate
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    controls = sum(ord(value) < 32 and value not in "\n\r\t\f" for value in text)
    if text and controls / len(text) > 0.01:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return text.replace("\r\n", "\n").replace("\r", "\n"), encoding


def _command(name: str) -> str:
    found = shutil.which(name)
    if found is None:
        raise SightglassError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            details={"reason": f"{name}_unavailable"},
        )
    return found


def processor_status() -> dict[str, bool]:
    try:
        _ffmpeg_command()
        ffmpeg = True
    except SightglassError:
        ffmpeg = False
    return {
        "sips": shutil.which("sips") is not None,
        "vipsheader": shutil.which("vipsheader") is not None,
        "vipsthumbnail": shutil.which("vipsthumbnail") is not None,
        "pdfinfo": shutil.which("pdfinfo") is not None,
        "pdftotext": shutil.which("pdftotext") is not None,
        "pdftoppm": shutil.which("pdftoppm") is not None,
        "ffmpeg": ffmpeg,
    }


def _private_file(directory: Path, name: str, data: bytes) -> Path:
    path = directory / name
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return path


def _run_bounded_file(
    command: list[str],
    output: Path,
    *,
    max_output_bytes: int,
    stdout_to_output: bool,
    timeout_seconds: float = PROCESS_TIMEOUT_SECONDS,
) -> bytes:
    started = time.monotonic()
    output_handle = output.open("wb") if stdout_to_output else subprocess.DEVNULL
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=output_handle,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        while process.poll() is None:
            if output.exists() and output.stat().st_size > max_output_bytes:
                process.kill()
                process.wait()
                raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
            if time.monotonic() - started > timeout_seconds:
                process.kill()
                process.wait()
                raise SightglassError(
                    ErrorCode.RESOURCE_DECODE_FAILED,
                    details={"reason": "processor_timeout"},
                )
            time.sleep(0.02)
    finally:
        if stdout_to_output:
            assert not isinstance(output_handle, int)
            output_handle.close()
    if process.returncode != 0 or not output.exists():
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    metadata = output.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size > max_output_bytes
        or metadata.st_nlink != 1
    ):
        raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
    return output.read_bytes()


def _ffmpeg_command() -> str:
    try:
        import imageio_ffmpeg

        executable = Path(imageio_ffmpeg.get_ffmpeg_exe()).resolve()
        metadata = executable.stat()
    except (ImportError, OSError, RuntimeError) as exc:
        raise SightglassError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            details={"reason": "ffmpeg_unavailable"},
        ) from exc
    if not stat.S_ISREG(metadata.st_mode) or not os.access(executable, os.X_OK):
        raise SightglassError(
            ErrorCode.RESOURCE_UNAVAILABLE,
            details={"reason": "ffmpeg_unavailable"},
        )
    return executable.as_posix()


def _run_capture(command: list[str], *, max_output_bytes: int = 256 * 1024) -> bytes:
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            close_fds=True,
            timeout=PROCESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise SightglassError(
            ErrorCode.RESOURCE_DECODE_FAILED,
            details={"reason": "processor_timeout"},
        ) from exc
    output = completed.stderr or b""
    if completed.returncode != 0 or len(output) > max_output_bytes:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return output


def inspect_media(data: bytes, mime_type: str) -> MediaInfo:
    if not (mime_type.startswith("audio/") or mime_type.startswith("video/")):
        raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input.media", data)
        raw = _run_capture(
            [
                _ffmpeg_command(),
                "-hide_banner",
                "-nostdin",
                "-i",
                source.as_posix(),
                "-f",
                "null",
                "-",
            ]
        )
    text = raw.decode("utf-8", "replace")
    duration_match = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", text)
    duration = None
    if duration_match is not None:
        duration = (
            int(duration_match.group(1)) * 3600
            + int(duration_match.group(2)) * 60
            + float(duration_match.group(3))
        )
    video_match = re.search(
        r"Stream[^\n]*Video:[^\n]*?\b(\d{2,5})x(\d{2,5})\b", text
    )
    width = int(video_match.group(1)) if video_match is not None else None
    height = int(video_match.group(2)) if video_match is not None else None
    if width is not None and height is not None:
        _require_safe_dimensions(width, height)
    has_video = video_match is not None
    has_audio = re.search(r"Stream[^\n]*Audio:", text) is not None
    if not has_audio and not has_video:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return MediaInfo(
        format=mime_type.removeprefix("audio/").removeprefix("video/"),
        duration_seconds=duration,
        width=width,
        height=height,
        has_audio=has_audio,
        has_video=has_video,
    )


def render_video_preview(data: bytes, *, max_bytes: int) -> tuple[bytes, MediaInfo]:
    info = inspect_media(data, sniff_mime(data))
    if not info.has_video:
        raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input.video", data)
        output = directory / "preview.png"
        preview = _run_bounded_file(
            [
                _ffmpeg_command(),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-i",
                source.as_posix(),
                "-frames:v",
                "1",
                "-vf",
                "scale=2048:-2:force_original_aspect_ratio=decrease",
                "-f",
                "image2",
                "-vcodec",
                "png",
                output.as_posix(),
            ],
            output,
            max_output_bytes=max_bytes,
            stdout_to_output=False,
        )
    if sniff_mime(preview) != "image/png":
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    inspect_image(preview)
    return preview, info


def extract_wechat_wxgf_hevc(data: bytes) -> bytes:
    """Extract the bounded Annex-B HEVC stream from a WXGF image container."""

    if not data.startswith(WXGF_MAGIC) or len(data) > MAX_SOURCE_BYTES:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    start = data.find(b"\x00\x00\x00\x01", len(WXGF_MAGIC), _MAX_WXGF_HEADER_BYTES)
    if start < 0:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return data[start:]


def decode_wechat_wxgf_preview(data: bytes, *, max_bytes: int) -> bytes:
    """Decode the first WXGF/HEVC frame to a bounded PNG preview."""

    if max_bytes < 1:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    hevc = extract_wechat_wxgf_hevc(data)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input.hevc", hevc)
        output = directory / "preview.png"
        preview = _run_bounded_file(
            [
                _ffmpeg_command(),
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "hevc",
                "-i",
                source.as_posix(),
                "-frames:v",
                "1",
                "-f",
                "image2",
                "-vcodec",
                "png",
                output.as_posix(),
            ],
            output,
            max_output_bytes=max_bytes,
            stdout_to_output=False,
        )
    if not preview.startswith(b"\x89PNG\r\n\x1a\n"):
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return preview


def _private_directory():
    return temporary_workspace(
        "sightglass-resource-", 2 * MAX_SOURCE_BYTES + MAX_EXTRACTED_TEXT_BYTES,
    )


def _header_image_dimensions(data: bytes) -> tuple[int, int] | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
    if data.startswith((b"GIF87a", b"GIF89a")) and len(data) >= 10:
        return int.from_bytes(data[6:8], "little"), int.from_bytes(data[8:10], "little")
    return None


def _require_safe_dimensions(width: int, height: int) -> None:
    if (
        width < 1
        or height < 1
        or width > MAX_IMAGE_DIMENSION
        or height > MAX_IMAGE_DIMENSION
        or width * height > MAX_IMAGE_PIXELS
    ):
        raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)


def inspect_image(data: bytes) -> ImageInfo:
    if data.startswith(WXGF_MAGIC):
        return inspect_image(decode_wechat_wxgf_preview(data, max_bytes=MAX_SOURCE_BYTES))
    header_dimensions = _header_image_dimensions(data)
    if header_dimensions is not None:
        _require_safe_dimensions(*header_dimensions)
    backend = _image_backend()
    return backend.inspect(data)

def image_preview(data: bytes, *, max_bytes: int) -> tuple[bytes, ImageInfo]:
    if data.startswith(WXGF_MAGIC):
        decoded = decode_wechat_wxgf_preview(data, max_bytes=MAX_SOURCE_BYTES)
        return image_preview(decoded, max_bytes=max_bytes)
    backend = _image_backend()
    return backend.preview(data, max_bytes=max_bytes)

class _ImageBackend:
    """One inspected/preview-capable image backend, probed before every use."""

    def inspect(self, data: bytes) -> ImageInfo:
        raise NotImplementedError

    def preview(self, data: bytes, *, max_bytes: int) -> tuple[bytes, ImageInfo]:
        raise NotImplementedError

class _SipsImageBackend(_ImageBackend):
    """macOS ``sips`` backend; the original local image path, unchanged."""

    def inspect(self, data: bytes) -> ImageInfo:
        return _sips_inspect_image(data)

    def preview(self, data: bytes, *, max_bytes: int) -> tuple[bytes, ImageInfo]:
        return _sips_image_preview(data, max_bytes=max_bytes)

class _VipsImageBackend(_ImageBackend):
    """Linux libvips backend with bounded ``vipsheader``/``vipsthumbnail`` calls.

    libvips owns the loader set, so HEIF/TIFF/BMP support follows the operator's
    installed ``libheif``/``libtiff`` loaders instead of a Sightglass guessing table.
    """

    def inspect(self, data: bytes) -> ImageInfo:
        return _vips_inspect_image(data)

    def preview(self, data: bytes, *, max_bytes: int) -> tuple[bytes, ImageInfo]:
        return _vips_image_preview(data, max_bytes=max_bytes)

def _sips_inspect_image(data: bytes) -> ImageInfo:
    command = _command("sips")
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input", data)
        output = directory / "info.txt"
        raw = _run_bounded_file(
            [
                command,
                "-g",
                "pixelWidth",
                "-g",
                "pixelHeight",
                "-g",
                "format",
                source.as_posix(),
            ],
            output,
            max_output_bytes=64 * 1024,
            stdout_to_output=True,
        )
    values = {
        match.group(1): match.group(2).strip()
        for match in re.finditer(
            r"^\s*(pixelWidth|pixelHeight|format):\s*(.+)$", raw.decode(), re.M
        )
    }
    try:
        width = int(values["pixelWidth"])
        height = int(values["pixelHeight"])
        format_name = values["format"]
    except (KeyError, ValueError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    _require_safe_dimensions(width, height)
    animated = data.startswith((b"GIF87a", b"GIF89a"))
    return ImageInfo(width, height, format_name, animated)

def _sips_image_preview(data: bytes, *, max_bytes: int) -> tuple[bytes, ImageInfo]:
    command = _command("sips")
    # Inspect through the public seam so callers (and tests) can bound/short-circuit
    # metadata probing; the backend itself stays a thin wrapper over ``sips``.
    source_info = inspect_image(data)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input", data)
        output = directory / "preview.png"
        argv = [command]
        if max(source_info.width, source_info.height) > 2048:
            argv.extend(["-Z", "2048"])
        argv.extend(["-s", "format", "png", source.as_posix(), "--out", output.as_posix()])
        preview = _run_bounded_file(
            argv,
            output,
            max_output_bytes=max_bytes,
            stdout_to_output=False,
        )
    if sniff_mime(preview) != "image/png":
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return preview, inspect_image(preview)

def _vips_inspect_image(data: bytes) -> ImageInfo:
    header = _command("vipsheader")
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input", data)
        output = directory / "info.txt"
        raw = _run_bounded_file(
            [header, "--all", source.as_posix()],
            output,
            max_output_bytes=64 * 1024,
            stdout_to_output=True,
            timeout_seconds=_VIPS_HEADER_TIMEOUT_SECONDS,
        )
    values = {
        match.group(1): match.group(2).strip()
        for match in re.finditer(
            r"^(width|height|n-pages):\s*(.+)$", raw.decode("utf-8", "replace"), re.M
        )
    }
    try:
        width = int(values["width"])
        height = int(values["height"])
        pages = int(values.get("n-pages", "1"))
    except (KeyError, ValueError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    _require_safe_dimensions(width, height)
    # libvips's `format` is the sample type (for example uchar), not the image
    # container. Keep ImageInfo's format names aligned with the macOS backend.
    mime = sniff_mime(data)
    if not mime.startswith("image/") or pages < 1:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    format_name = mime.removeprefix("image/")
    animated = pages > 1 or data.startswith((b"GIF87a", b"GIF89a"))
    return ImageInfo(width, height, format_name, animated)

def _vips_image_preview(data: bytes, *, max_bytes: int) -> tuple[bytes, ImageInfo]:
    thumbnail = _command("vipsthumbnail")
    inspect_image(data)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input", data)
        output = directory / "preview.png"
        preview = _run_bounded_file(
            [
                thumbnail,
                f"--size={_PREVIEW_SCALE_BOUND}x{_PREVIEW_SCALE_BOUND}>",
                f"--output={output.as_posix()}",
                source.as_posix(),
            ],
            output,
            max_output_bytes=max_bytes,
            stdout_to_output=False,
            timeout_seconds=_VIPS_HEADER_TIMEOUT_SECONDS,
        )
    if sniff_mime(preview) != "image/png":
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return preview, _vips_inspect_image(preview)

_SIPS_BACKEND = _SipsImageBackend()
_VIPS_BACKEND = _VipsImageBackend()

def _image_backend() -> _ImageBackend:
    """Select one probed image backend for this platform, or fail closed."""

    if sys.platform == "darwin":
        return _SIPS_BACKEND
    if _vips_available():
        return _VIPS_BACKEND
    raise SightglassError(
        ErrorCode.RESOURCE_UNAVAILABLE,
        details={"reason": "image_backend_unavailable"},
    )

def _vips_available() -> bool:
    return shutil.which("vipsheader") is not None and shutil.which("vipsthumbnail") is not None

def image_preview_processor_version() -> str:
    """The recipe version a generated image preview is attributed to on this platform.

    It changes when the platform backend changes (macOS ``sips`` versus Linux
    ``libvips``), so a cached
    preview produced by one backend is never served as another backend's provenance.
    Content-free: it names only the backend, never a path or a version string.
    """

    if sys.platform == "darwin":
        return "sips-v1"
    return "vips-v2" if _vips_available() else "vips-unavailable"

def _pdf_has_encrypt_marker(data: bytes) -> bool:
    trailer = data.rfind(b"trailer")
    startxref = data.rfind(b"startxref")
    return trailer >= 0 and startxref > trailer and b"/Encrypt" in data[trailer:startxref]


def inspect_pdf(data: bytes) -> PdfInfo:
    if _pdf_has_encrypt_marker(data):
        raise SightglassError(
            ErrorCode.RESOURCE_BLOCKED,
            details={"reason": "encrypted_pdf"},
        )
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input.pdf", data)
        output = directory / "pdfinfo.txt"
        raw = _run_bounded_file(
            [_command("pdfinfo"), source.as_posix()],
            output,
            max_output_bytes=128 * 1024,
            stdout_to_output=True,
        )
    values = {
        key.strip(): value.strip()
        for key, value in (
            line.split(":", 1)
            for line in raw.decode("utf-8", "replace").splitlines()
            if ":" in line
        )
    }
    if values.get("Encrypted", "no").casefold().startswith("yes"):
        raise SightglassError(
            ErrorCode.RESOURCE_BLOCKED,
            details={"reason": "encrypted_pdf"},
        )
    try:
        pages = int(values["Pages"])
    except (KeyError, ValueError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    if pages < 1 or pages > MAX_PDF_PAGES:
        raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
    return PdfInfo(pages, values.get("Page size"), values.get("PDF version"))


def extract_pdf_text(data: bytes, *, page: int | None = None) -> tuple[str, PdfInfo]:
    info = inspect_pdf(data)
    if page is not None and not 1 <= page <= info.page_count:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input.pdf", data)
        output = directory / "text.txt"
        command = [_command("pdftotext"), "-enc", "UTF-8"]
        if page is not None:
            command.extend(["-f", str(page), "-l", str(page)])
        command.extend([source.as_posix(), "-"])
        raw = _run_bounded_file(
            command,
            output,
            max_output_bytes=MAX_EXTRACTED_TEXT_BYTES,
            stdout_to_output=True,
        )
    return decode_text(raw).rstrip("\n\f"), info


def render_pdf_page(data: bytes, *, page: int, max_bytes: int) -> tuple[bytes, PdfInfo]:
    info = inspect_pdf(data)
    if not 1 <= page <= info.page_count:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    with _private_directory() as temporary_name:
        directory = Path(temporary_name)
        os.chmod(directory, 0o700)
        source = _private_file(directory, "input.pdf", data)
        prefix = directory / "page"
        output = directory / "page.png"
        rendered = _run_bounded_file(
            [
                _command("pdftoppm"),
                "-f",
                str(page),
                "-l",
                str(page),
                "-singlefile",
                "-png",
                "-scale-to",
                "2048",
                source.as_posix(),
                prefix.as_posix(),
            ],
            output,
            max_output_bytes=max_bytes,
            stdout_to_output=False,
        )
    if sniff_mime(rendered) != "image/png":
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    inspect_image(rendered)
    return rendered, info
