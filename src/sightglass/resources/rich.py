from __future__ import annotations

import csv
import io
import json
import posixpath
import re
import stat
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import PurePosixPath
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError

ZIP_MIME = "application/zip"
DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

JSON_MIMES = frozenset({"application/json", "text/json"})
XML_MIMES = frozenset({"application/xml", "text/xml"})
HTML_MIMES = frozenset({"text/html", "application/xhtml+xml"})
CSV_MIMES = frozenset({"text/csv", "application/csv"})
TSV_MIMES = frozenset({"text/tab-separated-values", "text/tsv"})
OFFICE_MIMES = frozenset({DOCX_MIME, XLSX_MIME, PPTX_MIME})
TEXTUAL_RICH_MIMES = JSON_MIMES | XML_MIMES | HTML_MIMES | CSV_MIMES | TSV_MIMES
RICH_MIMES = frozenset(
    {ZIP_MIME, *OFFICE_MIMES, *JSON_MIMES, *XML_MIMES, *HTML_MIMES, *CSV_MIMES, *TSV_MIMES}
)

MAX_ARCHIVE_MEMBERS = 2_048
MAX_ARCHIVE_TOTAL_BYTES = 64 * 1024 * 1024
MAX_ARCHIVE_MEMBER_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_COMPRESSION_RATIO = 1_000.0
MAX_ARCHIVE_FILENAME_BYTES = 1_024
MAX_ARCHIVE_DEPTH = 1
MAX_XML_BYTES = 16 * 1024 * 1024
MAX_XML_NODES = 100_000
MAX_XML_DEPTH = 64
MAX_STRUCTURED_NODES = 100_000
MAX_STRUCTURED_DEPTH = 64
MAX_TABLE_ROWS = 50_000
MAX_TABLE_CELLS = 250_000
MAX_CELL_RANGE_CELLS = 10_000
MAX_SEMANTIC_NAME_BYTES = 1_024

_ALLOWED_ZIP_COMPRESSION = frozenset(
    {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
)
_NESTED_ARCHIVE_SUFFIXES = (
    ".zip",
    ".docx",
    ".xlsx",
    ".pptx",
    ".jar",
    ".apk",
    ".epub",
)
_TEXT_MEMBER_SUFFIXES = frozenset(
    {
        ".txt",
        ".md",
        ".markdown",
        ".json",
        ".xml",
        ".html",
        ".htm",
        ".csv",
        ".tsv",
        ".py",
        ".js",
        ".ts",
        ".tsx",
        ".jsx",
        ".css",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".cfg",
        ".log",
        ".sql",
        ".sh",
        ".zsh",
    }
)


@dataclass(frozen=True)
class ArchiveMember:
    name: str
    compressed_bytes: int
    uncompressed_bytes: int
    compression_method: int
    crc: int
    nested_archive: bool
    is_directory: bool


@dataclass(frozen=True)
class ArchiveInspection:
    members: tuple[ArchiveMember, ...]
    total_compressed_bytes: int
    total_uncompressed_bytes: int


class _DuplicateJsonKey(ValueError):
    pass


def _blocked(reason: str) -> SightglassError:
    return SightglassError(ErrorCode.RESOURCE_BLOCKED, details={"reason": reason})


def _too_large(reason: str) -> SightglassError:
    return SightglassError(ErrorCode.RESOURCE_TOO_LARGE, details={"reason": reason})


def _decode_utf8(data: bytes) -> str:
    if b"\x00" in data:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    controls = sum(ord(value) < 32 and value not in "\n\r\t\f" for value in text)
    if text and controls / len(text) > 0.01:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _local_name(value: str) -> str:
    return value.rsplit("}", 1)[-1]


def _xml_root(data: bytes) -> tuple[ET.Element, int, int]:
    if len(data) > MAX_XML_BYTES:
        raise _too_large("xml_bytes")
    folded = data.lower()
    if b"<!doctype" in folded or b"<!entity" in folded:
        raise _blocked("xml_dtd_or_entity")
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    nodes = 0
    maximum_depth = 0
    stack = [(root, 1)]
    while stack:
        element, depth = stack.pop()
        nodes += 1
        maximum_depth = max(maximum_depth, depth)
        if nodes > MAX_XML_NODES:
            raise _too_large("xml_nodes")
        if depth > MAX_XML_DEPTH:
            raise _too_large("xml_depth")
        stack.extend((child, depth + 1) for child in reversed(list(element)))
    return root, nodes, maximum_depth


def _json_value(data: bytes) -> tuple[Any, int, int]:
    text = _decode_utf8(data)

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise _DuplicateJsonKey
            value[key] = item
        return value

    def invalid_constant(_value: str) -> None:
        raise ValueError

    try:
        value = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=invalid_constant,
        )
    except _DuplicateJsonKey as exc:
        raise _blocked("json_duplicate_key") from exc
    except RecursionError as exc:
        raise _too_large("structured_depth") from exc
    except (json.JSONDecodeError, ValueError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    nodes = 0
    maximum_depth = 0
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        nodes += 1
        maximum_depth = max(maximum_depth, depth)
        if nodes > MAX_STRUCTURED_NODES:
            raise _too_large("structured_nodes")
        if depth > MAX_STRUCTURED_DEPTH:
            raise _too_large("structured_depth")
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
    return value, nodes, maximum_depth


def _safe_archive_name(name: str, *, is_directory: bool) -> str:
    if not name or "\x00" in name or "\\" in name:
        raise _blocked("archive_member_name")
    if len(name.encode("utf-8")) > MAX_ARCHIVE_FILENAME_BYTES:
        raise _too_large("filename_bytes")
    if name.startswith("/") or re.match(r"^[A-Za-z]:", name):
        raise _blocked("archive_absolute_path")
    candidate = name[:-1] if is_directory and name.endswith("/") else name
    path = PurePosixPath(candidate)
    if not candidate or any(part in {"", ".", ".."} for part in path.parts):
        raise _blocked("archive_path_traversal")
    normalized = path.as_posix()
    if normalized != candidate:
        raise _blocked("archive_ambiguous_name")
    return normalized + ("/" if is_directory else "")


def _inspect_archive(data: bytes) -> ArchiveInspection:
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except (zipfile.BadZipFile, OSError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    try:
        infos = archive.infolist()
        if len(infos) > MAX_ARCHIVE_MEMBERS:
            raise _too_large("member_count")
        members: list[ArchiveMember] = []
        seen_exact: set[str] = set()
        seen_ambiguous: set[str] = set()
        total_compressed = 0
        total_uncompressed = 0
        for info in infos:
            directory = info.is_dir()
            name = _safe_archive_name(info.filename, is_directory=directory)
            identity = unicodedata.normalize("NFC", name).casefold()
            if name in seen_exact:
                raise _blocked("archive_duplicate_name")
            if identity in seen_ambiguous:
                raise _blocked("archive_ambiguous_name")
            seen_exact.add(name)
            seen_ambiguous.add(identity)
            if info.flag_bits & 0x1:
                raise _blocked("archive_encrypted_entry")
            if info.compress_type not in _ALLOWED_ZIP_COMPRESSION:
                raise _blocked("archive_compression_method")
            unix_mode = info.external_attr >> 16
            file_type = stat.S_IFMT(unix_mode)
            if file_type == stat.S_IFLNK:
                raise _blocked("archive_symlink_entry")
            if file_type not in {0, stat.S_IFREG, stat.S_IFDIR}:
                raise _blocked("archive_special_entry")
            if info.file_size > MAX_ARCHIVE_MEMBER_BYTES:
                raise _too_large("member_uncompressed_bytes")
            total_compressed += info.compress_size
            total_uncompressed += info.file_size
            if total_uncompressed > MAX_ARCHIVE_TOTAL_BYTES:
                raise _too_large("total_uncompressed_bytes")
            if info.file_size:
                if info.compress_size == 0:
                    raise _too_large("compression_ratio")
                if info.file_size / info.compress_size > MAX_ARCHIVE_COMPRESSION_RATIO:
                    raise _too_large("compression_ratio")
            members.append(
                ArchiveMember(
                    name=name,
                    compressed_bytes=info.compress_size,
                    uncompressed_bytes=info.file_size,
                    compression_method=info.compress_type,
                    crc=info.CRC,
                    nested_archive=(
                        not directory and name.casefold().endswith(_NESTED_ARCHIVE_SUFFIXES)
                    ),
                    is_directory=directory,
                )
            )
        return ArchiveInspection(tuple(members), total_compressed, total_uncompressed)
    finally:
        archive.close()


def _read_archive_member_bytes(data: bytes, member: ArchiveMember, *, max_bytes: int) -> bytes:
    if member.is_directory:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    if member.uncompressed_bytes > max_bytes:
        raise SightglassError(
            ErrorCode.RESOURCE_TOO_LARGE,
            details={"max_bytes": max_bytes},
        )
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            with archive.open(member.name, "r") as source:
                payload = source.read(max_bytes + 1)
    except (zipfile.BadZipFile, KeyError, RuntimeError, EOFError, OSError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    if len(payload) > max_bytes or len(payload) != member.uncompressed_bytes:
        raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE, details={"max_bytes": max_bytes})
    return payload


def _find_archive_member(inspection: ArchiveInspection, selected: str) -> ArchiveMember:
    if not selected or selected != _safe_archive_name(selected, is_directory=False):
        raise SightglassError(ErrorCode.QUERY_INVALID)
    matches = [member for member in inspection.members if member.name == selected]
    if len(matches) != 1:
        raise SightglassError(ErrorCode.RESOURCE_NOT_FOUND)
    return matches[0]


def _json_size(value: Any) -> tuple[int, int]:
    serialized = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return len(serialized), len(serialized.encode("utf-8"))


def _fits(value: Any, *, max_chars: int, max_bytes: int) -> bool:
    chars, byte_count = _json_size(value)
    return chars <= max_chars and byte_count <= max_bytes


def list_archive_members(data: bytes, *, max_chars: int, max_bytes: int) -> dict[str, Any]:
    inspection = _inspect_archive(data)
    result: dict[str, Any] = {
        "kind": "archive",
        "member_count": len(inspection.members),
        "total_compressed_bytes": inspection.total_compressed_bytes,
        "total_uncompressed_bytes": inspection.total_uncompressed_bytes,
        "max_nested_depth": MAX_ARCHIVE_DEPTH,
        "members": [],
        "truncated": False,
    }
    for member in inspection.members:
        item = {
            "name": member.name,
            "directory": member.is_directory,
            "compressed_bytes": member.compressed_bytes,
            "uncompressed_bytes": member.uncompressed_bytes,
            "nested_archive": member.nested_archive,
        }
        proposal = {**result, "members": [*result["members"], item]}
        if not _fits(proposal, max_chars=max_chars, max_bytes=max_bytes):
            result["truncated"] = True
            break
        result["members"].append(item)
    if inspection.members and not result["members"]:
        raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
    return result


def read_archive_text_member(
    data: bytes,
    member_name: str,
    *,
    max_chars: int,
    max_bytes: int,
) -> tuple[str, dict[str, Any]]:
    inspection = _inspect_archive(data)
    member = _find_archive_member(inspection, member_name)
    if member.nested_archive:
        raise _blocked("nested_archive_depth")
    suffix = PurePosixPath(member.name).suffix.casefold()
    if suffix not in _TEXT_MEMBER_SUFFIXES:
        raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
    payload = _read_archive_member_bytes(data, member, max_bytes=max_bytes)
    if payload.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        raise _blocked("nested_archive_depth")
    mime_type = _structured_mime_for_name(member.name)
    if mime_type in JSON_MIMES:
        _json_value(payload)
    elif mime_type in XML_MIMES:
        _xml_root(payload)
    elif mime_type in HTML_MIMES:
        _html_details(payload)
    elif mime_type in CSV_MIMES | TSV_MIMES:
        _csv_details(payload, delimiter="," if mime_type in CSV_MIMES else "\t")
    text = _decode_utf8(payload)
    text, truncated = _bounded_text(text, max_chars=max_chars, max_bytes=max_bytes)
    return text, {
        "member": member.name,
        "mime_type": mime_type or "text/plain",
        "truncated": truncated,
        "archive_depth": MAX_ARCHIVE_DEPTH,
    }


def _structured_mime_for_name(name: str) -> str | None:
    suffix = PurePosixPath(name).suffix.casefold()
    return {
        ".json": "application/json",
        ".xml": "application/xml",
        ".html": "text/html",
        ".htm": "text/html",
        ".csv": "text/csv",
        ".tsv": "text/tab-separated-values",
    }.get(suffix)


def rich_mime_from_name(name: str | None) -> str | None:
    if not name:
        return None
    suffix = PurePosixPath(name).suffix.casefold()
    return {
        ".zip": ZIP_MIME,
        ".docx": DOCX_MIME,
        ".xlsx": XLSX_MIME,
        ".pptx": PPTX_MIME,
        ".json": "application/json",
        ".xml": "application/xml",
        ".html": "text/html",
        ".htm": "text/html",
        ".csv": "text/csv",
        ".tsv": "text/tab-separated-values",
    }.get(suffix)


def sniff_rich_mime(data: bytes) -> str | None:
    if data.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                names = {item.filename for item in archive.infolist()}
        except (zipfile.BadZipFile, OSError):
            return ZIP_MIME
        if "word/document.xml" in names:
            return DOCX_MIME
        if "xl/workbook.xml" in names:
            return XLSX_MIME
        if "ppt/presentation.xml" in names:
            return PPTX_MIME
        return ZIP_MIME
    stripped = data.lstrip()
    if stripped.startswith((b"{", b"[")):
        try:
            _json_value(data)
        except SightglassError:
            return None
        return "application/json"
    lowered = stripped[:512].lower()
    if lowered.startswith(b"<!doctype html") or re.match(br"<html(?:\s|>)", lowered):
        return "text/html"
    if stripped.startswith((b"<?xml", b"<")):
        try:
            _xml_root(data)
        except SightglassError:
            return None
        return "application/xml"
    return None


def is_rich_mime(mime_type: str) -> bool:
    return mime_type.casefold() in RICH_MIMES


def _office_package(data: bytes, mime_type: str) -> _OfficePackage:
    package = _OfficePackage(data)
    expected = {
        DOCX_MIME: "word/document.xml",
        XLSX_MIME: "xl/workbook.xml",
        PPTX_MIME: "ppt/presentation.xml",
    }[mime_type]
    if expected not in package.members:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return package


class _OfficePackage:
    def __init__(self, data: bytes) -> None:
        self.data = data
        inspection = _inspect_archive(data)
        self.members = {
            member.name: member for member in inspection.members if not member.is_directory
        }
        lowered_names = tuple(name.casefold() for name in self.members)
        blocked_parts = (
            "vbaproject",
            "/activex/",
            "/embeddings/",
            "/externallinks/",
        )
        if any(any(marker in f"/{name}" for marker in blocked_parts) for name in lowered_names):
            raise _blocked("office_active_content")
        if any(name.endswith(".bin") for name in lowered_names):
            raise _blocked("office_binary_part")
        content_types = self.xml("[Content_Types].xml")
        if "macroenabled" in ET.tostring(content_types, encoding="unicode").casefold():
            raise _blocked("office_macro_enabled")
        for name in self.members:
            if name.casefold().endswith(".rels"):
                root = self.xml(name)
                for relationship in root.iter():
                    if _local_name(relationship.tag) != "Relationship":
                        continue
                    if relationship.attrib.get("TargetMode", "").casefold() == "external":
                        raise _blocked("office_external_relationship")

    def bytes(self, name: str) -> bytes:
        member = self.members.get(name)
        if member is None:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        return _read_archive_member_bytes(self.data, member, max_bytes=MAX_XML_BYTES)

    def xml(self, name: str) -> ET.Element:
        root, _nodes, _depth = _xml_root(self.bytes(name))
        return root

    def relationship_targets(self, rels_name: str, source_name: str) -> dict[str, str]:
        if rels_name not in self.members:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        root = self.xml(rels_name)
        targets: dict[str, str] = {}
        for relationship in root.iter():
            if _local_name(relationship.tag) != "Relationship":
                continue
            identity = relationship.attrib.get("Id")
            target = relationship.attrib.get("Target")
            if not identity or not target or "\\" in target or "\x00" in target:
                raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
            if relationship.attrib.get("TargetMode", "").casefold() == "external":
                raise _blocked("office_external_relationship")
            if target.startswith("//") or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target):
                raise _blocked("office_external_relationship")
            normalized = posixpath.normpath(
                target.lstrip("/")
                if target.startswith("/")
                else posixpath.join(posixpath.dirname(source_name), target)
            )
            if normalized.startswith("../") or normalized == ".." or normalized.startswith("/"):
                raise _blocked("office_relationship_escape")
            if identity in targets:
                raise _blocked("office_duplicate_relationship")
            targets[identity] = normalized
        return targets


def _docx_details(package: _OfficePackage) -> tuple[list[str], int]:
    root = package.xml("word/document.xml")
    paragraphs: list[str] = []
    tables = 0
    for element in root.iter():
        name = _local_name(element.tag)
        if name == "tbl":
            tables += 1
        if name == "p":
            value = "".join(
                child.text or "" for child in element.iter() if _local_name(child.tag) == "t"
            ).strip()
            if value:
                paragraphs.append(value)
    return paragraphs, tables


def _xlsx_sheets(package: _OfficePackage) -> list[tuple[str, str]]:
    workbook = package.xml("xl/workbook.xml")
    targets = package.relationship_targets(
        "xl/_rels/workbook.xml.rels",
        "xl/workbook.xml",
    )
    sheets: list[tuple[str, str]] = []
    seen: set[str] = set()
    for element in workbook.iter():
        if _local_name(element.tag) != "sheet":
            continue
        name = element.attrib.get("name")
        relationship_id = next(
            (value for value in element.attrib.values() if value in targets),
            None,
        )
        if not name or not relationship_id or relationship_id not in targets:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        if len(name.encode("utf-8")) > MAX_SEMANTIC_NAME_BYTES:
            raise _too_large("semantic_name_bytes")
        identity = unicodedata.normalize("NFC", name).casefold()
        if identity in seen:
            raise _blocked("xlsx_ambiguous_sheet")
        seen.add(identity)
        target = targets[relationship_id]
        if target not in package.members:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        sheets.append((name, target))
    if not sheets:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return sheets


def _xlsx_shared_strings(package: _OfficePackage) -> list[str]:
    if "xl/sharedStrings.xml" not in package.members:
        return []
    root = package.xml("xl/sharedStrings.xml")
    values: list[str] = []
    for element in root.iter():
        if _local_name(element.tag) == "si":
            values.append(
                "".join(
                    child.text or ""
                    for child in element.iter()
                    if _local_name(child.tag) == "t"
                )
            )
    return values


def _child_text(element: ET.Element, name: str) -> str | None:
    for child in element:
        if _local_name(child.tag) == name:
            return child.text or ""
    return None


def _xlsx_rows(
    package: _OfficePackage,
    target: str,
    shared_strings: list[str],
) -> list[dict[str, Any]]:
    root = package.xml(target)
    rows: list[dict[str, Any]] = []
    cell_count = 0
    seen_rows: set[int] = set()
    seen_cells: set[str] = set()
    for row in root.iter():
        if _local_name(row.tag) != "row":
            continue
        if len(rows) >= MAX_TABLE_ROWS:
            raise _too_large("table_rows")
        try:
            row_number = int(row.attrib.get("r", str(len(rows) + 1)))
        except ValueError as exc:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
        if row_number < 1 or row_number > 1_048_576 or row_number in seen_rows:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        seen_rows.add(row_number)
        cells: list[dict[str, Any]] = []
        for cell in row:
            if _local_name(cell.tag) != "c":
                continue
            cell_count += 1
            if cell_count > MAX_TABLE_CELLS:
                raise _too_large("table_cells")
            reference = cell.attrib.get("r")
            if not reference or not re.fullmatch(r"[A-Z]{1,3}[1-9][0-9]*", reference):
                raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
            reference_row = int(reference.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
            if (
                _column_number(reference) > 16_384
                or reference_row > 1_048_576
                or reference_row != row_number
                or reference in seen_cells
            ):
                raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
            seen_cells.add(reference)
            cell_type = cell.attrib.get("t")
            raw_value = _child_text(cell, "v")
            formula = _child_text(cell, "f")
            if cell_type == "inlineStr":
                value = "".join(
                    child.text or ""
                    for child in cell.iter()
                    if _local_name(child.tag) == "t"
                )
                source = "inline_string"
            elif cell_type == "s" and raw_value is not None:
                try:
                    value = shared_strings[int(raw_value)]
                except (ValueError, IndexError) as exc:
                    raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
                source = "shared_string"
            else:
                value = raw_value
                source = "stored_value"
            if formula is not None:
                item = {
                    "reference": reference,
                    "formula": formula,
                    "cached_value": value,
                    "value_source": (
                        "cached_formula_result" if value is not None else "formula_only"
                    ),
                }
            else:
                item = {
                    "reference": reference,
                    "value": value,
                    "value_source": source,
                }
            cells.append(item)
        rows.append({"row": row_number, "cells": cells})
    return rows


def _column_number(reference: str) -> int:
    match = re.match(r"^([A-Z]{1,3})", reference)
    if match is None:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    value = 0
    for character in match.group(1):
        value = value * 26 + ord(character) - ord("A") + 1
    return value


def _parse_cell_range(value: str) -> tuple[str, int, int, int, int]:
    match = re.fullmatch(
        r"([A-Za-z]{1,3})([1-9][0-9]*)(?::([A-Za-z]{1,3})([1-9][0-9]*))?",
        value,
    )
    if match is None:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    start_reference = f"{match.group(1).upper()}{match.group(2)}"
    end_reference = (
        f"{match.group(3).upper()}{match.group(4)}"
        if match.group(3) is not None and match.group(4) is not None
        else start_reference
    )
    start_column = _column_number(start_reference)
    end_column = _column_number(end_reference)
    start_row = int(match.group(2))
    end_row = int(match.group(4) or match.group(2))
    if (
        start_column > 16_384
        or end_column > 16_384
        or start_row > 1_048_576
        or end_row > 1_048_576
        or start_column > end_column
        or start_row > end_row
    ):
        raise SightglassError(ErrorCode.QUERY_INVALID)
    selected_cells = (end_column - start_column + 1) * (end_row - start_row + 1)
    if selected_cells > MAX_CELL_RANGE_CELLS:
        raise SightglassError(
            ErrorCode.QUERY_INVALID,
            details={"max_cell_range_cells": MAX_CELL_RANGE_CELLS},
        )
    normalized = (
        start_reference if ":" not in value else f"{start_reference}:{end_reference}"
    )
    return normalized, start_column, start_row, end_column, end_row


def _select_cell_range(
    rows: list[dict[str, Any]],
    cell_range: str,
) -> tuple[str, list[dict[str, Any]]]:
    normalized, start_column, start_row, end_column, end_row = _parse_cell_range(cell_range)
    selected: list[dict[str, Any]] = []
    for row in rows:
        row_number = int(row["row"])
        if not start_row <= row_number <= end_row:
            continue
        cells = [
            cell
            for cell in row["cells"]
            if start_column <= _column_number(str(cell["reference"])) <= end_column
        ]
        if cells:
            selected.append({"row": row_number, "cells": cells})
    return normalized, selected


def _pptx_slides(package: _OfficePackage) -> list[str]:
    presentation = package.xml("ppt/presentation.xml")
    targets = package.relationship_targets(
        "ppt/_rels/presentation.xml.rels",
        "ppt/presentation.xml",
    )
    slides: list[str] = []
    for element in presentation.iter():
        if _local_name(element.tag) != "sldId":
            continue
        relationship_id = next(
            (value for value in element.attrib.values() if value in targets),
            None,
        )
        if not relationship_id or relationship_id not in targets:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        target = targets[relationship_id]
        if target not in package.members:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        slides.append(target)
    if not slides:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return slides


def _slide_text(package: _OfficePackage, target: str) -> str:
    root = package.xml(target)
    paragraphs: list[str] = []
    for element in root.iter():
        if _local_name(element.tag) != "p":
            continue
        value = "".join(
            child.text or "" for child in element.iter() if _local_name(child.tag) == "t"
        ).strip()
        if value:
            paragraphs.append(value)
    return "\n".join(paragraphs)


def _slide_notes_text(package: _OfficePackage, target: str) -> str | None:
    """Speaker notes bound to this slide through its notesSlide relationship.

    Only the slide's own notes part is read; slide masters, layouts, themes, and
    other presentation XML are never folded into the result.
    """
    rels_name = posixpath.join(
        posixpath.dirname(target),
        "_rels",
        posixpath.basename(target) + ".rels",
    )
    if rels_name not in package.members:
        return None
    targets = package.relationship_targets(rels_name, target)
    notes_target: str | None = None
    for relationship in package.xml(rels_name).iter():
        if _local_name(relationship.tag) != "Relationship":
            continue
        relationship_type = relationship.attrib.get("Type", "")
        if not relationship_type.endswith("/notesSlide"):
            continue
        relationship_id = relationship.attrib.get("Id")
        resolved = targets.get(relationship_id) if relationship_id else None
        if resolved is None:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        if notes_target is not None and notes_target != resolved:
            raise _blocked("pptx_ambiguous_notes")
        notes_target = resolved
    if notes_target is None:
        return None
    if notes_target not in package.members:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return _slide_text(package, notes_target)


class _VisibleHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.depth = 0
        self.elements = 0
        self.values: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        self.elements += 1
        self.depth += 1
        if self.elements > MAX_XML_NODES:
            raise _too_large("html_nodes")
        if self.depth > MAX_XML_DEPTH:
            raise _too_large("html_depth")
        if tag.casefold() in {"script", "style", "template", "noscript"}:
            self.hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() in {"script", "style", "template", "noscript"} and self.hidden_depth:
            self.hidden_depth -= 1
        self.depth = max(0, self.depth - 1)

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth and data.strip():
            self.values.append(data.strip())


def _html_details(data: bytes) -> tuple[str, int]:
    text = _decode_utf8(data)
    parser = _VisibleHtmlParser()
    try:
        parser.feed(text)
        parser.close()
    except (ValueError, AssertionError) as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    return "\n".join(parser.values), parser.elements


def _csv_details(data: bytes, *, delimiter: str) -> tuple[list[list[str]], int]:
    text = _decode_utf8(data)
    rows: list[list[str]] = []
    cells = 0
    try:
        for row in csv.reader(io.StringIO(text), delimiter=delimiter, strict=True):
            if len(rows) >= MAX_TABLE_ROWS:
                raise _too_large("table_rows")
            cells += len(row)
            if cells > MAX_TABLE_CELLS:
                raise _too_large("table_cells")
            rows.append(row)
    except csv.Error as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    return rows, cells


def _bounded_text(text: str, *, max_chars: int, max_bytes: int | None = None) -> tuple[str, bool]:
    if max_chars < 1 or (max_bytes is not None and max_bytes < 1):
        raise SightglassError(ErrorCode.QUERY_INVALID)
    limit = min(len(text), max_chars)
    selected_length = limit
    if max_bytes is not None:
        low = 0
        high = limit
        while low < high:
            middle = (low + high + 1) // 2
            if len(text[:middle].encode("utf-8")) <= max_bytes:
                low = middle
            else:
                high = middle - 1
        selected_length = low
    selected = text[:selected_length]
    if text and not selected:
        raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
    return selected, len(selected) < len(text)


def bound_text(text: str, *, max_chars: int, max_bytes: int | None = None) -> tuple[str, bool]:
    """Bound derived text on the shared character and UTF-8 byte budgets.

    The reader's derived-text paths share this so no transcript can bypass the
    ordinary resource text budgets or return a byte-truncated partial character.
    """

    return _bounded_text(text, max_chars=max_chars, max_bytes=max_bytes)


def inspect_rich_metadata(data: bytes, mime_type: str) -> dict[str, Any]:
    mime_type = mime_type.casefold()
    if mime_type == ZIP_MIME:
        inspection = _inspect_archive(data)
        return {
            "kind": "archive",
            "archive": {
                "member_count": len(inspection.members),
                "total_compressed_bytes": inspection.total_compressed_bytes,
                "total_uncompressed_bytes": inspection.total_uncompressed_bytes,
                "max_nested_depth": MAX_ARCHIVE_DEPTH,
            },
        }
    if mime_type == DOCX_MIME:
        package = _office_package(data, mime_type)
        paragraphs, tables = _docx_details(package)
        return {
            "kind": "document",
            "document": {"paragraph_count": len(paragraphs), "table_count": tables},
        }
    if mime_type == XLSX_MIME:
        package = _office_package(data, mime_type)
        sheets = _xlsx_sheets(package)
        return {
            "kind": "workbook",
            "workbook": {"sheet_count": len(sheets), "sheets": [name for name, _ in sheets]},
        }
    if mime_type == PPTX_MIME:
        package = _office_package(data, mime_type)
        slides = _pptx_slides(package)
        return {"kind": "presentation", "presentation": {"slide_count": len(slides)}}
    if mime_type in JSON_MIMES:
        value, nodes, depth = _json_value(data)
        return {
            "kind": "json",
            "json": {
                "top_level": type(value).__name__,
                "item_count": len(value) if isinstance(value, (dict, list)) else 1,
                "node_count": nodes,
                "max_depth": depth,
            },
        }
    if mime_type in XML_MIMES:
        root, nodes, depth = _xml_root(data)
        root_name = _local_name(root.tag)
        if len(root_name.encode("utf-8")) > MAX_SEMANTIC_NAME_BYTES:
            raise _too_large("semantic_name_bytes")
        return {
            "kind": "xml",
            "xml": {
                "root": root_name,
                "node_count": nodes,
                "max_depth": depth,
            },
        }
    if mime_type in HTML_MIMES:
        _text, elements = _html_details(data)
        return {"kind": "html", "html": {"element_count": elements}}
    if mime_type in CSV_MIMES | TSV_MIMES:
        rows, cells = _csv_details(data, delimiter="," if mime_type in CSV_MIMES else "\t")
        return {
            "kind": "table",
            "table": {
                "row_count": len(rows),
                "cell_count": cells,
                "max_columns": max((len(row) for row in rows), default=0),
                "delimiter": "," if mime_type in CSV_MIMES else "tab",
            },
        }
    raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)


def _xml_visible_text(data: bytes) -> str:
    root, _nodes, _depth = _xml_root(data)
    values = [value.strip() for value in root.itertext() if value.strip()]
    return "\n".join(values)


def extract_rich_text(
    data: bytes,
    mime_type: str,
    *,
    max_chars: int,
    member: str | None = None,
    max_bytes: int | None = None,
) -> tuple[str, dict[str, Any]]:
    mime_type = mime_type.casefold()
    if mime_type == ZIP_MIME:
        if member is None:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return read_archive_text_member(
            data,
            member,
            max_chars=max_chars,
            max_bytes=MAX_ARCHIVE_MEMBER_BYTES if max_bytes is None else max_bytes,
        )
    if member is not None:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    detail: dict[str, Any] = {"truncated": False}
    if mime_type == DOCX_MIME:
        paragraphs, _tables = _docx_details(_office_package(data, mime_type))
        text = "\n".join(paragraphs)
        detail["kind"] = "extracted_docx_text"
    elif mime_type == XLSX_MIME:
        package = _office_package(data, mime_type)
        shared = _xlsx_shared_strings(package)
        values: list[str] = []
        for sheet_name, target in _xlsx_sheets(package):
            values.append(f"Sheet: {sheet_name}")
            for row in _xlsx_rows(package, target, shared):
                rendered: list[str] = []
                for cell in row["cells"]:
                    if "formula" in cell:
                        rendered.append(
                            f"{cell['reference']}: {cell['cached_value'] or ''} "
                            f"[formula: {cell['formula']}]"
                        )
                    else:
                        rendered.append(f"{cell['reference']}: {cell['value'] or ''}")
                if rendered:
                    values.append("\t".join(rendered))
        text = "\n".join(values)
        detail["kind"] = "extracted_xlsx_text"
        detail["formula_values"] = "cached_only"
    elif mime_type == PPTX_MIME:
        package = _office_package(data, mime_type)
        values = []
        for number, target in enumerate(_pptx_slides(package), start=1):
            values.append(f"Slide {number}\n{_slide_text(package, target)}".rstrip())
            notes = _slide_notes_text(package, target)
            if notes:
                values.append(f"Notes\n{notes}")
        text = "\n\n".join(values)
        detail["kind"] = "extracted_pptx_text"
    elif mime_type in JSON_MIMES:
        _json_value(data)
        text = _decode_utf8(data)
        detail["kind"] = "validated_json_text"
    elif mime_type in XML_MIMES:
        text = _xml_visible_text(data)
        detail["kind"] = "extracted_xml_text"
    elif mime_type in HTML_MIMES:
        text, _elements = _html_details(data)
        detail["kind"] = "extracted_html_text"
    elif mime_type in CSV_MIMES | TSV_MIMES:
        _csv_details(data, delimiter="," if mime_type in CSV_MIMES else "\t")
        text = _decode_utf8(data)
        detail["kind"] = "validated_delimited_text"
    else:
        raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
    text, truncated = _bounded_text(text, max_chars=max_chars, max_bytes=max_bytes)
    detail["truncated"] = truncated
    return text, detail


def _fit_rows(
    result: dict[str, Any],
    rows: Iterable[dict[str, Any]],
    *,
    max_chars: int,
    max_bytes: int,
) -> dict[str, Any]:
    saw_row = False
    for row in rows:
        saw_row = True
        proposal = {**result, "rows": [*result["rows"], row]}
        if not _fits(proposal, max_chars=max_chars, max_bytes=max_bytes):
            result["truncated"] = True
            break
        result["rows"].append(row)
    if saw_row and not result["rows"]:
        raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
    return result


def extract_table(
    data: bytes,
    mime_type: str,
    *,
    sheet: str | None,
    cell_range: str | None = None,
    max_chars: int,
    max_bytes: int,
) -> dict[str, Any]:
    mime_type = mime_type.casefold()
    if mime_type in CSV_MIMES | TSV_MIMES:
        if sheet is not None or cell_range is not None:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        rows, _cells = _csv_details(data, delimiter="," if mime_type in CSV_MIMES else "\t")
        result: dict[str, Any] = {
            "kind": "table",
            "sheet": None,
            "cell_range": None,
            "rows": [],
            "truncated": False,
        }
        return _fit_rows(
            result,
            ({"row": number, "cells": cells} for number, cells in enumerate(rows, start=1)),
            max_chars=max_chars,
            max_bytes=max_bytes,
        )
    if mime_type != XLSX_MIME:
        raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
    package = _office_package(data, mime_type)
    sheets = _xlsx_sheets(package)
    selected = sheets[0] if sheet is None else next(
        (item for item in sheets if item[0] == sheet),
        None,
    )
    if selected is None:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    rows = _xlsx_rows(package, selected[1], _xlsx_shared_strings(package))
    normalized_range = None
    if cell_range is not None:
        normalized_range, rows = _select_cell_range(rows, cell_range)
    result = {
        "kind": "table",
        "sheet": selected[0],
        "sheet_defaulted": sheet is None,
        "cell_range": normalized_range,
        "formula_values": "cached_only",
        "rows": [],
        "truncated": False,
    }
    return _fit_rows(result, rows, max_chars=max_chars, max_bytes=max_bytes)


def extract_slide(
    data: bytes,
    *,
    page: int,
    max_chars: int,
    max_bytes: int,
) -> dict[str, Any]:
    if page < 1:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    package = _office_package(data, PPTX_MIME)
    slides = _pptx_slides(package)
    if page > len(slides):
        raise SightglassError(ErrorCode.QUERY_INVALID)
    target = slides[page - 1]
    text, text_truncated = _bounded_text(
        _slide_text(package, target),
        max_chars=max_chars,
        max_bytes=max_bytes,
    )
    result: dict[str, Any] = {
        "kind": "slide",
        "number": page,
        "text": text,
        "truncated": text_truncated,
    }
    notes = _slide_notes_text(package, target)
    if notes:
        remaining_chars = max(1, max_chars - len(text))
        remaining_bytes = max(1, max_bytes - len(text.encode("utf-8")))
        notes_text, notes_truncated = _bounded_text(
            notes,
            max_chars=remaining_chars,
            max_bytes=remaining_bytes,
        )
        result["notes"] = notes_text
        result["truncated"] = bool(text_truncated or notes_truncated)
    return result
