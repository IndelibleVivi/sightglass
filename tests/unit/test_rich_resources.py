from __future__ import annotations

import io
import stat
import unittest
import warnings
import zipfile
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.rich import (
    DOCX_MIME,
    PPTX_MIME,
    XLSX_MIME,
    extract_rich_text,
    extract_slide,
    extract_table,
    inspect_rich_metadata,
    list_archive_members,
    read_archive_text_member,
    sniff_rich_mime,
)


def _zip(entries: list[tuple[str | zipfile.ZipInfo, bytes]]) -> bytes:
    output = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in entries:
                archive.writestr(name, payload)
    return output.getvalue()


def _mark_first_entry_encrypted(payload: bytes) -> bytes:
    value = bytearray(payload)
    local = value.find(b"PK\x03\x04")
    central = value.find(b"PK\x01\x02")
    if local < 0 or central < 0:
        raise AssertionError("synthetic ZIP lacks required headers")
    value[local + 6 : local + 8] = (
        int.from_bytes(value[local + 6 : local + 8], "little") | 0x1
    ).to_bytes(2, "little")
    value[central + 8 : central + 10] = (
        int.from_bytes(value[central + 8 : central + 10], "little") | 0x1
    ).to_bytes(2, "little")
    return bytes(value)


def _docx(
    *,
    external_relationship: bool = False,
    macro: bool = False,
    ole: bool = False,
) -> bytes:
    relationship = (
        b'<Relationship Id="rId9" Target="https://invalid.example/" '
        b'TargetMode="External" Type="urn:test"/>'
        if external_relationship
        else b""
    )
    entries: list[tuple[str | zipfile.ZipInfo, bytes]] = [
        (
            "[Content_Types].xml",
            b'<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        ),
        (
            "word/document.xml",
            b'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            b"<w:body><w:p><w:r><w:t>alpha</w:t></w:r></w:p>"
            b"<w:p><w:r><w:t>beta</w:t></w:r></w:p>"
            b"<w:tbl><w:tr><w:tc><w:p><w:r><w:t>cell</w:t></w:r></w:p>"
            b"</w:tc></w:tr></w:tbl></w:body></w:document>",
        ),
        (
            "word/_rels/document.xml.rels",
            b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + relationship
            + b"</Relationships>",
        ),
    ]
    if macro:
        entries.append(("word/vbaProject.bin", b"not-executable"))
    if ole:
        entries.append(("word/embeddings/object1.dat", b"not-readable"))
    return _zip(entries)


def _xlsx() -> bytes:
    return _zip(
        [
            (
                "[Content_Types].xml",
                b'<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
            ),
            (
                "xl/workbook.xml",
                b'<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                b'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                b'<sheets><sheet name="Budget" sheetId="1" r:id="rId1"/></sheets></workbook>',
            ),
            (
                "xl/_rels/workbook.xml.rels",
                b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                b'<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
                b'officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
                b"</Relationships>",
            ),
            (
                "xl/sharedStrings.xml",
                b'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                b"<si><t>Revenue</t></si></sst>",
            ),
            (
                "xl/worksheets/sheet1.xml",
                b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                b'<sheetData><row r="1"><c r="A1" t="s"><v>0</v></c>'
                b'<c r="B1"><f>SUM(1,2)</f><v>3</v></c></row></sheetData></worksheet>',
            ),
        ]
    )


def _pptx(*, notes: bool = False, external_notes: bool = False) -> bytes:
    slide_relationships = [
        b'<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
        b'officeDocument/2006/relationships/slide" Target="slides/slide1.xml"/>',
    ]
    entries: list[tuple[str | zipfile.ZipInfo, bytes]] = [
        (
            "[Content_Types].xml",
            b'<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        ),
        (
            "ppt/presentation.xml",
            b'<p:presentation '
            b'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
            b'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            b'<p:sldIdLst><p:sldId id="256" r:id="rId1"/></p:sldIdLst></p:presentation>',
        ),
        (
            "ppt/_rels/presentation.xml.rels",
            b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            b'<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
            b'officeDocument/2006/relationships/slide" Target="slides/slide1.xml"/>'
            b"</Relationships>",
        ),
        (
            "ppt/slides/slide1.xml",
            b'<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
            b'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
            b"<p:cSld><a:p><a:r><a:t>Launch plan</a:t></a:r></a:p>"
            b"<a:p><a:r><a:t>Ship safely</a:t></a:r></a:p></p:cSld></p:sld>",
        ),
    ]
    if external_notes:
        slide_relationships.append(
            b'<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/'
            b'officeDocument/2006/relationships/notesSlide" Target="https://invalid.example/notes" '
            b'TargetMode="External"/>'
        )
    elif notes:
        slide_relationships.append(
            b'<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/'
            b'officeDocument/2006/relationships/notesSlide" '
            b'Target="../notesSlides/notesSlide1.xml"/>'
        )
    entries.append(
        (
            "ppt/slides/_rels/slide1.xml.rels",
            b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + b"".join(slide_relationships)
            + b"</Relationships>",
        )
    )
    if notes:
        entries.append(
            (
                "ppt/notesSlides/notesSlide1.xml",
                b'<p:notes xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
                b'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
                b"<p:cSld><p:spTree><p:sp><p:txBody>"
                b"<a:p><a:r><a:t>Remind the audience to rehearse</a:t></a:r></a:p>"
                b"<a:p><a:r><a:t>Keep the demo short</a:t></a:r></a:p>"
                b"</p:txBody></p:sp></p:spTree></p:cSld></p:notes>",
            )
        )
    return _zip(entries)


class RichResourceProcessorTests(unittest.TestCase):
    def test_zip_members_and_selected_text_are_safe_and_bounded(self) -> None:
        nested = _zip([("inside.txt", b"never recurse")])
        payload = _zip(
            [
                ("notes/readme.txt", b"alpha\nbeta\n"),
                ("data.json", b'{"ok": true}'),
                ("nested.zip", nested),
            ]
        )

        metadata = inspect_rich_metadata(payload, "application/zip")
        self.assertEqual(metadata["archive"]["member_count"], 3)
        self.assertNotIn("notes/readme.txt", str(metadata))

        listing = list_archive_members(payload, max_chars=20_000, max_bytes=20_000)
        self.assertEqual(listing["member_count"], 3)
        self.assertEqual(listing["members"][0]["name"], "notes/readme.txt")
        self.assertTrue(listing["members"][2]["nested_archive"])
        self.assertNotIn("never recurse", str(listing))

        text, metadata = read_archive_text_member(
            payload,
            "data.json",
            max_chars=1_000,
            max_bytes=1_000,
        )
        self.assertEqual(text, '{"ok": true}')
        self.assertEqual(metadata["member"], "data.json")

        with self.assertRaises(SightglassError) as caught:
            read_archive_text_member(
                payload,
                "nested.zip",
                max_chars=1_000,
                max_bytes=1_000,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], "nested_archive_depth")

        with self.assertRaises(SightglassError) as caught:
            read_archive_text_member(
                payload,
                "../data.json",
                max_chars=1_000,
                max_bytes=1_000,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

        opaque = _zip([("opaque.bin", b"not executable")])
        with self.assertRaises(SightglassError) as caught:
            read_archive_text_member(
                opaque,
                "opaque.bin",
                max_chars=1_000,
                max_bytes=1_000,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNSUPPORTED)

    def test_archive_rejects_unsafe_or_ambiguous_entries(self) -> None:
        link = zipfile.ZipInfo("link")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        cases = {
            "traversal": (_zip([("../escape.txt", b"x")]), "archive_path_traversal"),
            "absolute": (_zip([("/absolute.txt", b"x")]), "archive_absolute_path"),
            "windows_absolute": (
                _zip([("C:/absolute.txt", b"x")]),
                "archive_absolute_path",
            ),
            "symlink": (_zip([(link, b"target")]), "archive_symlink_entry"),
            "duplicate": (
                _zip([("same.txt", b"one"), ("same.txt", b"two")]),
                "archive_duplicate_name",
            ),
            "ambiguous": (
                _zip([("A.txt", b"one"), ("a.txt", b"two")]),
                "archive_ambiguous_name",
            ),
            "encrypted": (
                _mark_first_entry_encrypted(_zip([("secret.txt", b"secret")])),
                "archive_encrypted_entry",
            ),
        }
        for label, (payload, reason) in cases.items():
            with self.subTest(label=label), self.assertRaises(SightglassError) as caught:
                list_archive_members(payload, max_chars=10_000, max_bytes=10_000)
            self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
            self.assertEqual(caught.exception.details["reason"], reason)

    def test_archive_enforces_every_declared_limit(self) -> None:
        payload = _zip([("one.txt", b"1234"), ("two.txt", b"5678")])
        patches = (
            ("MAX_ARCHIVE_MEMBERS", 1, "member_count"),
            ("MAX_ARCHIVE_TOTAL_BYTES", 7, "total_uncompressed_bytes"),
            ("MAX_ARCHIVE_MEMBER_BYTES", 3, "member_uncompressed_bytes"),
            ("MAX_ARCHIVE_FILENAME_BYTES", 4, "filename_bytes"),
            ("MAX_ARCHIVE_COMPRESSION_RATIO", 0.1, "compression_ratio"),
        )
        for constant, value, reason in patches:
            with self.subTest(constant=constant):
                with patch(f"sightglass.resources.rich.{constant}", value):
                    with self.assertRaises(SightglassError) as caught:
                        list_archive_members(payload, max_chars=10_000, max_bytes=10_000)
                self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_TOO_LARGE)
                self.assertEqual(caught.exception.details["reason"], reason)

    def test_office_packages_are_detected_and_only_safe_xml_is_parsed(self) -> None:
        docx = _docx()
        self.assertEqual(sniff_rich_mime(docx), DOCX_MIME)
        metadata = inspect_rich_metadata(docx, DOCX_MIME)
        self.assertEqual(metadata["document"]["paragraph_count"], 3)
        self.assertEqual(metadata["document"]["table_count"], 1)
        self.assertNotIn("word/document.xml", str(metadata))
        text, detail = extract_rich_text(docx, DOCX_MIME, max_chars=1_000)
        self.assertEqual(text, "alpha\nbeta\ncell")
        self.assertFalse(detail["truncated"])

        for payload in (
            _docx(external_relationship=True),
            _docx(macro=True),
            _docx(ole=True),
        ):
            with self.assertRaises(SightglassError) as caught:
                inspect_rich_metadata(payload, DOCX_MIME)
            self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_xlsx_table_preserves_formula_and_cached_value_provenance(self) -> None:
        payload = _xlsx()
        self.assertEqual(sniff_rich_mime(payload), XLSX_MIME)
        metadata = inspect_rich_metadata(payload, XLSX_MIME)
        self.assertEqual(metadata["workbook"]["sheets"], ["Budget"])

        table = extract_table(
            payload,
            XLSX_MIME,
            sheet="Budget",
            cell_range="b1:b1",
            max_chars=10_000,
            max_bytes=10_000,
        )
        self.assertEqual(table["sheet"], "Budget")
        self.assertEqual(table["cell_range"], "B1:B1")
        formula = table["rows"][0]["cells"][0]
        self.assertEqual(formula["formula"], "SUM(1,2)")
        self.assertEqual(formula["cached_value"], "3")
        self.assertEqual(formula["value_source"], "cached_formula_result")
        self.assertNotIn("calculated_value", formula)

        with self.assertRaises(SightglassError) as caught:
            extract_table(
                payload,
                XLSX_MIME,
                sheet="Budget",
                cell_range="A1:XFD1048576",
                max_chars=10_000,
                max_bytes=10_000,
            )
        self.assertEqual(caught.exception.code, ErrorCode.QUERY_INVALID)
        self.assertEqual(caught.exception.details["max_cell_range_cells"], 10_000)

    def test_pptx_slide_and_text_are_page_bounded(self) -> None:
        payload = _pptx()
        self.assertEqual(sniff_rich_mime(payload), PPTX_MIME)
        metadata = inspect_rich_metadata(payload, PPTX_MIME)
        self.assertEqual(metadata["presentation"]["slide_count"], 1)
        slide = extract_slide(payload, page=1, max_chars=1_000, max_bytes=1_000)
        self.assertEqual(slide["number"], 1)
        self.assertEqual(slide["text"], "Launch plan\nShip safely")
        self.assertNotIn("notes", slide)
        with self.assertRaises(SightglassError) as caught:
            extract_slide(payload, page=2, max_chars=1_000, max_bytes=1_000)
        self.assertEqual(caught.exception.code, ErrorCode.QUERY_INVALID)

    def test_pptx_notes_slide_text_is_separate_from_slide_content(self) -> None:
        payload = _pptx(notes=True)
        slide = extract_slide(payload, page=1, max_chars=1_000, max_bytes=1_000)
        self.assertEqual(slide["text"], "Launch plan\nShip safely")
        self.assertEqual(
            slide["notes"],
            "Remind the audience to rehearse\nKeep the demo short",
        )
        self.assertNotIn("Remind the audience", slide["text"])

        text, detail = extract_rich_text(payload, PPTX_MIME, max_chars=10_000)
        self.assertIn("Slide 1\nLaunch plan\nShip safely", text)
        self.assertIn("Notes\nRemind the audience to rehearse", text)
        self.assertEqual(detail["kind"], "extracted_pptx_text")
        self.assertFalse(detail["truncated"])

        # Notes must never surface on a slide that has no notes relationship.
        plain = extract_slide(_pptx(), page=1, max_chars=1_000, max_bytes=1_000)
        self.assertNotIn("notes", plain)

    def test_pptx_notes_with_external_relationship_fail_closed(self) -> None:
        with self.assertRaises(SightglassError) as caught:
            extract_slide(
                _pptx(external_notes=True),
                page=1,
                max_chars=1_000,
                max_bytes=1_000,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_json_xml_html_csv_and_tsv_are_structured_without_execution(self) -> None:
        values = (
            (b'{"a": [1, 2]}', "application/json", "json"),
            (b"<root><value>alpha</value></root>", "application/xml", "xml"),
            (b"<html><script>never()</script><body>hello</body></html>", "text/html", "html"),
            (b"name,count\nalpha,2\n", "text/csv", "table"),
            (b"name\tcount\nalpha\t2\n", "text/tab-separated-values", "table"),
        )
        for payload, mime_type, expected_kind in values:
            with self.subTest(mime_type=mime_type):
                metadata = inspect_rich_metadata(payload, mime_type)
                self.assertEqual(metadata["kind"], expected_kind)
                text, _detail = extract_rich_text(payload, mime_type, max_chars=1_000)
                if mime_type == "text/html":
                    self.assertEqual(text, "hello")
                    self.assertNotIn("never", text)
                else:
                    self.assertTrue(text)

        csv_table = extract_table(
            values[3][0],
            "text/csv",
            sheet=None,
            cell_range=None,
            max_chars=1_000,
            max_bytes=1_000,
        )
        self.assertEqual(csv_table["rows"][1]["cells"], ["alpha", "2"])

        with self.assertRaises(SightglassError) as caught:
            inspect_rich_metadata(b"<!DOCTYPE r [<!ENTITY x 'boom'>]><r>&x;</r>", "application/xml")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_structured_output_truncates_to_caller_budget(self) -> None:
        payload = b"name,value\n" + b"\n".join(
            f"row-{number},{'x' * 40}".encode() for number in range(20)
        )
        table = extract_table(
            payload,
            "text/csv",
            sheet=None,
            cell_range=None,
            max_chars=240,
            max_bytes=240,
        )
        self.assertTrue(table["truncated"])
        self.assertLessEqual(len(str(table)), 600)


if __name__ == "__main__":
    unittest.main()
