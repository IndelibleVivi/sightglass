"""Regression checks for hosted gate trigger, ownership and tooling semantics."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from scripts.ci_changes import changed_paths, docs_only

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "synthetic.yml"


class CIWorkflowContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_required_events_have_no_trigger_level_path_filter(self) -> None:
        triggers = self.workflow.split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
        self.assertIn("  push: {}", triggers.splitlines())
        self.assertIn("  pull_request: {}", triggers.splitlines())
        self.assertIn("  workflow_dispatch:", triggers.splitlines())
        self.assertNotIn("paths:", triggers)
        self.assertNotIn("schedule:", triggers)

    def test_main_runs_are_not_cancelled_but_prs_are_supersedable(self) -> None:
        self.assertIn("github.event.pull_request.number", self.workflow)
        self.assertIn("github.run_id", self.workflow)
        self.assertRegex(
            self.workflow,
            re.escape("synthetic-${{ github.workflow }}-${{\n")
            + r"[\s\S]+github\.event\.pull_request\.number[\s\S]+github\.run_id",
        )
        self.assertIn(
            "cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
            self.workflow,
        )

    def test_expensive_jobs_share_completed_docs_checks(self) -> None:
        self.assertIn("docs_only: ${{ steps.changes.outputs.docs_only }}", self.workflow)
        self.assertEqual(
            self.workflow.count("needs.classify.outputs.docs_only != 'true'"),
            2,
        )
        self.assertIn("python scripts/ci_changes.py", self.workflow)

    def test_uv_cache_is_owned_by_setup_uv(self) -> None:
        self.assertEqual(
            self.workflow.count("uses: astral-sh/setup-uv@v7"),
            3,
        )
        self.assertNotIn("pip install uv", self.workflow)
        self.assertNotIn("actions/cache", self.workflow)

    def test_macos_keeps_full_tools_without_sqlcipher_source_build(self) -> None:
        self.assertIn("brew install poppler", self.workflow)
        self.assertNotIn("brew install sqlcipher", self.workflow)
        self.assertNotIn("CPPFLAGS=", self.workflow)
        self.assertIn(
            "uv run python -m unittest discover -s tests -t . -p 'test_*.py'",
            self.workflow,
        )
        self.assertIn("uv run pyright", self.workflow)
        self.assertEqual(
            self.workflow.count("python scripts/check-installed-wheel.py"),
            3,
        )


class DocumentationSelectionTests(unittest.TestCase):
    def test_prose_uses_the_packaging_gate(self) -> None:
        for paths in (["README.md"], ["docs/OPERATIONS.md", "NOTICE.md"]):
            self.assertTrue(docs_only("pull_request", paths))
        self.assertIn("uv build", WORKFLOW.read_text())

    def test_config_code_assets_and_benchmarks_require_product_suites(self) -> None:
        for path in (
            "pyproject.toml", "uv.lock", ".github/workflows/synthetic.yml",
            "src/sightglass/cli.py", "scripts/ci_changes.py", "tests/unit/test_ci_workflow.py",
            "docs/benchmarks/native-connections.json", "docs/assets/architecture.mmd",
            "docs/assets/visual-tokens.json", "docs/assets/architecture.svg",
        ):
            with self.subTest(path=path):
                self.assertFalse(docs_only("pull_request", ["README.md", path]))

    def test_unknown_base_empty_diff_and_manual_runs_remain_full(self) -> None:
        self.assertIsNone(changed_paths("0" * 40, "HEAD"))
        self.assertIsNone(changed_paths("missing-ci-base", "HEAD"))
        self.assertFalse(docs_only("push", None))
        self.assertFalse(docs_only("pull_request", []))
        self.assertFalse(docs_only("workflow_dispatch", ["README.md"]))
        self.assertFalse(docs_only("schedule", ["README.md"]))


if __name__ == "__main__":
    unittest.main()
