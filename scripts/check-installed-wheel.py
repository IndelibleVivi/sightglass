#!/usr/bin/env python3
"""Validate one built wheel in a disposable, frozen runtime-only environment.

Run after ``uv build``: ``uv run python scripts/check-installed-wheel.py``.
An explicit ``--wheel`` accepts an artifact outside dist/. The script compares
its source/license payload with this checkout, exports locked runtime extras,
installs without the project/dev tools, and runs isolated installed probes from
an empty cwd. It never loads an operator config or a real source. A JSON receipt
identifies the exact artifact and observed interpreter/platform; it is not an
installation, activation, native-account or real-recognizer acceptance receipt.
"""
from __future__ import annotations

import argparse
import configparser
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

ENTRYPOINTS = {"sightglassctl", "sightglassd", "sightglass-mcp", "sightglass-pair"}
RUNTIME_EXTRAS = ("macos-wechat", "voice")


def clean_environment() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items()
           if key not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"}
           and not key.startswith("SIGHTGLASS_")}
    env.update(PYTHONNOUSERSITE="1", PYTHONSAFEPATH="1")
    return env


def validate_wheel(wheel: Path, repository: Path) -> dict:
    """Reject missing, stale, unexpected or privately bundled package payloads."""
    project = tomllib.loads((repository / "pyproject.toml").read_text())["project"]
    scripts = project["scripts"]
    if set(scripts) != ENTRYPOINTS:
        raise RuntimeError("the four supported console entrypoints changed")
    source = repository / "src"
    expected = {path.relative_to(source).as_posix(): path.read_bytes()
                for path in (source / "sightglass").rglob("*.py")}
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise RuntimeError("wheel contains duplicate archive members")
        package_names = {name for name in names if name.startswith("sightglass/")}
        if package_names != set(expected):
            raise RuntimeError("wheel source inventory differs from checkout")
        if any(archive.read(name) != body for name, body in expected.items()):
            raise RuntimeError("wheel source bytes differ from checkout")
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise RuntimeError("wheel must contain exactly one distribution")
        info = metadata_names[0].removesuffix("METADATA")
        if any(not name.startswith(("sightglass/", info)) for name in names):
            raise RuntimeError("wheel contains unexpected payload outside its distribution")
        metadata = BytesParser().parsebytes(archive.read(info + "METADATA"))
        if (metadata["Name"] != project["name"] or metadata["Version"] != project["version"]
                or metadata["License-Expression"] != project["license"]):
            raise RuntimeError("wheel metadata differs from project contract")
        licenses = project["license-files"]
        if set(metadata.get_all("License-File", [])) != set(licenses):
            raise RuntimeError("wheel license declarations differ from project contract")
        allowed_metadata = {info + name for name in
                            ("METADATA", "WHEEL", "RECORD", "entry_points.txt", "top_level.txt")}
        allowed_metadata.update(info + "licenses/" + relative for relative in licenses)
        if set(names) - package_names - allowed_metadata:
            raise RuntimeError("wheel contains undeclared distribution payload")
        for relative in licenses:
            if archive.read(info + "licenses/" + relative) != (repository / relative).read_bytes():
                raise RuntimeError("wheel license/provenance bytes differ from checkout")
        declared = configparser.ConfigParser()
        declared.read_string(archive.read(info + "entry_points.txt").decode())
        if dict(declared["console_scripts"]) != scripts:
            raise RuntimeError("wheel console entrypoints differ from project contract")
    return {"name": project["name"], "version": project["version"],
            "wheel": wheel.name, "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            "package_source_files": len(expected), "license_files": licenses,
            "entrypoints": scripts}


def installed_probe(root: Path) -> None:
    """Runs only in the fresh venv, using a copied script and Python isolated mode."""
    import anyio
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    purelib = Path(sysconfig.get_path("purelib")).resolve()
    if not purelib.is_relative_to(Path(sys.prefix).resolve()) or sys.prefix == sys.base_prefix:
        raise RuntimeError("probe requires its own isolated installed environment")
    if list(Path.cwd().iterdir()):
        raise RuntimeError("installed probe cwd is not empty")
    if "PYTHONPATH" in os.environ or "PYTHONHOME" in os.environ:
        raise RuntimeError("installed probe inherited a Python path override")
    distribution = importlib.metadata.distribution("sightglass")
    direct = json.loads(distribution.read_text("direct_url.json") or "{}")
    if direct.get("dir_info", {}).get("editable") or "archive_info" not in direct:
        raise RuntimeError("installed project must originate from a non-editable wheel")
    for name in ("ruff", "pyright"):
        try:
            importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError:
            pass
        else:
            raise RuntimeError("runtime-only environment contains dev tools")
    origins = {}
    for ep in distribution.entry_points:
        if ep.group == "console_scripts":
            function = ep.load()
            module = sys.modules[function.__module__]
            if module.__file__ is None:
                raise RuntimeError("entrypoint module has no installed file")
            location = Path(module.__file__).resolve()
            if not location.is_relative_to(purelib / "sightglass"):
                raise RuntimeError("entrypoint import escaped installed site-packages")
            origins[ep.name] = location.relative_to(purelib).as_posix()
    if set(origins) != ENTRYPOINTS:
        raise RuntimeError("installed entrypoints are incomplete")
    bin_dir = Path(sys.executable).parent
    env = clean_environment()
    # These help paths parse the real installed wrappers without loading config.
    for name in sorted(ENTRYPOINTS - {"sightglass-mcp"}):
        result = subprocess.run([str(bin_dir / name), "--help"], env=env,
                                capture_output=True, text=True, timeout=20, check=True)
        if "usage:" not in result.stdout:
            raise RuntimeError("installed entrypoint help did not execute")

    async def mcp_probe():
        # Initialize the real stdio executable and list tools; no tool calls or IPC.
        parameters = StdioServerParameters(command=str(bin_dir / "sightglass-mcp"),
                                           env=env, cwd=str(Path.cwd()))
        with anyio.fail_after(30):
            async with stdio_client(parameters) as streams:
                async with ClientSession(*streams) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    names = {tool.name for tool in tools.tools}
                    from sightglass.mcp.tools import ReaderTools
                    expected = {name for name in vars(ReaderTools) if name.startswith("wechat_")}
                    if names != expected:
                        raise RuntimeError("installed MCP tool catalog is incomplete")
                    return len(names)

    tool_count = anyio.run(mcp_probe)
    # Copy the canonical generated example beside this probe; isolated mode keeps
    # its directory off sys.path, so its imports can only use the installed wheel.
    smoke = subprocess.run([sys.executable, "-I", str(root / "synthetic_read.py")],
                           env=env, capture_output=True, text=True, timeout=60, check=True)
    observed = json.loads(smoke.stdout)
    if observed["returned_messages"] != 3 or observed["source_page_complete"] is not True:
        raise RuntimeError("installed generated read smoke failed")
    if (observed["inbox_schema"] != "sightglass.inbox-page.v2"
            or observed["recent_conversations"] != 2
            or observed["contacts_without_cached_bodies"] != 2):
        raise RuntimeError("installed generated daily recents smoke failed")
    for name, module in tuple(sys.modules.items()):
        if name == "sightglass" or name.startswith("sightglass."):
            if module.__file__ is None or not Path(module.__file__).resolve().is_relative_to(
                    purelib / "sightglass"):
                raise RuntimeError("package import escaped installed site-packages")
    print(json.dumps({"python": sys.version.split()[0], "platform": sys.platform,
                      "imports": origins, "editable": False, "dev_tools_installed": False,
                      "mcp_tools": tool_count, "synthetic_read": observed}, sort_keys=True))


def check(wheel: Path, repository: Path, python: str) -> dict:
    identity = validate_wheel(wheel, repository)
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required for frozen runtime installation")
    env = clean_environment()
    with tempfile.TemporaryDirectory(prefix="sightglass-installed-wheel-") as directory:
        root = Path(directory)
        empty = root / "empty"
        empty.mkdir()
        requirements = root / "requirements.txt"
        subprocess.run([uv, "export", "--frozen", "--no-dev", "--no-default-groups",
                        "--extra", RUNTIME_EXTRAS[0], "--extra", RUNTIME_EXTRAS[1],
                        "--no-emit-project", "--output-file", str(requirements)],
                       cwd=repository, env=env, capture_output=True, text=True, check=True)
        runtime = root / "runtime"
        subprocess.run([uv, "venv", "--python", python, str(runtime)], cwd=empty,
                       env=env, capture_output=True, text=True, check=True)
        interpreter = runtime / "bin" / "python"
        subprocess.run([uv, "pip", "sync", "--python", str(interpreter),
                        "--require-hashes", str(requirements)], cwd=empty, env=env,
                       capture_output=True, text=True, check=True)
        subprocess.run([uv, "pip", "install", "--python", str(interpreter), "--no-deps",
                        str(wheel)], cwd=empty, env=env, capture_output=True, text=True, check=True)
        shutil.copyfile(__file__, root / "probe.py")
        shutil.copyfile(repository / "examples" / "synthetic_read.py", root / "synthetic_read.py")
        result = subprocess.run([str(interpreter), "-I", str(root / "probe.py"), "--probe",
                                 str(root)], cwd=empty, env=env, capture_output=True,
                                text=True, timeout=120, check=True)
        return {**identity, "runtime_extras": list(RUNTIME_EXTRAS),
                "frozen_requirements_sha256": hashlib.sha256(requirements.read_bytes()).hexdigest(),
                "installed": json.loads(result.stdout)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--probe", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.probe is not None:
        installed_probe(args.probe)
        return
    repository = Path(__file__).resolve().parent.parent
    wheels = [args.wheel] if args.wheel else list((repository / "dist").glob("*.whl"))
    if len(wheels) != 1:
        parser.error("select exactly one built wheel with --wheel (or a clean dist/)")
    try:
        print(json.dumps(check(wheels[0].resolve(), repository, args.python), indent=2))
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"installed-wheel command failed: {error.cmd}\n"
                           f"{error.stdout}\n{error.stderr}") from error


if __name__ == "__main__":
    main()
