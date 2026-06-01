# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules


project_root = Path(SPECPATH)


def safe_collect_submodules(package_name: str) -> list[str]:
    try:
        return collect_submodules(package_name)
    except Exception:
        return []


def safe_collect_data_files(package_name: str) -> list[tuple[str, str]]:
    try:
        return collect_data_files(package_name)
    except Exception:
        return []


hiddenimports: list[str] = []
for package_name in (
    "flask",
    "werkzeug",
    "jinja2",
    "docx",
    "openpyxl",
    "lxml",
    "openai",
    "pydantic",
    "httpx",
    "anyio",
    "sniffio",
    "tqdm",
    "win32com",
):
    hiddenimports += safe_collect_submodules(package_name)
hiddenimports += ["pythoncom", "pywintypes", "win32com.client"]

datas: list[tuple[str, str]] = []
for package_name in ("docx", "openpyxl", "certifi"):
    datas += safe_collect_data_files(package_name)


a = Analysis(
    [str(project_root / "app.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["pytest", "playwright", "IPython", "matplotlib", "numpy", "pandas"],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="合同知识库控制台",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="合同知识库控制台",
)
