# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules


project_root = Path(SPECPATH)


def collect_project_data() -> list[tuple[str, str]]:
    datas: list[tuple[str, str]] = []
    for pattern in ("*.md", "*.json"):
        for path in project_root.glob(pattern):
            if path.is_file():
                datas.append((str(path), "."))

    for dirname in (
        "templates",
        "manual_regression_20260310_201555",
        "prospectus_materials",
        "packaged_assets",
    ):
        directory = project_root / dirname
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if path.is_file():
                datas.append((str(path), str(path.parent.relative_to(project_root))))
    return datas


datas = collect_project_data()
datas += collect_data_files("docx")
datas += collect_data_files("openpyxl")

hiddenimports = collect_submodules("docx")
hiddenimports += collect_submodules("openpyxl")


a = Analysis(
    [str(project_root / "app.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="ETF合同知识库",
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
    name="ETF合同知识库",
)
