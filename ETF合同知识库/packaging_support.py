import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


PACKAGED_ASSETS_RELATIVE_DIR = Path("packaged_assets")
PACKAGED_REFERENCE_PROSPECTUS_DIR = PACKAGED_ASSETS_RELATIVE_DIR / "reference_prospectus"
PACKAGED_PRODUCT_SUMMARY_DIR = PACKAGED_ASSETS_RELATIVE_DIR / "product_summary"
PACKAGED_REVIEW_RULES_DIR = PACKAGED_ASSETS_RELATIVE_DIR / "review_rules"
PACKAGED_REVIEW_WORKBOOKS_DIR = PACKAGED_ASSETS_RELATIVE_DIR / "review_workbooks"


REFERENCE_PROSPECTUS_VARIANT_FILENAMES = {
    "SSE_CROSS": "SSE_CROSS.docx",
    "SSE_SINGLE": "SSE_SINGLE.docx",
    "SSE_HK": "SSE_HK.docx",
    "SZSE_CROSS": "SZSE_CROSS.docx",
    "SZSE_SINGLE": "SZSE_SINGLE.docx",
    "SZSE_HK": "SZSE_HK.docx",
}

LEGACY_REFERENCE_PROSPECTUS_SOURCE_MAP = {
    key: (PACKAGED_REFERENCE_PROSPECTUS_DIR / filename,)
    for key, filename in REFERENCE_PROSPECTUS_VARIANT_FILENAMES.items()
}

PRODUCT_SUMMARY_TEMPLATE_FILENAME = (
    "\u5357\u65b9\u56fd\u8bc1\u77f3\u6cb9\u5929\u7136\u6c14\u4ea4\u6613\u578b\u5f00\u653e\u5f0f\u6307\u6570"
    "\u8bc1\u5238\u6295\u8d44\u57fa\u91d1\u57fa\u91d1\u4ea7\u54c1\u8d44\u6599\u6982\u8981.docx"
)
LEGACY_PRODUCT_SUMMARY_TEMPLATE_CANDIDATES = (
    PACKAGED_PRODUCT_SUMMARY_DIR / PRODUCT_SUMMARY_TEMPLATE_FILENAME,
)

RULES_XLSX_FILENAME = "\u57fa\u91d1\u5408\u540c\u4e0e\u62db\u52df\u8bf4\u660e\u4e66\u89c4\u5219.xlsx"
LEGACY_RULES_XLSX_CANDIDATES = (
    PACKAGED_REVIEW_RULES_DIR / RULES_XLSX_FILENAME,
)

REVIEW_WORKBOOK_FILENAMES = (
    "\u5357\u65b9\u4e2d\u8bc1\u5168\u6307\u7ea2\u5229\u8d28\u91cfETF_\u52fe\u7a3d\u5173\u7cfb\u6574\u7406.xlsx",
    "\u5357\u65b9\u4e2d\u8bc1\u901a\u7528\u822a\u7a7a\u4e3b\u9898ETF\u53d1\u8d77\u5f0f\u8054\u63a5\u57fa\u91d1_\u52fe\u7a3d\u5173\u7cfb\u6574\u7406.xlsx",
)
LEGACY_REVIEW_XLSX_CANDIDATES = tuple(
    PACKAGED_REVIEW_WORKBOOKS_DIR / name for name in REVIEW_WORKBOOK_FILENAMES
)


@dataclass(frozen=True)
class AssetSpec:
    key: str
    destination: Path
    sources: tuple[Path, ...]


class MissingPackagedAssetError(RuntimeError):
    def __init__(self, missing: list[dict]):
        self.missing = missing
        labels = ", ".join(item["key"] for item in missing)
        super().__init__(f"Missing packaged assets: {labels}")


def compute_app_root(module_file: str, frozen: bool | None = None, executable: str | None = None) -> Path:
    if frozen is None:
        frozen = bool(getattr(sys, "frozen", False))
    if frozen:
        executable_path = Path(executable or sys.executable).resolve()
        return executable_path.parent
    return Path(module_file).resolve().parent


def build_default_asset_specs() -> tuple[AssetSpec, ...]:
    specs: list[AssetSpec] = []
    for variant_key, sources in LEGACY_REFERENCE_PROSPECTUS_SOURCE_MAP.items():
        specs.append(
            AssetSpec(
                key=f"reference_prospectus:{variant_key}",
                destination=PACKAGED_REFERENCE_PROSPECTUS_DIR / REFERENCE_PROSPECTUS_VARIANT_FILENAMES[variant_key],
                sources=tuple(sources),
            )
        )

    specs.append(
        AssetSpec(
            key="product_summary_template",
            destination=PACKAGED_PRODUCT_SUMMARY_DIR / PRODUCT_SUMMARY_TEMPLATE_FILENAME,
            sources=tuple(LEGACY_PRODUCT_SUMMARY_TEMPLATE_CANDIDATES),
        )
    )
    specs.append(
        AssetSpec(
            key="review_rules_xlsx",
            destination=PACKAGED_REVIEW_RULES_DIR / RULES_XLSX_FILENAME,
            sources=tuple(LEGACY_RULES_XLSX_CANDIDATES),
        )
    )

    for filename, source in zip(REVIEW_WORKBOOK_FILENAMES, LEGACY_REVIEW_XLSX_CANDIDATES):
        specs.append(
            AssetSpec(
                key=f"review_workbook:{filename}",
                destination=PACKAGED_REVIEW_WORKBOOKS_DIR / filename,
                sources=(source,),
            )
        )

    return tuple(specs)


def _first_existing_source(sources: tuple[Path, ...]) -> Path | None:
    for source in sources:
        if source.exists():
            return source
    return None


def prepare_packaged_assets(
    project_root: Path,
    specs: list[AssetSpec] | tuple[AssetSpec, ...] | None = None,
    clean: bool = False,
) -> list[dict]:
    project_root = Path(project_root).resolve()
    packaged_assets_root = project_root / PACKAGED_ASSETS_RELATIVE_DIR
    if clean and packaged_assets_root.exists():
        shutil.rmtree(packaged_assets_root)

    resolved_specs = tuple(specs or build_default_asset_specs())
    missing: list[dict] = []
    copied: list[dict] = []

    for spec in resolved_specs:
        source = _first_existing_source(spec.sources)
        if source is None:
            missing.append(
                {
                    "key": spec.key,
                    "destination": str(spec.destination),
                    "sources": [str(path) for path in spec.sources],
                }
            )
            continue

        target = project_root / spec.destination
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied.append(
            {
                "key": spec.key,
                "source": str(source),
                "destination": str(target),
            }
        )

    if missing:
        raise MissingPackagedAssetError(missing)

    return copied


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare packaged assets for the ETF EXE build.")
    parser.add_argument("--project-root", default=".", help="Project root that will receive packaged_assets/")
    parser.add_argument("--clean", action="store_true", help="Remove the existing packaged_assets directory first.")
    args = parser.parse_args(argv)

    try:
        copied = prepare_packaged_assets(Path(args.project_root), clean=args.clean)
    except MissingPackagedAssetError as exc:
        print("Missing packaged assets:")
        for item in exc.missing:
            print(f"- {item['key']}:")
            for source in item["sources"]:
                print(f"    {source}")
        return 1

    print(f"Prepared {len(copied)} packaged assets.")
    for item in copied:
        print(f"- {item['key']}: {item['source']} -> {item['destination']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
