from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path


PROJECT_NAMES = (
    "ETF合同知识库统一入口",
    "ETF合同知识库",
    "ETF联接基金合同知识库",
)

EXCLUDED_DIRECTORY_NAMES = {
    ".git",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "backups",
    "build",
    "dist",
    "logs",
    "node_modules",
    "output",
    "outputs",
    "qa_artifacts",
    "tmp",
    "venv",
}

MAINTAINED_SUFFIXES = {
    ".bat",
    ".cmd",
    ".docx",
    ".html",
    ".json",
    ".md",
    ".ps1",
    ".py",
    ".spec",
    ".toml",
    ".txt",
    ".xlsx",
    ".yaml",
    ".yml",
}


def resolve_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def ensure_under(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} must stay inside workspace: {path}") from exc


def has_excluded_part(relative_path: Path) -> bool:
    return any(part in EXCLUDED_DIRECTORY_NAMES for part in relative_path.parts[:-1])


def should_copy_file(path: Path, source_root: Path) -> bool:
    relative_path = path.relative_to(source_root)
    if has_excluded_part(relative_path):
        return False
    return path.suffix.lower() in MAINTAINED_SUFFIXES


def copy_project_files(source_root: Path, destination_root: Path) -> int:
    copied_count = 0
    for path in source_root.rglob("*"):
        if not path.is_file() or not should_copy_file(path, source_root):
            continue
        relative_path = path.relative_to(source_root)
        destination = destination_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        copied_count += 1
    return copied_count


def backup_workspace(
    workspace_root: str | Path,
    backup_root: str | Path,
    timestamp: str | None = None,
) -> Path:
    workspace = resolve_path(workspace_root)
    backup_base = resolve_path(backup_root)
    ensure_under(backup_base, workspace, "backup_root")

    stamp = timestamp or datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = backup_base / stamp / "pre-implementation"
    backup_dir.mkdir(parents=True, exist_ok=False)

    for project_name in PROJECT_NAMES:
        source_root = workspace / project_name
        if not source_root.exists():
            continue
        source_root = source_root.resolve()
        ensure_under(source_root, workspace, project_name)
        copy_project_files(source_root, backup_dir / project_name)

    return backup_dir


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Back up maintainable contract knowledge-base project files.")
    parser.add_argument("--workspace-root", required=True, type=Path)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--timestamp")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    backup_dir = backup_workspace(args.workspace_root, args.backup_root, args.timestamp)
    print(f"Backup created: {backup_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
