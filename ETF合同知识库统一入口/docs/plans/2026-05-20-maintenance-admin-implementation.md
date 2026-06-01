# Maintenance Admin Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a unified maintenance admin inside the launcher so templates, variables, organization data, validation, publishing, and rollback can be managed from the Web UI without editing source code.

**Architecture:** The launcher becomes the management surface and owns `config/`, `managed_assets/`, and `backups/maintenance-admin/`. The ETF and linked child systems read published JSON configuration first, then fall back to their existing local files for compatibility. The release build reads `template_manifest.json` instead of filename heuristics.

**Tech Stack:** Python 3, Flask, Jinja templates, standard-library JSON/file operations, python-docx for DOCX validation, PowerShell build script updates, unittest.

---

### Task 1: Create A Safety Backup Command

**Files:**
- Create: `scripts/backup_maintenance_project.py`
- Modify: `README.md`
- Test: `tests/test_backup_maintenance_project.py`

**Step 1: Write the failing test**

Create tests that build a temporary workspace containing launcher, ETF, and linked folders, then run the backup function and assert that selected source/config/template files are copied into a timestamped backup folder while ignored generated folders are skipped.

Run:

```powershell
python -m unittest tests.test_backup_maintenance_project
```

Expected: fail because the script does not exist.

**Step 2: Implement the backup script**

Create a script that accepts `--workspace-root` and `--backup-root`, verifies all resolved paths stay under the intended workspace, and copies only maintainable files.

**Step 3: Verify**

Run:

```powershell
python -m unittest tests.test_backup_maintenance_project
python scripts/backup_maintenance_project.py --workspace-root D:\codex --backup-root D:\codex\ETF合同知识库统一入口\backups\maintenance-admin
```

Expected: tests pass and a backup folder is created.

### Task 2: Add Configuration Models And Seed Files

**Files:**
- Create: `config/template_manifest.json`
- Create: `config/variable_registry.json`
- Create: `config/organization_master_data.json`
- Create: `config/publish_state.json`
- Create: `maintenance_config.py`
- Test: `tests/test_maintenance_config.py`

**Step 1: Write tests**

Cover loading defaults, atomic JSON writes, validation of required top-level keys, and migration from missing files to seed files.

Run:

```powershell
python -m unittest tests.test_maintenance_config
```

Expected: fail because `maintenance_config.py` does not exist.

**Step 2: Implement config helpers**

Add helper functions for reading, writing, validating, and backing up JSON configs. Use only standard-library JSON and pathlib.

**Step 3: Seed data**

Seed the manifest from current ETF and linked template filenames. Seed variables from the existing `02_变量定义表.json` and `03_变量定义表.json` where practical, and explicitly include known hard-coded institution fields.

**Step 4: Verify**

Run:

```powershell
python -m unittest tests.test_maintenance_config
python -m py_compile maintenance_config.py
```

Expected: pass.

### Task 3: Build Maintenance Admin Routes

**Files:**
- Modify: `app.py`
- Create: `templates/admin.html`
- Test: `tests/test_admin_routes.py`

**Step 1: Write route tests**

Cover:

- `GET /admin` returns 200.
- `GET /api/admin/templates` returns manifest data.
- `GET /api/admin/variables` returns registry data.
- `GET /api/admin/organizations` returns master data.
- `POST` update endpoints create backups and reject invalid JSON.

Run:

```powershell
python -m unittest tests.test_admin_routes
```

Expected: fail because routes do not exist.

**Step 2: Implement API routes**

Add launcher-level routes only. Do not modify child generation behavior in this task.

**Step 3: Implement UI shell**

Create a workbench UI with left navigation, top system filter, tables, detail drawer, and validation panel.

**Step 4: Verify**

Run:

```powershell
python -m unittest tests.test_admin_routes
python -m py_compile app.py
```

Expected: pass.

### Task 4: Add Template Asset Upload And Versioning

**Files:**
- Modify: `app.py`
- Modify: `templates/admin.html`
- Modify: `maintenance_config.py`
- Test: `tests/test_template_assets.py`

**Step 1: Write tests**

Cover upload of a DOCX/text template into `managed_assets/templates/<system>/`, manifest version creation, backup creation, and rollback to a prior version.

Run:

```powershell
python -m unittest tests.test_template_assets
```

Expected: fail.

**Step 2: Implement upload handling**

Accept only configured suffixes: `.md`, `.json`, `.docx`. Store versions with stable generated filenames and preserve original filenames in metadata.

**Step 3: Implement rollback**

Rollback changes manifest state only, without deleting historical files.

**Step 4: Verify**

Run:

```powershell
python -m unittest tests.test_template_assets
```

Expected: pass.

### Task 5: Add Validation And Publish Flow

**Files:**
- Create: `maintenance_validation.py`
- Modify: `app.py`
- Modify: `templates/admin.html`
- Test: `tests/test_maintenance_validation.py`

**Step 1: Write tests**

Cover missing files, unreadable DOCX, unregistered variables, missing required variable sources, and successful publish state updates.

Run:

```powershell
python -m unittest tests.test_maintenance_validation
```

Expected: fail.

**Step 2: Implement validation**

Use `python-docx` only for DOCX open checks. Use regex scanning for `{{VARIABLE}}` placeholders in text templates.

**Step 3: Implement publish endpoint**

Publish only if validation has no blocking errors. Write `publish_state.json` with timestamp and validation summary.

**Step 4: Verify**

Run:

```powershell
python -m unittest tests.test_maintenance_validation
```

Expected: pass.

### Task 6: Make The Build Script Read The Manifest

**Files:**
- Modify: `build_release.ps1`
- Test: `tests/test_build_manifest.py`

**Step 1: Write tests**

Test helper behavior by invoking a Python parser/helper for manifest layout, and use PowerShell parser validation for syntax.

Run:

```powershell
python -m unittest tests.test_build_manifest
```

Expected: fail until manifest helper exists.

**Step 2: Implement manifest-driven copy**

Replace filename `Contains(...)` template selection with explicit paths from `template_manifest.json`, while keeping a compatibility fallback for legacy directories.

**Step 3: Verify**

Run:

```powershell
python -m unittest tests.test_build_manifest
$tokens = $null; $errors = $null; [System.Management.Automation.Language.Parser]::ParseFile((Join-Path (Get-Location) 'build_release.ps1'), [ref]$tokens, [ref]$errors); $errors.Count
```

Expected: tests pass and parser reports zero errors.

### Task 7: Teach Child Systems To Read Published Config

**Files:**
- Modify: `D:\codex\ETF合同知识库\app.py`
- Modify: `D:\codex\ETF联接基金合同知识库\app.py`
- Test: existing child-system unittest suites plus focused config tests.

**Step 1: Write compatibility tests**

For each child system, test that a published config path overrides hard-coded template names, and missing config falls back to existing behavior.

**Step 2: Implement read-only config loading**

Pass `CONTRACT_KB_CONFIG_ROOT` from the launcher to child services. Child systems read `template_manifest.json`, `variable_registry.json`, and `organization_master_data.json` without writing them.

**Step 3: Verify**

Run:

```powershell
python -m unittest discover -s tests
```

Expected: launcher tests pass. Then run relevant child-system tests from each child project.

### Task 8: End-To-End Smoke Test

**Files:**
- No new files unless failures require fixes.

**Step 1: Run launcher tests**

```powershell
python -m unittest discover -s tests
```

Expected: all launcher tests pass.

**Step 2: Run child-system minimal tests**

Run the reasonable minimal test command in each child project, based on existing test files.

**Step 3: Start the launcher**

```powershell
python app.py --no-browser
```

Expected:

- `/admin` loads.
- `/api/admin/templates` returns manifest.
- `/api/status` points to `D:\codex` child systems.

**Step 4: UI check**

Open the launcher in a browser and inspect desktop and mobile layouts for the admin workbench. Confirm no overlapping controls, readable tables, and safe confirm dialogs.

### Task 9: Documentation

**Files:**
- Modify: `README.md`
- Create or modify: `docs/maintenance-admin.md`

**Step 1: Document user workflow**

Explain template upload, variable addition, organization data update, validation, publish, and rollback.

**Step 2: Document operator workflow**

Explain backup, build, release, and how to recover from a bad publish.

**Step 3: Verify**

Run:

```powershell
python -m unittest discover -s tests
```

Expected: pass.

## Execution Notes

- Before Task 1 implementation touches source files, create a backup using the new script as soon as it exists. For the very first implementation edit, create a manual timestamped copy of current maintainable files.
- Do not remove legacy paths until the manifest-driven path has passed end-to-end tests.
- Do not introduce SQLite in phase one.
- Do not weaken existing child-system tests to make this pass.
