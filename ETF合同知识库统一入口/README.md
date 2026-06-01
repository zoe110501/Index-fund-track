# 合同知识库控制台

本项目是 ETF / ETF 联接两套合同知识库的统一入口。它只负责启动、分流和打包分发；业务模板、条款库、生成逻辑仍在两个子系统目录中维护。

## 本地运行

```powershell
python -m pip install -r requirements.txt
python app.py
```

也可以运行：

```powershell
.\start.bat
```

启动器优先使用 `http://127.0.0.1:5000`。如果端口被占用，会自动选择后续空余端口。点击入口后，ETF 和 ETF 联接子系统也会自动选择空余端口。

## 子系统来源

启动器按下面顺序查找子系统：

1. 发布包内的 `systems\etf\` 和 `systems\linked\`。
2. `$env:CONTRACT_KB_SYSTEM_ROOT\ETF合同知识库` 和 `$env:CONTRACT_KB_SYSTEM_ROOT\ETF联接基金合同知识库`。
3. 如果没有设置 `CONTRACT_KB_SYSTEM_ROOT`，默认使用本目录的上一级目录。本工作区中即 `D:\codex`。
4. 用户桌面上的同名目录作为兼容兜底。

如果本地和桌面同时存在子系统，请以 `D:\codex` 工作区为维护源，避免两个副本漂移。

## 子系统模式

打包后的启动器会用自身的子进程模式启动两个系统：

```powershell
python app.py --serve etf --port 5001
python app.py --serve linked --port 5002
```

## 测试

```powershell
python -m unittest discover -s tests
```

## 维护前备份

涉及模板后台、变量配置或子系统读取逻辑的改动前，先创建一次可回退备份：

```powershell
python scripts\backup_maintenance_project.py --workspace-root D:\codex --backup-root D:\codex\ETF合同知识库统一入口\backups\maintenance-admin
```

## 构建分发包

构建前安装打包依赖：

```powershell
python -m pip install -r requirements-build.txt
```

然后运行：

```powershell
.\build_release.ps1
```

构建完成后会生成：

- `dist\合同知识库控制台\合同知识库控制台.exe`
- `dist\合同知识库控制台\systems\etf\`
- `dist\合同知识库控制台\systems\linked\`
- `dist\合同知识库控制台.zip`

分发给同事时发送 zip 包即可。同事解压后双击 exe 使用，不需要安装 Python。

## 版本控制约定

应纳入版本控制的内容包括：

- `app.py`
- `templates\`
- `build_release.ps1`
- `patch_release.py`
- `launcher.spec`
- `requirements*.txt`
- `tests\`
- `README.md`

不应纳入版本控制的内容包括：

- `build\`
- `dist\`
- `logs\`
- `diagnostics\`
- `__pycache__\`
