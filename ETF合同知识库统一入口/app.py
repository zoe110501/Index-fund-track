from __future__ import annotations

import argparse
import atexit
import ctypes
import importlib.util
import os
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from dataclasses import dataclass
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template, request, url_for

import maintenance_config


HOST = "127.0.0.1"
DEFAULT_LAUNCHER_PORT = 5000
PORT_SEARCH_LIMIT = 200


def compute_runtime_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = compute_runtime_root()
SOURCE_DIR = Path(__file__).resolve().parent
SYSTEM_SOURCE_ROOT = Path(
    os.environ.get("CONTRACT_KB_SYSTEM_ROOT", SOURCE_DIR.parent)
).expanduser()
SYSTEMS_DIR = BASE_DIR / "systems"
LOG_DIR = BASE_DIR / "logs"
ADMIN_CONFIG_BASE_DIR = BASE_DIR
ACTIVE_LAUNCHER_PORT: int | None = None
_CHILD_JOB_HANDLE: int | None = None


if os.name == "nt":
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    _KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _KERNEL32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    _KERNEL32.CreateJobObjectW.restype = wintypes.HANDLE
    _KERNEL32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _KERNEL32.SetInformationJobObject.restype = wintypes.BOOL
    _KERNEL32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _KERNEL32.OpenProcess.restype = wintypes.HANDLE
    _KERNEL32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _KERNEL32.AssignProcessToJobObject.restype = wintypes.BOOL
    _KERNEL32.CloseHandle.argtypes = [wintypes.HANDLE]
    _KERNEL32.CloseHandle.restype = wintypes.BOOL

    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    _PROCESS_SET_QUOTA = 0x0100
    _PROCESS_TERMINATE = 0x0001


@dataclass(frozen=True)
class ServiceConfig:
    key: str
    title: str
    subtitle: str
    description: str
    system_dir: str
    dev_paths: tuple[Path, ...]
    preferred_port: int


SERVICES = {
    "etf": ServiceConfig(
        key="etf",
        title="ETF",
        subtitle="ETF 基金合同知识库",
        description="适用于普通 ETF 基金合同、招募说明书、产品资料概要生成与复核。",
        system_dir="etf",
        dev_paths=(
            SYSTEM_SOURCE_ROOT / "ETF合同知识库",
            Path.home() / "Desktop" / "ETF合同知识库",
        ),
        preferred_port=5001,
    ),
    "linked": ServiceConfig(
        key="linked",
        title="ETF联接",
        subtitle="ETF 联接基金合同知识库",
        description="适用于 ETF 联接基金合同、招募说明书、产品资料概要生成与复核。",
        system_dir="linked",
        dev_paths=(
            SYSTEM_SOURCE_ROOT / "ETF联接基金合同知识库",
            Path.home() / "Desktop" / "ETF联接基金合同知识库",
        ),
        preferred_port=5002,
    ),
}


class RunningService:
    def __init__(
        self,
        config: ServiceConfig,
        process: subprocess.Popen,
        log_file,
        port: int,
        path: Path,
    ):
        self.config = config
        self.process = process
        self.log_file = log_file
        self.port = port
        self.path = path


app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
_running: dict[str, RunningService] = {}
_last_ports: dict[str, int] = {}

ADMIN_CONFIG_SECTIONS = {
    "templates": "template_manifest",
    "variables": "variable_registry",
    "organizations": "organization_master_data",
}


def resolve_system_path(config: ServiceConfig) -> Path:
    packaged_path = SYSTEMS_DIR / config.system_dir
    if (packaged_path / "app.py").exists():
        return packaged_path
    for dev_path in config.dev_paths:
        if (dev_path / "app.py").exists():
            return dev_path
    return packaged_path


def is_port_open(port: int, timeout: float = 0.25) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((HOST, port)) == 0


def can_bind_port(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((HOST, port))
        except OSError:
            return False
        return True


def find_available_port(preferred_port: int, reserved: set[int] | None = None) -> int:
    reserved = reserved or set()
    for port in range(preferred_port, preferred_port + PORT_SEARCH_LIMIT):
        if port in reserved:
            continue
        if can_bind_port(port):
            return port
    raise RuntimeError(
        f"No available port found from {preferred_port} to "
        f"{preferred_port + PORT_SEARCH_LIMIT - 1}."
    )


def wait_for_port(port: int, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_port_open(port):
            return True
        time.sleep(0.25)
    return is_port_open(port)


def current_reserved_ports() -> set[int]:
    reserved = {ACTIVE_LAUNCHER_PORT} if ACTIVE_LAUNCHER_PORT is not None else set()
    for running in _running.values():
        if running.process.poll() is None:
            reserved.add(running.port)
    return reserved


def service_port(config: ServiceConfig) -> int:
    running = _running.get(config.key)
    if running and running.process.poll() is None:
        return running.port
    return _last_ports.get(config.key, config.preferred_port)


def service_url(port: int) -> str:
    return f"http://{HOST}:{port}/"


def close_log_safely(running: RunningService) -> None:
    try:
        running.log_file.close()
    except Exception:
        pass


def ensure_child_job() -> int | None:
    global _CHILD_JOB_HANDLE
    if os.name != "nt":
        return None
    if _CHILD_JOB_HANDLE:
        return _CHILD_JOB_HANDLE

    job = _KERNEL32.CreateJobObjectW(None, None)
    if not job:
        return None

    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    ok = _KERNEL32.SetInformationJobObject(
        job,
        _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if not ok:
        _KERNEL32.CloseHandle(job)
        return None

    _CHILD_JOB_HANDLE = int(job)
    return _CHILD_JOB_HANDLE


def assign_process_to_child_job(process: subprocess.Popen) -> None:
    if os.name != "nt":
        return

    job = ensure_child_job()
    if not job:
        return

    handle = _KERNEL32.OpenProcess(
        _PROCESS_SET_QUOTA | _PROCESS_TERMINATE,
        False,
        process.pid,
    )
    if not handle:
        return

    try:
        _KERNEL32.AssignProcessToJobObject(job, handle)
    finally:
        _KERNEL32.CloseHandle(handle)


def service_process_args(key: str, port: int, system_path: Path) -> list[str]:
    base_args = [
        "--serve",
        key,
        "--port",
        str(port),
        "--system-path",
        str(system_path),
    ]
    if getattr(sys, "frozen", False):
        return [sys.executable, *base_args]
    return [sys.executable, str(SOURCE_DIR / "app.py"), *base_args]


def service_process_env(config: ServiceConfig, system_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    if config.key == "linked":
        template_dir = system_path / "packaged_assets" / "product_summary_templates"
        if template_dir.exists():
            env["PRODUCT_SUMMARY_TEMPLATE_DIR"] = str(template_dir)
        legal_template_dir = system_path / "packaged_assets" / "legal_templates"
        if legal_template_dir.exists():
            env["CONTRACT_TEMPLATE_DIR"] = str(legal_template_dir)
    return env


def start_service(config: ServiceConfig) -> RunningService | None:
    existing = _running.get(config.key)
    if existing and existing.process.poll() is None:
        return existing
    if existing:
        close_log_safely(existing)
        _running.pop(config.key, None)

    system_path = resolve_system_path(config)
    if not (system_path / "app.py").exists():
        return None

    port = find_available_port(config.preferred_port, current_reserved_ports())
    _last_ports[config.key] = port

    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"{config.key}.log"
    log_file = log_path.open("a", encoding="utf-8", buffering=1)
    log_file.write(
        f"\n\n===== launcher start {time.strftime('%Y-%m-%d %H:%M:%S')} "
        f"port={port} path={system_path} =====\n"
    )

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(
        service_process_args(config.key, port, system_path),
        cwd=str(system_path),
        env=service_process_env(config, system_path),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
        creationflags=creationflags,
    )
    assign_process_to_child_job(process)
    running = RunningService(config, process, log_file, port, system_path)
    _running[config.key] = running
    return running


def service_state(config: ServiceConfig) -> dict[str, object]:
    port = service_port(config)
    running = _running.get(config.key)
    process_alive = bool(running and running.process.poll() is None)
    port_open = is_port_open(port)
    system_path = running.path if running else resolve_system_path(config)
    return {
        "key": config.key,
        "title": config.title,
        "subtitle": config.subtitle,
        "description": config.description,
        "url": service_url(port),
        "preferred_port": config.preferred_port,
        "port": port,
        "path": str(system_path),
        "exists": (system_path / "app.py").exists(),
        "process_alive": process_alive,
        "port_open": port_open,
        "ready": process_alive and port_open,
    }


def stop_children() -> None:
    for running in list(_running.values()):
        process = running.process
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        close_log_safely(running)
    _running.clear()


atexit.register(stop_children)


def load_child_flask_app(key: str, system_path: Path):
    system_path = system_path.resolve()
    app_file = system_path / "app.py"
    if not app_file.exists():
        raise FileNotFoundError(f"Cannot find child system app.py: {app_file}")

    sys.path.insert(0, str(system_path))
    os.chdir(system_path)

    module_name = f"_contract_kb_{key}_app"
    spec = importlib.util.spec_from_file_location(module_name, app_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load child system module: {app_file}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    had_frozen = hasattr(sys, "frozen")
    frozen_value = getattr(sys, "frozen", None)
    if had_frozen:
        sys.frozen = False
    try:
        spec.loader.exec_module(module)
    finally:
        if had_frozen:
            sys.frozen = frozen_value
    child_app = getattr(module, "app", None)
    if child_app is None:
        raise RuntimeError(f"Child system does not expose Flask app: {app_file}")
    return child_app


def serve_child_system(key: str, port: int, system_path: Path) -> int:
    child_app = load_child_flask_app(key, system_path)
    child_app.run(host=HOST, port=port, debug=False, use_reloader=False)
    return 0


@app.route("/")
def index():
    return render_template("index.html", services=SERVICES.values(), log_dir=LOG_DIR)


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/admin")
def admin():
    return render_template("admin.html")


@app.route("/api/admin/<section>", methods=["GET", "POST"])
def admin_config(section: str):
    config_name = ADMIN_CONFIG_SECTIONS.get(section)
    if config_name is None:
        return jsonify({"success": False, "error": "Unknown admin config section."}), 404

    if request.method == "GET":
        try:
            data = maintenance_config.load_config(config_name, base_dir=ADMIN_CONFIG_BASE_DIR)
        except Exception as exc:
            return jsonify({"success": False, "error": str(exc)}), 500
        return jsonify({"success": True, "data": data})

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": "Request body must be a JSON object."}), 400

    try:
        path = maintenance_config.save_config(
            config_name,
            data,
            base_dir=ADMIN_CONFIG_BASE_DIR,
            backup=True,
        )
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500
    return jsonify({"success": True, "path": str(path), "data": data})


@app.route("/open/<key>")
def open_service(key: str):
    config = SERVICES.get(key)
    if config is None:
        return "Unknown service", 404

    try:
        running = start_service(config)
    except RuntimeError as exc:
        return f"{config.subtitle} 启动失败：{exc}", 503

    if running is None:
        return f"{config.subtitle} 启动失败：未找到 app.py：{resolve_system_path(config)}", 503

    if not wait_for_port(running.port):
        log_path = LOG_DIR / f"{config.key}.log"
        return (
            f"{config.subtitle} 启动失败或端口 {running.port} 不可用。"
            f"请查看日志：{log_path}",
            503,
        )
    return redirect(service_url(running.port), code=302)


@app.route("/api/status")
def status():
    return jsonify([service_state(config) for config in SERVICES.values()])


def open_launcher_page(port: int) -> None:
    time.sleep(1.0)
    webbrowser.open(service_url(port))


def run_launcher(open_browser: bool = True) -> int:
    global ACTIVE_LAUNCHER_PORT
    ACTIVE_LAUNCHER_PORT = find_available_port(DEFAULT_LAUNCHER_PORT)
    print(f"Launcher URL: {service_url(ACTIVE_LAUNCHER_PORT)}")
    print("Service ports are selected automatically when you click an entry.")
    if open_browser:
        threading.Thread(target=open_launcher_page, args=(ACTIVE_LAUNCHER_PORT,), daemon=True).start()
    app.run(host=HOST, port=ACTIVE_LAUNCHER_PORT, debug=False, use_reloader=False)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="合同知识库控制台")
    parser.add_argument("--serve", choices=sorted(SERVICES), help="启动指定子系统服务")
    parser.add_argument("--port", type=int, help="子系统服务端口")
    parser.add_argument("--system-path", type=Path, help="子系统目录")
    parser.add_argument("--no-browser", action="store_true", help="启动控制台但不自动打开浏览器")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.serve:
        if args.port is None:
            raise SystemExit("--serve requires --port")
        config = SERVICES[args.serve]
        system_path = args.system_path or resolve_system_path(config)
        return serve_child_system(args.serve, args.port, system_path)
    return run_launcher(open_browser=not args.no_browser)


if __name__ == "__main__":
    raise SystemExit(main())
