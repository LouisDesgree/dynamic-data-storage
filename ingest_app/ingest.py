#!/usr/bin/env python3
"""
Local ingest CLI for transferring external volume contents to a Synology NAS.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import fnmatch
import functools
import errno
import hashlib
import json
import logging
import os
import platform
import posixpath
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import paramiko
import psutil
from tqdm import tqdm

DEFAULT_IGNORE_PATTERNS: Tuple[str, ...] = (
    ".DS_Store",
    "._*",
    ".DocumentRevisions-V100",
    ".Spotlight-V100",
    ".TemporaryItems",
    ".Trash",
    ".Trashes",
    ".fseventsd",
    "$RECYCLE.BIN",
    "System Volume Information",
    "Thumbs.db",
    "desktop.ini",
    "lost+found",
    ".ingestignore",
)

CATALOG_FILENAME = "catalog.jsonl"


@dataclass
class FileRecord:
    absolute: Path
    relative: Path
    size: int
    mtime: float
    sha256: str


@dataclass
class VolumeInfo:
    path: Path
    label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan an external volume, build a catalog, and send files to a Synology NAS."
    )
    parser.add_argument("--source", type=Path, help="Path to the source volume to ingest.")
    parser.add_argument("--label", help="Override the label used to name the remote ingest run.")
    parser.add_argument("--nas-host", help="NAS host or IP address.")
    parser.add_argument("--nas-user", help="NAS SSH username.")
    parser.add_argument("--nas-dest", help="Remote base path on the NAS (e.g. /volume1/BigSave).")
    parser.add_argument("--ssh-port", type=int, default=None, help="SSH port (defaults to 22 or .env SSH_PORT).")
    parser.add_argument("--remote-hook", help="Remote command to invoke after upload (optional).")
    parser.add_argument("--threads", type=int, default=None, help="Hashing threads (default: CPU count).")
    parser.add_argument("--env", type=Path, default=None, help="Path to a .env file (default: autodetect).")
    parser.add_argument("--dry-run", action="store_true", help="Only hash and build catalog; skip transfers.")
    parser.add_argument("--rsync-path", default="rsync", help="Path to rsync binary (default: rsync in PATH).")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"))
    parser.add_argument("--password", default=None, help="SSH password (keys/agent used when omitted).")
    parser.add_argument(
        "--status-interval",
        type=float,
        default=15.0,
        help="Seconds between progress log messages (set 0 to disable).",
    )
    parser.add_argument(
        "--single-file",
        type=Path,
        help="Limit ingest to a single file (absolute or relative to the source).",
    )
    parser.add_argument(
        "--skip-rsync",
        action="store_true",
        help="Disable rsync even if available (use SFTP only).",
    )
    parser.add_argument(
        "--no-timestamp",
        action="store_true",
        help="Reuse a static ingest directory instead of creating a timestamped run.",
    )
    parser.add_argument(
        "--run-subdir",
        help="Custom name for the ingest subdirectory (default: drive label).",
    )
    return parser.parse_args()


def setup_logging(level: str) -> Path:
    log_path = Path.cwd() / "ingest.log"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    return log_path


def flush_logging_handlers() -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        try:
            handler.flush()
        except Exception:
            continue


def deduce_env_path(path_hint: Optional[Path]) -> Optional[Path]:
    candidates: List[Path] = []
    if path_hint:
        candidates.append(Path(path_hint).expanduser())
    candidates.extend(
        [
            Path.cwd() / ".env",
            Path(__file__).resolve().parent / ".env",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def load_env_file(path: Optional[Path]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if path is None:
        return values
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        logging.debug("Unable to read env file %s: %s", path, exc)
        return values
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('\'"')
    return values


def parse_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", ""}


def resolve_config(args: argparse.Namespace) -> Dict[str, Optional[str]]:
    env_path = deduce_env_path(args.env)
    env_values = load_env_file(env_path)
    host = args.nas_host or env_values.get("NAS_HOST")
    user = args.nas_user or env_values.get("NAS_USER")
    dest = args.nas_dest or env_values.get("NAS_DEST")
    password = args.password or env_values.get("NAS_PASSWORD")
    port_value = args.ssh_port or env_values.get("SSH_PORT")
    port = int(port_value) if port_value else 22
    remote_hook = args.remote_hook or env_values.get("REMOTE_HOOK")
    finalize_move = parse_bool(env_values.get("FINALIZE_MOVE"), default=True)
    run_subdir = env_values.get("RUN_SUBDIR")
    timestamp_runs = parse_bool(env_values.get("RUN_TIMESTAMPED"), default=True)
    if not host or not user or not dest:
        raise ValueError("NAS_HOST, NAS_USER, and NAS_DEST must be provided via flags or .env")
    return {
        "host": host,
        "user": user,
        "dest": dest,
        "port": port,
        "password": password,
        "remote_hook": remote_hook,
        "env_path": str(env_path) if env_path else None,
        "finalize_move": finalize_move,
        "run_subdir": run_subdir,
        "timestamp_runs": timestamp_runs,
    }


def read_volume_label(path: Path, system: str) -> str:
    if system == "Windows":
        try:
            import ctypes
            from ctypes import wintypes
        except ImportError:
            return path.drive.rstrip(":\\/")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        volume_name_buf = ctypes.create_unicode_buffer(261)
        file_system_name_buf = ctypes.create_unicode_buffer(261)
        serial_number = wintypes.DWORD()
        max_component_length = wintypes.DWORD()
        file_system_flags = wintypes.DWORD()
        root = str(path)
        if not root.endswith("\\"):
            root = f"{root}\\"
        result = kernel32.GetVolumeInformationW(
            ctypes.c_wchar_p(root),
            volume_name_buf,
            len(volume_name_buf),
            ctypes.byref(serial_number),
            ctypes.byref(max_component_length),
            ctypes.byref(file_system_flags),
            file_system_name_buf,
            len(file_system_name_buf),
        )
        if result:
            label = volume_name_buf.value.strip()
            if label:
                return label
        return path.drive.rstrip(":\\/")
    name = path.name.strip()
    if name:
        return name
    parts = [segment for segment in path.as_posix().split("/") if segment]
    return parts[-1] if parts else "volume"


def discover_latest_volume() -> VolumeInfo:
    system = platform.system()
    candidates: List[Tuple[float, VolumeInfo]] = []
    try:
        partitions = psutil.disk_partitions(all=False)
    except Exception as exc:
        raise RuntimeError(f"Unable to enumerate mounted volumes: {exc}") from exc
    for part in partitions:
        mountpoint = Path(part.mountpoint)
        if system == "Darwin":
            if not str(mountpoint).startswith("/Volumes/"):
                continue
        elif system == "Linux":
            mount_str = str(mountpoint)
            if not (mount_str.startswith("/media/") or mount_str.startswith("/run/media/")):
                continue
        elif system == "Windows":
            if "removable" not in part.opts.lower():
                continue
        else:
            continue
        try:
            stat_result = mountpoint.stat()
        except OSError:
            continue
        label = read_volume_label(mountpoint, system)
        candidates.append((stat_result.st_mtime, VolumeInfo(path=mountpoint, label=label)))
    if not candidates:
        raise RuntimeError("No removable volumes detected. Provide --source to pick one manually.")
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def sanitize_label(raw_label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in raw_label.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "volume"


def load_ignore_patterns(source: Path) -> List[str]:
    patterns = list(DEFAULT_IGNORE_PATTERNS)
    ignore_file = source / ".ingestignore"
    if ignore_file.exists():
        try:
            lines = ignore_file.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            logging.warning("Unable to read %s: %s", ignore_file, exc)
            return patterns
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
    return patterns


def should_ignore(relative_path: Path, patterns: Sequence[str], is_dir: bool = False) -> bool:
    path_str = relative_path.as_posix()
    name = relative_path.name
    for pattern in patterns:
        if fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(name, pattern):
            return True
        if is_dir and fnmatch.fnmatch(f"{path_str}/", pattern):
            return True
    return False


def collect_files(source: Path, ignore_patterns: Sequence[str]) -> List[Tuple[Path, Path]]:
    pairs: List[Tuple[Path, Path]] = []
    total_entries = 0
    for _root, _dirs, files in os.walk(source):
        total_entries += len(files)

    progress = create_progress(total_entries or 1, "Scanning", unit="entry", unit_scale=False, leave=True)
    next_log = time.time() + 2.0
    try:
        for root, dirs, files in os.walk(source):
            root_path = Path(root)
            rel_root = root_path.relative_to(source)

            for dirname in list(dirs):
                rel_dir = rel_root / dirname
                if should_ignore(rel_dir, ignore_patterns, is_dir=True):
                    dirs.remove(dirname)

            for filename in files:
                rel_path = rel_root / filename
                if should_ignore(rel_path, ignore_patterns):
                    progress.update(1)
                    continue
                absolute_path = root_path / filename
                try:
                    if not absolute_path.is_file():
                        progress.update(1)
                        continue
                except OSError as exc:
                    logging.warning("Skipping %s due to access error: %s", absolute_path, exc)
                    progress.update(1)
                    continue
                pairs.append((absolute_path, rel_path))
                progress.update(1)
                progress.set_postfix_str(shorten_path(rel_path.as_posix()))

                if time.time() >= next_log:
                    log_progress(
                        "Scanning",
                        int(progress.n),
                        int(progress.total or progress.n or 1),
                        detail=shorten_path(rel_path.as_posix()),
                    )
                    next_log = time.time() + 2.0

        log_progress(
            "Scanning",
            int(progress.n),
            int(progress.total or progress.n or 1),
            detail="completed",
        )
    finally:
        progress.close()
    return pairs


def hash_file(pair: Tuple[Path, Path]) -> FileRecord:
    absolute, relative = pair
    try:
        stat_result = absolute.stat()
    except OSError as exc:
        raise RuntimeError(f"Unable to stat {absolute}: {exc}") from exc
    hasher = hashlib.sha256()
    try:
        with absolute.open("rb") as handle:
            for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                hasher.update(chunk)
    except OSError as exc:
        raise RuntimeError(f"Unable to read {absolute}: {exc}") from exc
    return FileRecord(
        absolute=absolute,
        relative=relative,
        size=stat_result.st_size,
        mtime=stat_result.st_mtime,
        sha256=hasher.hexdigest(),
    )


def hash_files(
    pairs: List[Tuple[Path, Path]],
    threads: int,
    status_interval: float,
) -> Tuple[List[FileRecord], int]:
    records: List[FileRecord] = []
    total_bytes = 0
    total_files = len(pairs)
    next_log = time.time() + status_interval if status_interval and status_interval > 0 else float("inf")
    start_time = time.time()
    with create_progress(total_files, "Hashing", unit="file", unit_scale=False, leave=False) as progress:
        with cf.ThreadPoolExecutor(max_workers=threads) as executor:
            for index, record in enumerate(executor.map(hash_file, pairs), start=1):
                records.append(record)
                total_bytes += record.size
                progress.update(1)
                if time.time() >= next_log:
                    elapsed = max(time.time() - start_time, 1e-6)
                    rate = index / elapsed
                    remaining = (total_files - index) / rate if rate > 0 else 0
                    log_progress(
                        "Hashing",
                        index,
                        total_files,
                        eta=format_duration(remaining),
                    )
                    next_log = time.time() + status_interval
    return records, total_bytes


def write_catalog(records: List[FileRecord], tmpdir: Path) -> Path:
    catalog_path = tmpdir / CATALOG_FILENAME
    with catalog_path.open("w", encoding="utf-8") as handle:
        for record in records:
            payload = {
                "relative_path": record.relative.as_posix(),
                "size": record.size,
                "mtime": record.mtime,
                "sha256": record.sha256,
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return catalog_path


def build_manifest(records: List[FileRecord], tmpdir: Path) -> Path:
    manifest_path = tmpdir / "manifest.txt"
    with manifest_path.open("wb") as handle:
        for record in records:
            handle.write(record.relative.as_posix().encode("utf-8"))
            handle.write(b"\0")
        handle.flush()
        os.fsync(handle.fileno())
    return manifest_path


def normalize_posix(path: str) -> str:
    return posixpath.normpath(path.replace("\\", "/"))


def load_catalog(catalog_path: Path) -> Dict[str, Dict[str, float]]:
    catalog: Dict[str, Dict[str, float]] = {}
    with catalog_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            rel_path = entry["relative_path"]
            catalog[rel_path] = {
                "sha256": entry.get("sha256", ""),
                "size": entry.get("size", 0),
                "mtime": entry.get("mtime", 0.0),
            }
    return catalog


def remote_file_sha256(client: paramiko.SSHClient, abs_path: str) -> Optional[str]:
    quoted = shlex.quote(abs_path)
    command = (
        f"if command -v sha256sum >/dev/null 2>&1; then sha256sum {quoted}; "
        f"elif command -v shasum >/dev/null 2>&1; then shasum -a 256 {quoted}; "
        "else exit 127; fi"
    )
    _, stdout, stderr = client.exec_command(command)
    output = stdout.read().decode("utf-8", errors="ignore").strip()
    error_output = stderr.read().decode("utf-8", errors="ignore").strip()
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        if error_output:
            logging.debug("Unable to compute remote hash for %s: %s", abs_path, error_output)
        return None
    if not output:
        return None
    hash_value = output.split()[0]
    if len(hash_value) < 32:
        return None
    return hash_value


def compute_conflict_free_name(sftp: paramiko.SFTPClient, dest_root: str, rel_path: str) -> str:
    rel = PurePosixPath(rel_path)
    parent = rel.parent
    stem = rel.stem
    suffix = rel.suffix
    index = 1
    while True:
        if suffix:
            candidate_name = f"{stem} ({index}){suffix}"
        else:
            candidate_name = f"{stem} ({index})"
        candidate_rel = str(parent / candidate_name) if parent != PurePosixPath('.') else candidate_name
        candidate_abs = posixpath.join(dest_root, candidate_rel)
        try:
            sftp.stat(candidate_abs)
        except IOError as exc:
            if getattr(exc, "errno", None) == errno.ENOENT:
                return candidate_rel
            logging.debug("Unable to stat %s while searching for conflict-free name (errno=%s); using candidate.", candidate_abs, getattr(exc, "errno", None))
            return candidate_rel
        index += 1


def stream_command_output(
    cmd: Sequence[str],
    label: str,
    *,
    progress: Optional[tqdm] = None,
) -> None:
    logging.info("Running rsync (%s)", label)
    logging.debug("Command: %s", " ".join(shlex.quote(part) for part in cmd))
    process = subprocess.Popen(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )
    assert process.stdout is not None
    last_log = time.time()
    try:
        for raw_line in process.stdout:
            line = raw_line.rstrip()
            if not line:
                continue
            info = parse_rsync_progress(line) if progress else None
            if progress and info:
                target = info["bytes"]
                total = progress.total or 0
                if total:
                    target = min(target, total)
                delta = target - progress.n
                if delta > 0:
                    progress.update(delta)
                progress.set_postfix({"speed": info["speed"], "eta": info["eta"]})
                if time.time() - last_log >= 2.0:
                    speed_text = info.get("speed")
                    eta_text = info.get("eta")
                    log_progress(
                        "rsync",
                        int(progress.n),
                        int(progress.total or progress.n or 1),
                        speed=speed_text,
                        eta=eta_text,
                    )
                    last_log = time.time()
            logging.info("[rsync] %s", line)
    finally:
        process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)
    logging.info("rsync (%s) completed.", label)


def open_ssh_client(
    host: str,
    port: int,
    user: str,
    password: Optional[str],
) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=user,
        password=password,
        allow_agent=True,
        look_for_keys=True,
        timeout=30,
    )
    return client


@contextmanager
def sftp_session(client: paramiko.SSHClient) -> paramiko.SFTPClient:
    try:
        sftp = client.open_sftp()
    except paramiko.SSHException as exc:
        raise RuntimeError(
            "SFTP subsystem unavailable on NAS. Enable SFTP in DSM (Control Panel -> File Services -> FTP -> Enable SFTP)."
        ) from exc
    try:
        yield sftp
    finally:
        sftp.close()


def ensure_remote_directory(client: paramiko.SSHClient, remote_path: str) -> None:
    command = f"mkdir -p {shlex.quote(remote_path)}"
    _, stdout, stderr = client.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        err_output = stderr.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Unable to create remote directory {remote_path}: {err_output.strip()}")


@functools.lru_cache(maxsize=None)
def rsync_capabilities(rsync_path: str) -> Tuple[bool, Optional[str], Tuple[int, int, int]]:
    try:
        result = subprocess.run(
            [rsync_path, "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception as exc:
        logging.debug("Unable to determine rsync version via %s: %s", rsync_path, exc)
        return False, None, (0, 0, 0)
    match = re.search(r"rsync\s+version\s+(\d+)\.(\d+)(?:\.(\d+))?", result.stdout)
    if not match:
        logging.debug("Could not parse rsync version from output: %s", result.stdout.splitlines()[0] if result.stdout else "<empty>")
        return False, None, (0, 0, 0)
    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3) or 0)
    components = [major, minor]
    if match.group(3) is not None:
        components.append(patch)
    version_str = ".".join(str(part) for part in components)
    supports = (major, minor, patch) >= (3, 0, 0)
    return supports, version_str, (major, minor, patch)


def transfer_with_rsync(
    rsync_path: str,
    manifest_path: Path,
    source: Path,
    remote_files_dir: str,
    user: str,
    host: str,
    port: int,
    total_bytes: int,
) -> None:
    supports_protect_args, version, version_tuple = rsync_capabilities(rsync_path)
    supports_progress2 = version_tuple >= (3, 0, 0)
    cmd = [
        rsync_path,
        "-av",
        "--human-readable",
        "--prune-empty-dirs",
        "--files-from",
        str(manifest_path),
        "--from0",
    ]
    if supports_progress2:
        cmd.insert(3, "--info=progress2")
        cmd.insert(4, "--outbuf=L")
    else:
        cmd.insert(3, "--progress")
    if supports_protect_args:
        cmd.insert(3, "--protect-args")
    else:
        label = f"rsync {version}" if version else "Local rsync"
        logging.info(
            "%s does not support --protect-args; quoting remote path manually. Consider installing rsync >= 3.0 for better compatibility.",
            label,
        )
    if port != 22:
        cmd.extend(["-e", f"ssh -p {port}"])
    cmd.append(str(source) + "/")
    remote_path = remote_files_dir if remote_files_dir.endswith("/") else f"{remote_files_dir}/"
    if supports_protect_args:
        remote_arg = f"{user}@{host}:{remote_path}"
    else:
        remote_arg = f"{user}@{host}:{shlex.quote(remote_path)}"
    cmd.append(remote_arg)
    progress = create_progress(total_bytes, f"rsync -> {remote_files_dir}", unit="B", unit_scale=True, leave=False) if total_bytes else None
    try:
        if progress:
            progress.set_postfix_str("starting...")
        stream_command_output(cmd, f"transfer -> {remote_files_dir}", progress=progress)
    finally:
        if progress:
            log_progress(
                "rsync",
                int(progress.n),
                int(progress.total or progress.n or 1),
                detail="completed",
            )
            if progress.total and progress.n < progress.total:
                progress.update(progress.total - progress.n)
            progress.close()


def transfer_single_file_with_rsync(
    rsync_path: str,
    local_path: Path,
    remote_path: str,
    user: str,
    host: str,
    port: int,
    description: str,
) -> None:
    supports_protect_args, version, version_tuple = rsync_capabilities(rsync_path)
    supports_progress2 = version_tuple >= (3, 0, 0)
    cmd = [
        rsync_path,
        "-av",
        "--human-readable",
    ]
    if supports_progress2:
        cmd.extend(["--info=progress2", "--outbuf=L"])
    else:
        cmd.append("--progress")
    if supports_protect_args:
        cmd.insert(3, "--protect-args")
    else:
        label = f"rsync {version}" if version else "Local rsync"
        logging.debug("%s does not support --protect-args for single-file upload.", label)
    if port != 22:
        cmd.extend(["-e", f"ssh -p {port}"])
    cmd.append(str(local_path))
    remote_arg = f"{user}@{host}:{remote_path}" if supports_protect_args else f"{user}@{host}:{shlex.quote(remote_path)}"
    cmd.append(remote_arg)
    file_size = local_path.stat().st_size if local_path.exists() else 0
    progress = create_progress(file_size, f"rsync -> {description}", unit="B", unit_scale=True, leave=False) if file_size else None
    try:
        if progress:
            progress.set_postfix_str("starting...")
        stream_command_output(cmd, description, progress=progress)
    finally:
        if progress:
            log_progress(
                "rsync",
                int(progress.n),
                int(progress.total or progress.n or 1),
                detail="completed",
            )
            if progress.total and progress.n < progress.total:
                progress.update(progress.total - progress.n)
            progress.close()


def sftp_mkdirs(
    sftp: paramiko.SFTPClient,
    remote_dir: str,
    base_dir: Optional[str] = None,
) -> None:
    """
    Ensure that `remote_dir` exists on the SFTP server.
    If `base_dir` is provided, we first try to `chdir` into it and
    then only create *relative* subpaths beneath it. This avoids
    SFTP permissions on top-level mounts like '/volume1'.
    """
    remote_dir = posixpath.normpath(remote_dir)
    base_dir = posixpath.normpath(base_dir) if base_dir else None

    def _ensure_relative(subpath: str) -> None:
        subpath = posixpath.normpath(subpath)
        if subpath in (".", "", "/"):
            return
        parts = [p for p in subpath.split("/") if p]
        current = "."
        for part in parts:
            current = posixpath.join(current, part)
            try:
                sftp.stat(current)
                continue
            except IOError:
                try:
                    sftp.mkdir(current)
                except IOError as mk_exc:
                    errno_value = getattr(mk_exc, "errno", None)
                    if errno_value in (errno.EEXIST, errno.EACCES, errno.ENOENT):
                        logging.debug("Ignoring mkdir failure for %s (errno=%s)", current, errno_value)
                        continue
                    # If it already exists or we cannot create due to race, re-check
                    try:
                        sftp.stat(current)
                    except Exception as recheck_exc:
                        raise RuntimeError(f"SFTP mkdir failed for '{current}': {recheck_exc}") from recheck_exc

    # If base_dir is provided and remote_dir is within it, work relatively
    if base_dir and (remote_dir == base_dir or remote_dir.startswith(base_dir.rstrip("/") + "/")):
        tail = remote_dir[len(base_dir):].lstrip("/")
        # Try to chdir into base_dir first; if that fails, fall back to absolute handling.
        try:
            sftp.chdir(base_dir)
        except IOError as exc:
            logging.debug(
                "Unable to chdir into base_dir %s via SFTP (%s); falling back to absolute mkdir.",
                base_dir,
                exc,
            )
        else:
            _ensure_relative(tail)
            return

    # Otherwise, try absolute creation cautiously (skip attempting to mkdir root components like '/volume1')
    path = remote_dir
    is_abs = path.startswith("/")
    parts = [p for p in posixpath.normpath(path).split("/") if p]
    current = "/" if is_abs else "."
    for idx, part in enumerate(parts):
        current = posixpath.join(current, part)
        try:
            sftp.stat(current)
            continue
        except IOError:
            # Only attempt to mkdir if not a top-level like '/volume1'
            if is_abs and idx == 0:
                # Assume top-level mount exists but is not stat'able due to perms; continue into subdirs
                continue
            try:
                sftp.mkdir(current)
            except IOError as mk_exc:
                errno_value = getattr(mk_exc, "errno", None)
                if errno_value in (errno.EEXIST, errno.EACCES, errno.ENOENT):
                    logging.debug("Ignoring mkdir failure for %s (errno=%s)", current, errno_value)
                    continue
                try:
                    sftp.stat(current)
                except Exception as recheck_exc:
                    raise RuntimeError(f"SFTP mkdir failed for '{current}': {recheck_exc}") from recheck_exc


def upload_files_via_sftp(
    sftp: paramiko.SFTPClient,
    records: List[FileRecord],
    remote_files_dir: str,
    status_interval: float,
    base_dir: str,
) -> None:
    total_bytes = sum(record.size for record in records)
    total_files = len(records)
    next_log = time.time() + status_interval if status_interval and status_interval > 0 else float("inf")
    start_time = time.time()
    logging.info(
        "Uploading %d files via SFTP (%s total)",
        total_files,
        format_size(total_bytes),
    )
    with create_progress(total_bytes, "Uploading", unit="B", unit_scale=True) as progress:
        for record in records:
            remote_path = posixpath.join(remote_files_dir, record.relative.as_posix())
            remote_parent = posixpath.dirname(remote_path)
            sftp_mkdirs(sftp, remote_parent, base_dir=base_dir)
            last = {"value": 0}

            def callback(transferred: int, _total: int) -> None:
                delta = transferred - last["value"]
                if delta > 0:
                    progress.update(delta)
                    last["value"] = transferred

            progress.set_postfix_str(shorten_path(record.relative.as_posix()))
            sftp.put(str(record.absolute), remote_path, callback=callback)
            progress.update(last["value"] - progress.n)
            if time.time() >= next_log:
                uploaded_bytes = progress.n
                percent = (uploaded_bytes / total_bytes) * 100 if total_bytes else 100.0
                elapsed = max(time.time() - start_time, 1e-6)
                rate = uploaded_bytes / elapsed if elapsed > 0 else 0
                remaining = (total_bytes - uploaded_bytes) / rate if rate > 0 else 0
                log_progress(
                    "SFTP",
                    int(uploaded_bytes),
                    int(total_bytes),
                    speed=f"{format_size(rate)}/s" if rate else None,
                    eta=format_duration(remaining),
                    detail=shorten_path(record.relative.as_posix()),
                )
                next_log = time.time() + status_interval
    logging.info("SFTP upload complete.")


def upload_file(
    sftp: paramiko.SFTPClient,
    local_path: Path,
    remote_path: str,
    description: str,
    base_dir: Optional[str] = None,
) -> None:
    total_bytes = local_path.stat().st_size
    logging.info("Uploading %s (%s)", description, format_size(total_bytes))
    start_time = time.time()
    with create_progress(total_bytes, f"Uploading {description}", unit="B", unit_scale=True, leave=False) as progress:
        last = {"value": 0}

        def callback(transferred: int, _total: int) -> None:
            delta = transferred - last["value"]
            if delta > 0:
                progress.update(delta)
                last["value"] = transferred

        remote_parent = posixpath.dirname(remote_path)
        sftp_mkdirs(sftp, remote_parent, base_dir=base_dir)
        progress.set_postfix_str(shorten_path(remote_path))
        sftp.put(str(local_path), remote_path, callback=callback)
        progress.update(last["value"] - progress.n)
    elapsed = max(time.time() - start_time, 1e-6)
    logging.info("%s upload complete in %s (avg %s/s)", description, format_duration(elapsed), format_size(total_bytes / elapsed if elapsed > 0 else 0))


def finalize_move(
    client: paramiko.SSHClient,
    dest_root: str,
    run_root: str,
    records: List[FileRecord],
    catalog_map: Dict[str, Dict[str, float]],
) -> Dict[str, object]:
    files_root = posixpath.join(run_root, "files")
    dest_root = posixpath.normpath(dest_root)
    summary: Dict[str, object] = {
        "moved": [],
        "skipped_same_hash": [],
        "renamed_conflict": [],
        "errors": [],
    }

    total_records = len(records)
    if not total_records:
        logging.info("No files to finalize.")
    else:
        logging.info(
            "Finalizing ingest: moving %d files from %s to %s",
            total_records,
            files_root,
            dest_root,
        )

    with sftp_session(client) as sftp:
        start_time = time.time()
        progress = create_progress(total_records, "Finalizing", unit="file", unit_scale=False, leave=False) if total_records else None
        try:
            for index, record in enumerate(records, start=1):
                rel_path = record.relative.as_posix()
                expected = catalog_map.get(rel_path, {}).get("sha256")
                source_abs = posixpath.join(files_root, rel_path)
                dest_abs = posixpath.join(dest_root, rel_path)
                try:
                    sftp.stat(source_abs)
                except IOError as exc:
                    if getattr(exc, "errno", None) == errno.ENOENT:
                        logging.warning("Source missing during finalize: %s", source_abs)
                    summary["errors"].append({"path": rel_path, "error": str(exc)})
                    continue

                destination_parent = posixpath.dirname(dest_abs)
                try:
                    sftp_mkdirs(sftp, destination_parent, base_dir=dest_root)
                except Exception as exc:
                    logging.error("Unable to prepare destination directory %s: %s", destination_parent, exc)
                    summary["errors"].append({"path": rel_path, "error": str(exc)})
                    continue

                dest_exists = False
                try:
                    sftp.stat(dest_abs)
                    dest_exists = True
                except IOError as exc:
                    if getattr(exc, "errno", None) != errno.ENOENT:
                        logging.debug("stat(%s) failed with %s", dest_abs, exc)
                        dest_exists = True

                if not dest_exists:
                    try:
                        sftp.rename(source_abs, dest_abs)
                        summary["moved"].append(rel_path)
                    except Exception as exc:
                        logging.error("Failed to move %s to %s: %s", source_abs, dest_abs, exc)
                        summary["errors"].append({"path": rel_path, "error": str(exc)})
                    finally:
                        if progress:
                            progress.update(1)
                            progress.set_postfix_str(shorten_path(rel_path))
                    continue

                remote_hash = None
                if expected:
                    remote_hash = remote_file_sha256(client, dest_abs)
                    if remote_hash:
                        remote_hash = remote_hash.lower()
                        expected = expected.lower()

                if remote_hash and expected and remote_hash == expected:
                    logging.info("Duplicate detected for %s; discarding uploaded copy.", rel_path)
                    try:
                        sftp.remove(source_abs)
                        summary["skipped_same_hash"].append(rel_path)
                    except Exception as exc:
                        logging.error("Failed to remove duplicate %s: %s", source_abs, exc)
                        summary["errors"].append({"path": rel_path, "error": str(exc)})
                    if progress:
                        progress.update(1)
                        progress.set_postfix_str(shorten_path(rel_path))
                    continue

                conflict_rel = compute_conflict_free_name(sftp, dest_root, rel_path)
                conflict_abs = posixpath.join(dest_root, conflict_rel)
                conflict_parent = posixpath.dirname(conflict_abs)
                try:
                    sftp_mkdirs(sftp, conflict_parent, base_dir=dest_root)
                    sftp.rename(source_abs, conflict_abs)
                    logging.info("Renamed %s due to conflict -> %s", rel_path, conflict_rel)
                    summary["renamed_conflict"].append({"from": rel_path, "to": conflict_rel})
                except Exception as exc:
                    logging.error("Failed to rename %s to %s: %s", source_abs, conflict_abs, exc)
                    summary["errors"].append({"path": rel_path, "error": str(exc)})

                if progress:
                    progress.update(1)
                    progress.set_postfix_str(shorten_path(rel_path))
                    if total_records >= 10 and (index % 10 == 0 or index == total_records):
                        elapsed = max(time.time() - start_time, 1e-6)
                        rate = index / elapsed if elapsed > 0 else 0
                        remaining = (total_records - index) / rate if rate > 0 else 0
                        log_progress(
                            "Finalize",
                            index,
                            total_records,
                            eta=format_duration(remaining),
                            detail=shorten_path(rel_path),
                        )
        finally:
            if progress:
                progress.close()

        # Cleanup empty directories under files_root
        cleanup_cmd = (
            f"if [ -d {shlex.quote(files_root)} ]; then "
            f"find {shlex.quote(files_root)} -type d -empty -delete >/dev/null 2>&1 || true; "
            f"rmdir {shlex.quote(files_root)} >/dev/null 2>&1 || true; "
            "fi"
        )
        _, stdout, _ = client.exec_command(cleanup_cmd)
        stdout.channel.recv_exit_status()

        # Write finalize summary file
        summary_payload = {
            "moved": summary["moved"],
            "skipped_same_hash": summary["skipped_same_hash"],
            "renamed_conflict": summary["renamed_conflict"],
            "errors": summary["errors"],
            "counts": {
                "moved": len(summary["moved"]),
                "skipped_same_hash": len(summary["skipped_same_hash"]),
                "renamed_conflict": len(summary["renamed_conflict"]),
                "errors": len(summary["errors"]),
            },
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        finalize_path = posixpath.join(run_root, "finalize.json")
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp_file:
            json.dump(summary_payload, tmp_file, indent=2)
            tmp_file_path = Path(tmp_file.name)

        try:
            upload_file(
                sftp,
                tmp_file_path,
                finalize_path,
                "finalize.json",
                base_dir=normalize_posix(dest_root),
            )
        finally:
            try:
                tmp_file_path.unlink(missing_ok=True)
            except OSError:
                pass

    return summary_payload


def trigger_remote_hook(client: paramiko.SSHClient, command: str, remote_root: str) -> None:
    full_command = f"{command} {shlex.quote(remote_root)}"
    logging.info("Triggering remote hook: %s", full_command)
    _, stdout, stderr = client.exec_command(full_command)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        output = stderr.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Remote hook failed with exit code {exit_status}: {output.strip()}")


def find_duplicates(records: List[FileRecord]) -> Dict[str, List[FileRecord]]:
    by_digest: Dict[str, List[FileRecord]] = {}
    for record in records:
        by_digest.setdefault(record.sha256, []).append(record)
    return {digest: items for digest, items in by_digest.items() if len(items) > 1}


def format_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} PB"


def format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if sec or not parts:
        parts.append(f"{sec}s")
    return " ".join(parts)


GREEN_BAR_FORMAT = "{l_bar}\033[92m{bar}\033[0m| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
RSYNC_PROGRESS_RE = re.compile(
    r"(?P<bytes>[\d,]+)\s+(?P<percent>\d{1,3})%\s+(?P<speed>[\d\.,]+[KMGTP]?B/s)\s+(?P<eta>\d+:\d+:\d+)"
)


def create_progress(
    total: Optional[int],
    description: str,
    unit: str,
    *,
    unit_scale: bool = False,
    unit_divisor: int = 1024,
    leave: bool = True,
) -> tqdm:
    total_value = max(total or 0, 0)
    return tqdm(
        total=total_value,
        desc=description,
        unit=unit,
        unit_scale=unit_scale,
        unit_divisor=unit_divisor,
        bar_format=GREEN_BAR_FORMAT,
        dynamic_ncols=True,
        leave=leave,
    )


def render_ascii_bar(current: int, total: int, width: int = 24) -> str:
    if total <= 0:
        fraction = 0.0
    else:
        fraction = max(0.0, min(1.0, current / total))
    filled = int(round(fraction * width))
    filled = min(width, max(0, filled))
    bar = "#" * filled + "-" * (width - filled)
    return f"\033[92m[{bar}]\033[0m"


def shorten_path(path: str, max_len: int = 40) -> str:
    if len(path) <= max_len:
        return path
    return "..." + path[-(max_len - 3) :]


def parse_rsync_progress(line: str) -> Optional[Dict[str, Any]]:
    match = RSYNC_PROGRESS_RE.search(line)
    if not match:
        return None
    bytes_done = int(match.group("bytes").replace(",", ""))
    percent = int(match.group("percent"))
    speed = match.group("speed").replace(",", ".")
    eta = match.group("eta")
    return {
        "bytes": bytes_done,
        "percent": percent,
        "speed": speed,
        "eta": eta,
    }


def log_progress(
    label: str,
    current: int,
    total: int,
    *,
    speed: Optional[str] = None,
    eta: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    bar = render_ascii_bar(current, total)
    percent = (current / total * 100) if total else 0.0
    segments = [f"{label} {bar} {percent:5.1f}% ({current}/{total})"]
    if speed:
        segments.append(f"speed {speed}")
    if eta:
        segments.append(f"ETA {eta}")
    if detail:
        segments.append(detail)
    logging.info(" | ".join(segments))


def print_summary(
    records: List[FileRecord],
    total_bytes: int,
    duplicates: Dict[str, List[FileRecord]],
    remote_root: str,
) -> None:
    logging.info("Remote ingest location: %s", remote_root)
    logging.info("Files ingested: %d", len(records))
    logging.info("Total size: %s", format_size(total_bytes))
    if duplicates:
        logging.warning("Duplicate content detected (%d digests). First few collisions:", len(duplicates))
        shown = 0
        for digest, items in duplicates.items():
            rel_paths = ", ".join(str(record.relative) for record in items[:3])
            logging.warning("  %s -> %s%s", digest[:12], rel_paths, "..." if len(items) > 3 else "")
            shown += 1
            if shown >= 5:
                break
    else:
        logging.info("No duplicate SHA-256 hashes detected.")


def main() -> int:
    args = parse_args()
    log_path = setup_logging(args.log_level)
    logging.info("Logging to %s", log_path)
    try:
        config = resolve_config(args)
        logging.debug("Resolved configuration: %s", {k: v for k, v in config.items() if k != "password"})
    except ValueError as exc:
        logging.error("%s", exc)
        return 2

    if args.source:
        source_path = Path(args.source).expanduser().resolve()
        if not source_path.exists() or not source_path.is_dir():
            logging.error("Source path %s does not exist or is not a directory.", source_path)
            return 2
        detected_label = args.label or sanitize_label(source_path.name or "volume")
    else:
        try:
            volume = discover_latest_volume()
        except RuntimeError as exc:
            logging.error("%s", exc)
            return 2
        source_path = volume.path
        detected_label = args.label or sanitize_label(volume.label)
    logging.info("Source volume: %s", source_path)
    logging.info("Drive label: %s", detected_label)

    ignore_patterns = load_ignore_patterns(source_path)
    if args.single_file:
        logging.info("Single-file test mode enabled.")
        single_path = Path(args.single_file)
        if single_path.is_absolute():
            absolute_single = single_path.expanduser().resolve()
        else:
            absolute_single = (source_path / single_path).resolve()
        if not absolute_single.exists() or not absolute_single.is_file():
            logging.error("Single file %s does not exist or is not a file.", absolute_single)
            return 2
        source_resolved = source_path.resolve()
        try:
            relative_single = absolute_single.relative_to(source_resolved)
        except ValueError:
            logging.error("Single file %s is outside the source %s.", absolute_single, source_path)
            return 2
        if should_ignore(relative_single, ignore_patterns):
            logging.error("Single file %s is ignored by ingest rules.", relative_single)
            return 2
        pairs = [(absolute_single, relative_single)]
        logging.info("Ingesting only %s", relative_single)
    else:
        logging.info("Scanning source for files...")
        pairs = collect_files(source_path, ignore_patterns)
        logging.info("Discovered %d files to ingest.", len(pairs))
    if not pairs:
        logging.warning("No files found to ingest under %s", source_path)
        return 0

    cpu_count = os.cpu_count() or 1
    threads = args.threads or cpu_count
    threads = max(1, min(threads, cpu_count))
    logging.info("Hashing with %d worker threads (status interval %.1fs).", threads, args.status_interval)

    try:
        records, total_bytes = hash_files(pairs, threads, args.status_interval)
    except RuntimeError as exc:
        logging.error("%s", exc)
        return 2

    duplicates = find_duplicates(records)

    with tempfile.TemporaryDirectory(prefix="ingest_") as tmpdir_str:
        tmpdir = Path(tmpdir_str)
        catalog_path = write_catalog(records, tmpdir)
        manifest_path = build_manifest(records, tmpdir)
        logging.info("Catalog written to %s", catalog_path)

        if args.dry_run:
            logging.info("Dry-run complete. Skipping transfer.")
            print_summary(records, total_bytes, duplicates, "<dry-run>")
            return 0

        dest_root = normalize_posix(config["dest"])
        run_subdir_override = args.run_subdir or config.get("run_subdir")
        base_run_label = sanitize_label(run_subdir_override) if run_subdir_override else detected_label
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        use_timestamp = config.get("timestamp_runs", True)
        if args.no_timestamp:
            use_timestamp = False
        if use_timestamp:
            run_suffix = f"{base_run_label}_{timestamp}"
        else:
            run_suffix = base_run_label
            logging.info("Using static ingest folder: %s", run_suffix)
        remote_root = posixpath.join(dest_root, ".ingest", run_suffix)
        remote_files_dir = posixpath.join(remote_root, "files")
        remote_catalog_path = posixpath.join(remote_root, CATALOG_FILENAME)
        remote_manifest_path = posixpath.join(remote_root, "manifest.txt")
        remote_log_path = posixpath.join(remote_root, "ingest.log")

        try:
            client = open_ssh_client(
                host=config["host"],
                port=config["port"],
                user=config["user"],
                password=config["password"],
            )
        except Exception as exc:
            logging.error("SSH connection failed: %s", exc)
            return 2

        try:
            ensure_remote_directory(client, remote_files_dir)
            used_rsync = False
            skip_rsync = args.skip_rsync
            if config["password"] and not skip_rsync:
                logging.info(
                    "NAS_PASSWORD provided; skipping rsync because password-based SSH cannot be automated. Configure SSH keys or use --skip-rsync explicitly."
                )
                skip_rsync = True
            if not skip_rsync and shutil.which(args.rsync_path):
                try:
                    transfer_with_rsync(
                        rsync_path=args.rsync_path,
                        manifest_path=manifest_path,
                        source=source_path,
                        remote_files_dir=remote_files_dir,
                        user=config["user"],
                        host=config["host"],
                        port=config["port"],
                        total_bytes=total_bytes,
                    )
                    used_rsync = True
                except subprocess.CalledProcessError as exc:
                    logging.warning("rsync exited with %s; falling back to SFTP.", exc)
                except FileNotFoundError:
                    logging.warning("rsync binary %s not found. Falling back to SFTP.", args.rsync_path)
            else:
                logging.info("Skipping rsync; using SFTP fallback.")

            if used_rsync:
                try:
                    transfer_single_file_with_rsync(
                        rsync_path=args.rsync_path,
                        local_path=catalog_path,
                        remote_path=remote_catalog_path,
                        user=config["user"],
                        host=config["host"],
                        port=config["port"],
                        description=CATALOG_FILENAME,
                    )
                except subprocess.CalledProcessError as exc:
                    logging.warning(
                        "rsync catalog upload failed with %s; retrying via SFTP.",
                        exc,
                    )
                    try:
                        with sftp_session(client) as sftp:
                            upload_file(
                                sftp,
                                catalog_path,
                                remote_catalog_path,
                                CATALOG_FILENAME,
                                base_dir=dest_root,
                            )
                    except RuntimeError as sftp_exc:
                        logging.error("%s", sftp_exc)
                        return 4
                try:
                    transfer_single_file_with_rsync(
                        rsync_path=args.rsync_path,
                        local_path=manifest_path,
                        remote_path=remote_manifest_path,
                        user=config["user"],
                        host=config["host"],
                        port=config["port"],
                        description="manifest.txt",
                    )
                except subprocess.CalledProcessError as exc:
                    logging.warning(
                        "rsync manifest upload failed with %s; retrying via SFTP.",
                        exc,
                    )
                    try:
                        with sftp_session(client) as sftp:
                            upload_file(
                                sftp,
                                manifest_path,
                                remote_manifest_path,
                                "manifest.txt",
                                base_dir=dest_root,
                            )
                    except RuntimeError as sftp_exc:
                        logging.error("%s", sftp_exc)
                        return 4
            else:
                try:
                    with sftp_session(client) as sftp:
                        upload_files_via_sftp(
                            sftp,
                            records,
                            remote_files_dir,
                            args.status_interval,
                            base_dir=dest_root,
                        )
                        upload_file(
                            sftp,
                            catalog_path,
                            remote_catalog_path,
                            CATALOG_FILENAME,
                            base_dir=dest_root,
                        )
                        upload_file(
                            sftp,
                            manifest_path,
                            remote_manifest_path,
                            "manifest.txt",
                            base_dir=dest_root,
                        )
                except RuntimeError as exc:
                    logging.error("%s", exc)
                    return 4

            finalize_summary: Optional[Dict[str, object]] = None
            finalize_error_count = 0
            if config.get("finalize_move", True):
                try:
                    catalog_map = load_catalog(catalog_path)
                    finalize_summary = finalize_move(
                        client,
                        dest_root=dest_root,
                        run_root=remote_root,
                        records=records,
                        catalog_map=catalog_map,
                    )
                    counts = finalize_summary.get("counts", {}) if isinstance(finalize_summary, dict) else {}
                    logging.info(
                        "Finalize complete: moved=%s, skipped_same_hash=%s, renamed=%s, errors=%s",
                        counts.get("moved", "?"),
                        counts.get("skipped_same_hash", "?"),
                        counts.get("renamed_conflict", "?"),
                        counts.get("errors", "?"),
                    )
                    finalize_error_count = int(counts.get("errors", 0) or 0)
                except Exception as exc:
                    logging.error("Finalize step failed: %s", exc)
                    return 5
            else:
                logging.info("Finalize move disabled. Files remain under %s", remote_files_dir)

            flush_logging_handlers()
            try:
                with sftp_session(client) as sftp:
                    upload_file(
                        sftp,
                        log_path,
                        remote_log_path,
                        "ingest.log",
                        base_dir=dest_root,
                    )
            except RuntimeError as exc:
                logging.error("%s", exc)
                return 4

            if finalize_error_count:
                logging.error(
                    "Finalize reported %s errors; inspect finalize.json under %s for details.",
                    finalize_error_count,
                    remote_root,
                )
                return 5

            if config["remote_hook"]:
                try:
                    trigger_remote_hook(client, config["remote_hook"], remote_root)
                except RuntimeError as exc:
                    logging.error("%s", exc)
                    return 3
        finally:
            client.close()

    print_summary(records, total_bytes, duplicates, remote_root)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.error("Interrupted by user.")
        sys.exit(130)
