"""Archive and prune run directories.

``ArchiveStore`` is the seam for long-term storage. ``LocalArchive`` writes
``<root>/<YYYY-MM>/<session>.tar.gz`` plus a ``.sha256`` sidecar; an object
storage backend only has to implement ``put``/``exists``.

Pruning never deletes a run that is still open (``run.lock``), and by default
only deletes runs that are already archived with a matching checksum.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tarfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol


class ArchiveStore(Protocol):
    def put(self, name: str, archive: Path, sha256: str) -> str: ...

    def exists(self, name: str, sha256: str) -> bool: ...


class LocalArchive:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, name: str) -> Path:
        return self.root / name

    def put(self, name: str, archive: Path, sha256: str) -> str:
        dest = self._path(name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        shutil.copyfile(archive, tmp)
        if _sha256(tmp) != sha256:
            tmp.unlink(missing_ok=True)
            raise OSError(f"archive copy of {name} is corrupt")
        os.replace(tmp, dest)
        dest.with_name(dest.name + ".sha256").write_text(sha256 + "\n", encoding="utf-8")
        return str(dest)

    def exists(self, name: str, sha256: str) -> bool:
        sidecar = self._path(name).with_name(Path(name).name + ".sha256")
        try:
            return sidecar.read_text(encoding="utf-8").strip() == sha256 and self._path(name).exists()
        except OSError:
            return False


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class RunDirInfo:
    path: Path
    mtime: float
    bytes: int
    open: bool


def scan(runs_root: Path) -> list[RunDirInfo]:
    out = []
    for d in sorted(p for p in Path(runs_root).iterdir() if p.is_dir()):
        files = [f for f in d.rglob("*") if f.is_file() and not f.is_symlink()]
        mtime = max((f.stat().st_mtime for f in files), default=d.stat().st_mtime)
        out.append(RunDirInfo(d, mtime, sum(f.stat().st_size for f in files),
                              (d / "run.lock").exists()))
    return out


def _archive_name(info: RunDirInfo) -> str:
    month = datetime.fromtimestamp(info.mtime, UTC).strftime("%Y-%m")
    return f"{month}/{info.path.name}.tar.gz"


def archive_run(info: RunDirInfo, store: ArchiveStore, scratch: Path) -> tuple[str, str]:
    """Pack one run directory; returns ``(name, sha256)``."""
    scratch.mkdir(parents=True, exist_ok=True)
    tmp = scratch / f"{info.path.name}.tar.gz"
    with tarfile.open(tmp, "w:gz") as tar:
        tar.add(info.path, arcname=info.path.name)
    digest = _sha256(tmp)
    name = _archive_name(info)
    try:
        if not store.exists(name, digest):
            store.put(name, tmp, digest)
    finally:
        tmp.unlink(missing_ok=True)
    (info.path / "archived.sha256").write_text(f"{name} {digest}\n", encoding="utf-8")
    return name, digest


def is_archived(info: RunDirInfo, store: ArchiveStore) -> bool:
    try:
        name, digest = (info.path / "archived.sha256").read_text(encoding="utf-8").split()
    except (OSError, ValueError):
        return False
    return store.exists(name, digest)


def select_for_prune(
    runs: list[RunDirInfo], *, older_than_days: float, max_total_bytes: int | None,
) -> list[RunDirInfo]:
    """Oldest-first: everything past the age limit, then more until under the cap."""
    closed = sorted((r for r in runs if not r.open), key=lambda r: r.mtime)
    cutoff = time.time() - older_than_days * 86400
    chosen = [r for r in closed if r.mtime < cutoff]
    if max_total_bytes is not None:
        total = sum(r.bytes for r in runs) - sum(r.bytes for r in chosen)
        for r in closed:
            if total <= max_total_bytes:
                break
            if r not in chosen:
                chosen.append(r)
                total -= r.bytes
    return chosen
