# ingest

Ingest an external disk and push its contents into a Synology NAS target via SSH/SFTP or rsync.

## Prerequisites

- Python 3.10+
- SSH access (key or password) to the Synology NAS
- `rsync` on the local machine for the fast path (optional; SFTP fallback is automatic)
- Synology: enable SFTP in DSM (`Control Panel -> File Services -> FTP -> Enable SFTP`) or install rsync ≥ 3.0 on your Mac for the best experience

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and adjust it to match your NAS:

```bash
cp .env.example .env
```

Required variables:

- `NAS_HOST` – NAS IP or hostname
- `NAS_USER` – SSH user
- `NAS_DEST` – Destination folder (e.g. `/volume1/BigSave`)

Optional:

- `SSH_PORT` – SSH port (default 22)
- `NAS_PASSWORD` – SSH password if you do not use keys
- `REMOTE_HOOK` – Command to run on the NAS after upload (receives the ingest folder path)
- `FINALIZE_MOVE` – `true` by default. When enabled, files are moved out of `.ingest/<run>` into the root destination and only metadata remains in `.ingest`.
- `RUN_TIMESTAMPED` – `true` by default. Set to `false` to reuse a static ingest folder and avoid timestamped directories.
- `RUN_SUBDIR` – Optional custom name for the ingest run folder (defaults to the drive label).

You can also provide these values via CLI flags.

## Usage

Auto-detect the most recently mounted external drive:

```bash
python ingest.py
```

Explicit source and NAS details:

```bash
python ingest.py \
  --source "/Volumes/MyDisk" \
  --nas-host "192.168.1.25" \
  --nas-user "ingest" \
  --nas-dest "/volume1/BigSave"
```

Optional flags:

- `--label NAME` to override the drive label used in the remote path.
- `--remote-hook "/volume1/BigSave/.ingest/hooks/merge.sh"` to trigger a script after upload.
- `--dry-run` to build hashes and the catalog without transferring files.
- `--threads 4` to limit hashing parallelism.
- `--status-interval 5` to emit progress logs to the console every 5 seconds.
- `--single-file relative/path/to/file` to try an ingest with just one file.
- `--skip-rsync` to force the SFTP path (handy when you only have password auth).
- `--no-timestamp` to reuse the same ingest folder on the NAS instead of creating a fresh timestamped run.
- `--run-subdir my_folder` to override the ingest folder name (pairs nicely with `--no-timestamp`).
- `--rsync-path /opt/homebrew/bin/rsync` to point to a custom rsync binary.

The script writes a detailed log to `ingest.log` in the current directory.

With `FINALIZE_MOVE` enabled (default), each run uploads into `.ingest/<run_id>/files/` and then, on the NAS, moves the payload into the root destination (skipping duplicates, renaming conflicts, and leaving metadata + `finalize.json` under `.ingest/<run_id>`).

## Ignore rules

Place a `.ingestignore` file at the root of the source drive to skip files or folders using glob patterns. Default ignores include system artefacts like `.DS_Store`, `Thumbs.db`, and `System Volume Information`.
