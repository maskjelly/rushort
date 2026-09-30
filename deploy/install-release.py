#!/usr/bin/env python3
"""Install a verified Linux binary with a consistent DB backup and health rollback."""
import argparse
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.request


def install(source, target):
    fd, name = tempfile.mkstemp(prefix='.rushort-release-', dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as output, source.open('rb') as input_file:
            shutil.copyfileobj(input_file, output)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(name, 0o755)
        os.replace(name, target)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def healthy(url):
    for _ in range(40):
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(.5)
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path)
    parser.add_argument('--target', type=Path, default=Path('/usr/local/bin/shortener'))
    parser.add_argument('--database', type=Path, default=Path('/var/lib/rushort/urls.db'))
    parser.add_argument('--backups', type=Path, default=Path('/var/backups/rushort'))
    parser.add_argument('--service', default='rushort')
    parser.add_argument('--health-url', default='http://127.0.0.1:8080/health')
    args = parser.parse_args()
    if os.name != 'posix' or os.geteuid() != 0:
        parser.error('Run with sudo on the Linux deployment host')
    if not args.binary.is_file() or not args.target.is_file():
        parser.error('Both the new binary and existing target must exist')
    if args.binary.resolve() == args.target.resolve():
        parser.error('New binary must be a separate file')
    subprocess.run([str(args.binary.resolve()), '--help'], check=True, stdout=subprocess.DEVNULL)
    os.umask(0o077)
    backup = args.backups / time.strftime('%Y%m%d-%H%M%S')
    backup.mkdir(parents=True, mode=0o700)
    previous = backup / 'shortener'
    shutil.copy2(args.target, previous)
    if args.database.exists():
        with sqlite3.connect(args.database.resolve().as_uri() + '?mode=ro', uri=True) as source:
            with sqlite3.connect(backup / 'urls.db') as destination:
                source.backup(destination)
                if destination.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                    raise RuntimeError('Backup integrity check failed; release not installed')
    install(args.binary, args.target)
    try:
        subprocess.run(['systemctl', 'restart', args.service], check=True)
        if not healthy(args.health_url):
            raise RuntimeError('New release failed health check')
    except Exception:
        install(previous, args.target)
        subprocess.run(['systemctl', 'restart', args.service], check=True)
        if not healthy(args.health_url):
            raise RuntimeError('Rollback also failed health check; inspect journalctl')
        raise
    print('Installed and healthy. Previous binary and consistent SQLite backup: ' + str(backup))


if __name__ == '__main__':
    main()
