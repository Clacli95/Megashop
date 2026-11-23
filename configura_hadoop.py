"""Installa i binari Hadoop 3.5.0 per Windows nella cartella del progetto."""
import hashlib
import io
from pathlib import Path, PurePosixPath
import urllib.request
import zipfile

URL = "https://github.com/zehelh/winutils/releases/download/hadoop-3.5.0/hadoop-3.5.0.zip"
SHA256 = "8b1bbfd09568aa3a52449671486a131e06162433444f84f38c3ded221484a741"


def main():
    with urllib.request.urlopen(URL, timeout=30) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise ValueError("L'impronta SHA-256 dell'archivio Hadoop non corrisponde.")

    target = Path(__file__).resolve().parent / ".hadoop" / "bin"
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        target.mkdir(parents=True, exist_ok=True)
        for name in ("hadoop.dll", "winutils.exe"):
            member, = [path for path in archive.namelist() if PurePosixPath(path).name == name]
            (target / name).write_bytes(archive.read(member))
    print(f"Binari Hadoop pronti in {target}")


if __name__ == "__main__":
    main()

