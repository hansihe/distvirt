#!/usr/bin/env python
"""Regenerate betterproto stubs from client.proto.

Usage:
    uv run python generate_proto.py
"""
import shutil
import subprocess
import sys
from pathlib import Path

PROTO_ROOT = Path(__file__).resolve().parent.parent.parent / "distvirt-client-protocol" / "proto"
OUT_DIR = Path(__file__).resolve().parent / "distvirt" / "_proto"
PROTO_FILE = "distvirt/client/v1/client.proto"


def main() -> int:
    # Clean previous output
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)

    result = subprocess.run(
        [
            "protoc",
            f"-I{PROTO_ROOT}",
            f"--python_betterproto_out={OUT_DIR}",
            PROTO_FILE,
        ],
        check=False,
    )
    if result.returncode != 0:
        print("protoc failed", file=sys.stderr)
        return result.returncode

    print(f"Generated stubs in {OUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
