#!/usr/bin/env python3
"""Strict busy-only native-result exception for PR897 ruling 2.

Never change native valid/exit status. Only the measurement harness uses this
predicate; all other invalidity causes and non-busy status-8 errors stay fatal.
"""
import json
from pathlib import Path
import re
import sys

BUSY = re.compile(r"(?:(?:put|txn|range|range-stream|list|delete|lease-grant|lease-revoke|lease-keepalive|lease-ttl|compact|watch|watch-progress|progress): )?grpc ResourceExhausted: etcdserver: too many requests\Z")


def busy_only(data):
    errors = data.get("errors")
    return (data.get("valid") is False
            and data.get("invalid_causes") == ["request_errors"]
            and data.get("unsent") == 0
            and isinstance(errors, dict) and bool(errors)
            and all(isinstance(k, str) and BUSY.fullmatch(k)
                    and isinstance(v, int) and not isinstance(v, bool) and v > 0
                    for k, v in errors.items()))


if __name__ == "__main__":
    try:
        accepted = busy_only(json.loads(Path(sys.argv[1]).read_text()))
    except (OSError, ValueError, IndexError, TypeError):
        accepted = False
    raise SystemExit(0 if accepted else 1)
