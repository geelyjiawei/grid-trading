import argparse
import difflib
import json
from pathlib import Path


def canonical(path):
    raw = path.read_bytes()
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        text = raw.decode("utf-16")
    else:
        text = raw.decode("utf-8-sig")
    value = json.loads(text)
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True).splitlines()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("expected", type=Path)
    parser.add_argument("actual", type=Path, nargs="+")
    arguments = parser.parse_args()
    expected = canonical(arguments.expected)
    failed = False
    for actual_path in arguments.actual:
        actual = canonical(actual_path)
        if actual == expected:
            continue
        failed = True
        print(
            "\n".join(
                difflib.unified_diff(
                    expected,
                    actual,
                    fromfile=str(arguments.expected),
                    tofile=str(actual_path),
                    lineterm="",
                )
            )
        )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
