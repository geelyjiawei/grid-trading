from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
FRONTEND_APP = ROOT / "frontend" / "app.js"
OPENAPI_OUTPUT = ROOT / "contracts" / "openapi-v1.json"
FRONTEND_OUTPUT = ROOT / "contracts" / "frontend-api-v1.json"


def export_openapi() -> None:
    sys.path.insert(0, str(BACKEND))
    import main  # noqa: PLC0415

    spec = main.app.openapi()
    spec["x-grid-contract-version"] = 1
    OPENAPI_OUTPUT.write_text(
        json.dumps(spec, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def export_frontend_usage() -> None:
    source = FRONTEND_APP.read_text(encoding="utf-8")
    endpoints = sorted(
        {
            match.replace("${symbol}", "{symbol}")
            for match in re.findall(r"/api/[A-Za-z0-9_?=&${}/.-]+", source)
        }
    )
    FRONTEND_OUTPUT.write_text(
        json.dumps(
            {
                "contract_version": 1,
                "source": "frontend/app.js",
                "endpoints": endpoints,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    export_openapi()
    export_frontend_usage()
