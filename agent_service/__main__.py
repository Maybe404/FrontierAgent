"""Run the service: ``python -m agent_service [--host H] [--port P]``."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(prog="python -m agent_service")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8800)
    p.add_argument("--env-file", default=".env", help="loaded before start; workers inherit it")
    a = p.parse_args()

    from dotenv import load_dotenv

    if Path(a.env_file).exists():
        load_dotenv(a.env_file, override=False)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    import uvicorn

    from agent_service.api import create_app

    uvicorn.run(create_app(), host=a.host, port=a.port, log_level="info")


if __name__ == "__main__":
    main()
