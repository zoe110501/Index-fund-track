from __future__ import annotations

import argparse

import uvicorn

from .app import create_app, create_repository


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local financial hotspot terminal.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    parser.add_argument("--db", default="data/financial_hot_terminal.sqlite")
    parser.add_argument("--seed-demo", action="store_true")
    args = parser.parse_args()

    repository = create_repository(args.db)
    app = create_app(repository, seed_demo=args.seed_demo)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
