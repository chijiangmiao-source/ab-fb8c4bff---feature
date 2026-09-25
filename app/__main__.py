"""开发/单机入口：python -m app

容器内默认使用 gunicorn（见 Dockerfile / compose）。
"""

from __future__ import annotations

import os

from .server import create_app


def main() -> None:
    host = os.environ.get("CALIBRATION_HOST", "0.0.0.0")
    port = int(os.environ.get("CALIBRATION_PORT", "8080"))
    app = create_app()
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
