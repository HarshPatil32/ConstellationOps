import uvicorn

from constellationops.app import app
from constellationops.config import Settings


def main() -> None:
    settings = Settings.from_env()
    uvicorn.run(app, host=settings.http_host, port=settings.http_port)


if __name__ == "__main__":
    main()
