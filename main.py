# Load the dotenv file
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

project_root = Path(__file__).parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)

import os
import asyncio
import logging
import mavsdk


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "sitl").lower()


def init_logging(log_level: str) -> None:
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level.upper())

    handler = logging.StreamHandler()
    handler.setLevel(log_level.upper())
    logger.addHandler(handler)


async def _async_main():
    plane = mavsdk.System()

    CONNECTION_CONFIGS = {
        "deployment": f"serial://{os.getenv('SERIAL_PORT_PATH', '/dev/ttyUSB0')}:{os.getenv('BAUD_RATE', '57600')}",
        "sitl": f"tcp://{os.getenv('SITL_HOST', '127.0.0.1')}:{os.getenv('SITL_PORT', '5760')}",
    }

    connection_string: Optional[str] = CONNECTION_CONFIGS.get(ENVIRONMENT, None)
    if connection_string is None:
        raise ValueError(f"Unknown environment: {ENVIRONMENT}")

    while True:
        try:
            logging.info(f"Attempting to connect to FC at {connection_string}")
            await plane.connect(system_address=connection_string)

            logging.info("Waiting for valid FC state...")
            async for state in plane.core.connection_state():
                if state.is_connected:
                    break

            logging.info("Successfully connected to FC")
            break

        except Exception as e:
            logging.warning(f"Connection failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

    while True:
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down...")
            break


def main():
    init_logging(LOG_LEVEL)
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
