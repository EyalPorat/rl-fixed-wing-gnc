from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load the dotenv file
project_root = Path(__file__).parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)

import os
import asyncio
import logging
import mavsdk
from datetime import datetime
from fc_telemetry_receptor import FcTelemetryCollector, FcTelemetryLogger


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "sitl").lower()
LOG_DIR = Path(os.getenv("LOG_DIR", "logs/"))


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


def create_logfile_path(base_filename: str) -> str:
    LOG_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    path = Path(base_filename)
    name_without_ext = path.stem
    extension = path.suffix

    return str(LOG_DIR / f"{name_without_ext}_{timestamp}{extension}")


async def connect_to_flight_controller() -> mavsdk.System:
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
            return plane

        except Exception as e:
            logging.warning(f"Connection failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


async def start_telemetry_reception(collector: FcTelemetryCollector):
    logging.info("Starting telemetry reception...")
    await collector.start_telemetry_streams()


async def start_logging(
    collector: FcTelemetryCollector, filename: str, log_rate_hz: float = 10.0
):
    logger = FcTelemetryLogger(collector, filename, log_rate_hz)
    await logger.start_logging()


async def print_telemetry_data(
    collector: FcTelemetryCollector, print_rate_hz: float = 1.0
):
    print_interval = 1.0 / print_rate_hz

    while True:
        try:
            data = await collector.get_latest_data()

            logging.debug(f"\n=== Telemetry Data (t={data.timestamp_ms:.2f}) ===")
            logging.debug(
                f"Position: Lat={data.latitude_deg:.6f}°, Lon={data.longitude_deg:.6f}°, Alt={data.altitude_m_rel:.2f}m"
            )
            logging.debug(
                f"Velocity: N={data.velocity_north_mps:.2f}, E={data.velocity_east_mps:.2f}, D={data.velocity_down_mps:.2f} m/s"
            )
            logging.debug(
                f"Attitude: Roll={data.roll_deg:.1f}°, Pitch={data.pitch_deg:.1f}°, Yaw={data.yaw_deg:.1f}°"
            )
            logging.debug(
                f"Quaternion: w={data.q_w:.3f}, x={data.q_x:.3f}, y={data.q_y:.3f}, z={data.q_z:.3f}"
            )
            logging.debug(
                f"Acceleration: X={data.accel_body_x_m_per_s2:.2f}, Y={data.accel_body_y_m_per_s2:.2f}, Z={data.accel_body_z_m_per_s2:.2f} m/s²"
            )
            logging.debug(
                f"Angular Vel: X={data.angular_vel_body_x_rad_ps:.3f}, Y={data.angular_vel_body_y_rad_ps:.3f}, Z={data.angular_vel_body_z_rad_ps:.3f} rad/s"
            )
            logging.debug(f"Motor: {data.motor_out} µs")
            logging.debug(
                f"Servos: [{data.servo1_out}, {data.servo2_out}, {data.servo3_out}] µs"
            )

            await asyncio.sleep(print_interval)

        except KeyboardInterrupt:
            break
        except Exception:
            logging.exception(f"Error printing telemetry")
            await asyncio.sleep(print_interval)


async def _async_main():
    plane = await connect_to_flight_controller()

    collector = FcTelemetryCollector(plane)

    log_filename = create_logfile_path(
        os.getenv("LOG_BASE_FILENAME", "fc_telemetry_log.csv")
    )
    log_rate = float(os.getenv("CSV_LOG_RATE_HZ", "10.0"))
    print_rate = float(os.getenv("STREAM_LOG_RATE_HZ", "1.0"))

    tasks = [
        start_telemetry_reception(collector),
    ]

    logging.info(f"CSV logging enabled: {log_filename} at {log_rate} Hz")
    tasks.append(start_logging(collector, log_filename, log_rate))

    logging.info(f"Stream logging output enabled at {print_rate} Hz")
    tasks.append(print_telemetry_data(collector, print_rate))

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
        collector.stop()


def main():
    init_logging(LOG_LEVEL)

    logging.info(f"Environment: {ENVIRONMENT}")
    logging.info(f"Log level: {LOG_LEVEL}")

    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        logging.info("Application terminated by keyboard interrupt")


if __name__ == "__main__":
    main()
