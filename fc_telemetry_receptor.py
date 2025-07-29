import os
import asyncio
import logging
import time
import dataclasses
from dataclasses import dataclass
from typing import Optional
import mavsdk

ENVIRONMENT = os.getenv("ENVIRONMENT", "sitl").lower()


@dataclass
class FcTelemetryData:
    timestamp_ms: float

    latitude_deg: float = 0.0
    longitude_deg: float = 0.0
    altitude_m_amsl: float = 0.0  # Above mean sea level
    altitude_m_rel: float = 0.0  # Relative to home

    velocity_north_mps: float = 0.0
    velocity_east_mps: float = 0.0
    velocity_down_mps: float = 0.0

    # Euler attitude
    roll_deg: float = 0.0
    pitch_deg: float = 0.0
    yaw_deg: float = 0.0

    # Attitude (Quaternion)
    q_w: float = 1.0
    q_x: float = 0.0
    q_y: float = 0.0
    q_z: float = 0.0

    # Acceleration 3D
    accel_body_x_m_per_s2: float = 0.0
    accel_body_y_m_per_s2: float = 0.0
    accel_body_z_m_per_s2: float = 0.0

    # Rotation rate 3D
    angular_vel_body_x_rad_ps: float = 0.0
    angular_vel_body_y_rad_ps: float = 0.0
    angular_vel_body_z_rad_ps: float = 0.0

    # Motor and Servo outputs (PWM)
    motor_out: int = 0
    servo1_out: int = 0
    servo2_out: int = 0
    servo3_out: int = 0

    @classmethod
    def _get_field_formats(cls):
        return {
            cls.__dataclass_fields__["timestamp_ms"]: ".3f",
            cls.__dataclass_fields__["latitude_deg"]: ".8f",
            cls.__dataclass_fields__["longitude_deg"]: ".8f",
            cls.__dataclass_fields__["altitude_m_amsl"]: ".2f",
            cls.__dataclass_fields__["altitude_m_rel"]: ".2f",
            cls.__dataclass_fields__["velocity_north_mps"]: ".3f",
            cls.__dataclass_fields__["velocity_east_mps"]: ".3f",
            cls.__dataclass_fields__["velocity_down_mps"]: ".3f",
            cls.__dataclass_fields__["roll_deg"]: ".2f",
            cls.__dataclass_fields__["pitch_deg"]: ".2f",
            cls.__dataclass_fields__["yaw_deg"]: ".2f",
            cls.__dataclass_fields__["q_w"]: ".4f",
            cls.__dataclass_fields__["q_x"]: ".4f",
            cls.__dataclass_fields__["q_y"]: ".4f",
            cls.__dataclass_fields__["q_z"]: ".4f",
            cls.__dataclass_fields__["accel_body_x_m_per_s2"]: ".3f",
            cls.__dataclass_fields__["accel_body_y_m_per_s2"]: ".3f",
            cls.__dataclass_fields__["accel_body_z_m_per_s2"]: ".3f",
            cls.__dataclass_fields__["angular_vel_body_x_rad_ps"]: ".4f",
            cls.__dataclass_fields__["angular_vel_body_y_rad_ps"]: ".4f",
            cls.__dataclass_fields__["angular_vel_body_z_rad_ps"]: ".4f",
            cls.__dataclass_fields__["motor_out"]: "d",
            cls.__dataclass_fields__["servo1_out"]: "d",
            cls.__dataclass_fields__["servo2_out"]: "d",
            cls.__dataclass_fields__["servo3_out"]: "d",
        }

    def to_csv_row(self) -> str:
        values = []
        field_formats = self._get_field_formats()

        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            format_spec = field_formats.get(field)

            if format_spec:
                values.append(f"{value:{format_spec}}")
            else:
                values.append(str(value))

        return ",".join(values)

    @staticmethod
    def csv_header() -> str:
        return ",".join([field.name for field in dataclasses.fields(FcTelemetryData)])


class FcTelemetryCollector:

    _plane: mavsdk.System
    _current_data: FcTelemetryData
    _data_lock: asyncio.Lock
    _running: bool

    def __init__(self, plane: mavsdk.System):
        self._plane = plane
        self._current_data = FcTelemetryData(timestamp_ms=time.time())
        self._data_lock = asyncio.Lock()
        self._running = False

    async def configure_stream_rates(self, rate_hz: float = 30.0):
        logging.info(f"Configuring telemetry stream rates to {rate_hz} Hz")

        try:
            await self._plane.telemetry.set_rate_position(rate_hz)
            await self._plane.telemetry.set_rate_velocity_ned(rate_hz)
            await self._plane.telemetry.set_rate_attitude_euler(rate_hz)
            await self._plane.telemetry.set_rate_attitude_quaternion(rate_hz)
            await self._plane.telemetry.set_rate_imu(rate_hz)
            try:
                await self._plane.telemetry.set_rate_actuator_output_status(rate_hz)
            except Exception as e:
                if ENVIRONMENT != "sitl":
                    logging.exception("Failed to set actuator state output rate")

            logging.info(f"Successfully configured stream rates to {rate_hz} Hz")

        except Exception as e:
            logging.error(f"Failed to configure stream rates: {e}")
            raise

    async def start_telemetry_streams(self, stream_rate_hz: float = 30.0):
        self._running = True
        logging.info("Starting telemetry streams...")

        # Configure stream rates before starting streams
        await self.configure_stream_rates(stream_rate_hz)

        # Small delay to allow rate configuration to take effect
        await asyncio.sleep(0.5)

        await asyncio.gather(
            self._position_stream(),
            self._velocity_stream(),
            self._attitude_euler_stream(),
            self._attitude_quaternion_stream(),
            self._imu_stream(),
            self._actuator_output_stream(),
        )

    def stop(self):
        self._running = False

    async def get_latest_data(self) -> FcTelemetryData:
        async with self._data_lock:
            data_copy = dataclasses.replace(
                self._current_data, timestamp_ms=time.time() * 1000
            )
            return data_copy

    async def _position_stream(self):
        try:
            async for position in self._plane.telemetry.position():
                if not self._running:
                    break
                async with self._data_lock:
                    self._current_data.latitude_deg = position.latitude_deg
                    self._current_data.longitude_deg = position.longitude_deg
                    self._current_data.altitude_m_amsl = position.absolute_altitude_m
                    self._current_data.altitude_m_rel = position.relative_altitude_m
        except Exception:
            logging.exception(f"Position stream error")

    async def _velocity_stream(self):
        try:
            async for velocity in self._plane.telemetry.velocity_ned():
                if not self._running:
                    break
                async with self._data_lock:
                    self._current_data.velocity_north_mps = velocity.north_m_s
                    self._current_data.velocity_east_mps = velocity.east_m_s
                    self._current_data.velocity_down_mps = velocity.down_m_s
        except Exception:
            logging.exception(f"Velocity stream error")

    async def _attitude_euler_stream(self):
        try:
            async for attitude in self._plane.telemetry.attitude_euler():
                if not self._running:
                    break
                async with self._data_lock:
                    self._current_data.roll_deg = attitude.roll_deg
                    self._current_data.pitch_deg = attitude.pitch_deg
                    self._current_data.yaw_deg = attitude.yaw_deg
        except Exception:
            logging.exception(f"Attitude Euler stream error")

    async def _attitude_quaternion_stream(self):
        try:
            async for attitude in self._plane.telemetry.attitude_quaternion():
                if not self._running:
                    break
                async with self._data_lock:
                    self._current_data.q_w = attitude.w
                    self._current_data.q_x = attitude.x
                    self._current_data.q_y = attitude.y
                    self._current_data.q_z = attitude.z
        except Exception:
            logging.exception(f"Attitude Quaternion stream error")

    async def _imu_stream(self):
        try:
            async for imu in self._plane.telemetry.imu():
                if not self._running:
                    break
                async with self._data_lock:
                    self._current_data.accel_body_x_m_per_s2 = (
                        imu.acceleration_frd.forward_m_s2
                    )
                    self._current_data.accel_body_y_m_per_s2 = (
                        imu.acceleration_frd.right_m_s2
                    )
                    self._current_data.accel_body_z_m_per_s2 = (
                        imu.acceleration_frd.down_m_s2
                    )

                    self._current_data.angular_vel_body_x_rad_ps = (
                        imu.angular_velocity_frd.forward_rad_s
                    )
                    self._current_data.angular_vel_body_y_rad_ps = (
                        imu.angular_velocity_frd.right_rad_s
                    )
                    self._current_data.angular_vel_body_z_rad_ps = (
                        imu.angular_velocity_frd.down_rad_s
                    )
        except Exception:
            logging.exception(f"IMU stream error")

    async def _actuator_output_stream(self):
        try:
            async for output in self._plane.telemetry.actuator_output_status():
                if not self._running:
                    break
                async with self._data_lock:
                    actuator_values = output.actuator

                    self._current_data.motor_out = (
                        int(actuator_values[0]) if len(actuator_values) > 0 else 0
                    )

                    servo_start_idx = 8
                    self._current_data.servo1_out = (
                        int(actuator_values[servo_start_idx])
                        if len(actuator_values) > servo_start_idx
                        else 0
                    )
                    self._current_data.servo2_out = (
                        int(actuator_values[servo_start_idx + 1])
                        if len(actuator_values) > servo_start_idx + 1
                        else 0
                    )
                    self._current_data.servo3_out = (
                        int(actuator_values[servo_start_idx + 2])
                        if len(actuator_values) > servo_start_idx + 2
                        else 0
                    )

        except Exception:
            logging.exception(f"Actuator output stream error")


class FcTelemetryLogger:

    _collector: FcTelemetryCollector
    _filename: str
    _log_interval: float
    _running: bool
    _file_handle: Optional[open]

    def __init__(
        self, collector: FcTelemetryCollector, filename: str, log_rate_hz: float = 30.0
    ):
        self._collector = collector
        self._filename = filename
        self._log_interval = 1.0 / log_rate_hz
        self._running = False
        self._file_handle = None

    async def start_logging(self):
        self._running = True
        logging.info(
            f"Starting telemetry logging to {self._filename} at {1/self._log_interval:.1f} Hz"
        )

        try:
            self._file_handle = open(self._filename, "w", buffering=1)

            self._file_handle.write(FcTelemetryData.csv_header() + "\n")

            while self._running:
                try:
                    data = await self._collector.get_latest_data()
                    self._file_handle.write(data.to_csv_row() + "\n")
                    await asyncio.sleep(self._log_interval)
                except Exception:
                    logging.exception(f"Error logging telemetry data")
                    await asyncio.sleep(self._log_interval)

        except Exception:
            logging.exception(f"Failed to open log file {self._filename}")
        finally:
            if self._file_handle:
                self._file_handle.close()
                logging.info(f"Closed log file {self._filename}")

    def stop_logging(self):
        self._running = False
        logging.info("Stopping telemetry logging...")
