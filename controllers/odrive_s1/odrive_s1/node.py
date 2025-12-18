# controllers/odrive-s1.py
import asyncio
import json
import math
import os
import struct
import threading
import sys
import time
import traceback
from typing import Any, Dict, Optional

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from sensor_msgs.msg import JointState
from trajectory_msgs.msg import JointTrajectory


def _now_monotonic_s() -> float:
    return time.monotonic()


def _odrive_cansimple_arbitration_id(node_id: int, command_id: int) -> int:
    # CANSimple uses 11-bit standard IDs:
    #   arbitration_id = (node_id << 5) | command_id
    return (int(node_id) << 5) | (int(command_id) & 0x1F)


class _CANEncoder:
    def __init__(self) -> None:
        self.pos_estimate = 0.0
        self.vel_estimate = 0.0


class _CANMotorFOC:
    def __init__(self) -> None:
        self.Iq_measured = 0.0


class _CANMotor:
    def __init__(self, torque_constant: Optional[float] = None) -> None:
        self.torque_constant = torque_constant
        self.foc = _CANMotorFOC()


class _CANControllerConfig:
    def __init__(self) -> None:
        self.vel_limit: Optional[float] = None


class _CANController:
    def __init__(self, axis: "CANSimpleAxis") -> None:
        self._axis = axis
        self.config = _CANControllerConfig()

    @property
    def input_pos(self) -> float:
        return float(self._axis._last_command_pos_turns or 0.0)

    @input_pos.setter
    def input_pos(self, value: float) -> None:
        self._axis.set_input_pos(float(value))

    @property
    def input_vel(self) -> float:
        return float(self._axis._last_command_vel_turns_s or 0.0)

    @input_vel.setter
    def input_vel(self, value: float) -> None:
        self._axis.set_input_vel(float(value))

    @property
    def input_torque(self) -> float:
        return float(self._axis._last_command_torque_nm or 0.0)

    @input_torque.setter
    def input_torque(self, value: float) -> None:
        self._axis.set_input_torque(float(value))


class CANSimpleAxis:
    """
    Minimal CANSimple transport for a single ODrive axis/node_id.

    Requires the `python-can` package and a configured CAN interface (e.g. SocketCAN `can0`).
    """

    # CANSimple command IDs (5-bit). These defaults match ODrive CANSimple v0.x.
    # Override with `can.command_ids` if your firmware differs.
    CMD_HEARTBEAT = 0x001
    CMD_SET_AXIS_STATE = 0x007
    CMD_GET_ENCODER_ESTIMATES = 0x009
    CMD_SET_CONTROLLER_MODE = 0x00B
    CMD_SET_INPUT_POS = 0x00C
    CMD_SET_INPUT_VEL = 0x00D
    CMD_SET_INPUT_TORQUE = 0x00E
    CMD_GET_IQ = 0x014

    AXIS_STATE_CLOSED_LOOP_CONTROL = 8

    CONTROL_MODE_TORQUE_CONTROL = 1
    CONTROL_MODE_VELOCITY_CONTROL = 2
    CONTROL_MODE_POSITION_CONTROL = 3

    INPUT_MODE_PASSTHROUGH = 1

    def __init__(
        self,
        *,
        node_id: int,
        can_interface: str,
        can_channel: str,
        can_bitrate: Optional[int] = None,
        torque_constant: Optional[float] = None,
        command_ids: Optional[Dict[str, int]] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.node_id = int(node_id)
        self._logger = logger
        self.encoder = _CANEncoder()
        self.motor = _CANMotor(torque_constant=torque_constant)
        self.controller = _CANController(self)

        self._last_heartbeat_error: Optional[int] = None
        self._last_heartbeat_state: Optional[int] = None
        self._last_heartbeat_time_s: Optional[float] = None

        self._last_command_pos_turns: Optional[float] = None
        self._last_command_vel_turns_s: Optional[float] = None
        self._last_command_torque_nm: Optional[float] = None

        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        if command_ids:
            self.CMD_SET_AXIS_STATE = int(command_ids.get("set_axis_state", self.CMD_SET_AXIS_STATE))
            self.CMD_GET_ENCODER_ESTIMATES = int(command_ids.get("get_encoder_estimates", self.CMD_GET_ENCODER_ESTIMATES))
            self.CMD_SET_CONTROLLER_MODE = int(command_ids.get("set_controller_mode", self.CMD_SET_CONTROLLER_MODE))
            self.CMD_SET_INPUT_POS = int(command_ids.get("set_input_pos", self.CMD_SET_INPUT_POS))
            self.CMD_SET_INPUT_VEL = int(command_ids.get("set_input_vel", self.CMD_SET_INPUT_VEL))
            self.CMD_SET_INPUT_TORQUE = int(command_ids.get("set_input_torque", self.CMD_SET_INPUT_TORQUE))
            self.CMD_GET_IQ = int(command_ids.get("get_iq", self.CMD_GET_IQ))

        try:
            import can  # type: ignore
        except ImportError as exc:
            raise ImportError("python-can is required for CAN transport") from exc

        bus_kwargs: Dict[str, Any] = {"interface": can_interface, "channel": can_channel}
        if can_bitrate is not None:
            bus_kwargs["bitrate"] = int(can_bitrate)
        self._bus = can.Bus(**bus_kwargs)

        self._rx_thread = threading.Thread(target=self._rx_loop, name="odrive-can-rx", daemon=True)
        self._rx_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        try:
            self._rx_thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            self._bus.shutdown()
        except Exception:
            pass

    def _log_debug(self, msg: str) -> None:
        if self._logger is not None:
            try:
                self._logger.debug(msg)
            except Exception:
                pass

    def _log_warn(self, msg: str) -> None:
        if self._logger is not None:
            try:
                self._logger.warning(msg)
            except Exception:
                pass

    def _send(self, command_id: int, data: bytes = b"") -> None:
        try:
            import can  # type: ignore
        except ImportError:
            self._log_warn("python-can not available, cannot send CAN message")
            return

        arb_id = _odrive_cansimple_arbitration_id(self.node_id, command_id)
        msg = can.Message(arbitration_id=arb_id, data=data, is_extended_id=False)
        with self._lock:
            try:
                self._bus.send(msg)
            except Exception as exc:
                self._log_warn(f"Failed to send CAN message: {exc}")

    def _rx_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                msg = self._bus.recv(timeout=0.25)
            except Exception as exc:
                self._log_warn(f"CAN recv failed: {exc}")
                continue
            if msg is None:
                continue

            try:
                self._handle_message(msg.arbitration_id, bytes(msg.data))
            except Exception as exc:
                self._log_warn(f"Failed to parse CAN message: {exc}")

    def _handle_message(self, arbitration_id: int, data: bytes) -> None:
        src_node_id = (int(arbitration_id) >> 5) & 0x3F
        if src_node_id != self.node_id:
            return
        command_id = int(arbitration_id) & 0x1F

        now_s = _now_monotonic_s()
        if command_id == self.CMD_HEARTBEAT:
            if len(data) >= 5:
                error = struct.unpack_from("<I", data, 0)[0]
                state = int(data[4])
                with self._lock:
                    self._last_heartbeat_error = int(error)
                    self._last_heartbeat_state = int(state)
                    self._last_heartbeat_time_s = now_s
            return

        if command_id == self.CMD_GET_ENCODER_ESTIMATES and len(data) >= 8:
            pos_turns, vel_turns_s = struct.unpack_from("<ff", data, 0)
            if math.isfinite(pos_turns) and math.isfinite(vel_turns_s):
                with self._lock:
                    self.encoder.pos_estimate = float(pos_turns)
                    self.encoder.vel_estimate = float(vel_turns_s)
            return

        if command_id == self.CMD_GET_IQ and len(data) >= 8:
            _, iq_measured = struct.unpack_from("<ff", data, 0)
            if math.isfinite(iq_measured):
                with self._lock:
                    self.motor.foc.Iq_measured = float(iq_measured)
            return

    def last_heartbeat_age_s(self) -> Optional[float]:
        with self._lock:
            if self._last_heartbeat_time_s is None:
                return None
            return max(0.0, _now_monotonic_s() - self._last_heartbeat_time_s)

    def set_axis_state(self, requested_state: int) -> None:
        self._send(self.CMD_SET_AXIS_STATE, struct.pack("<I", int(requested_state)))

    def set_controller_mode(self, control_mode: int, input_mode: int = INPUT_MODE_PASSTHROUGH) -> None:
        self._send(self.CMD_SET_CONTROLLER_MODE, struct.pack("<II", int(control_mode), int(input_mode)))

    def request_encoder_estimates(self) -> None:
        self._send(self.CMD_GET_ENCODER_ESTIMATES, b"")

    def request_iq(self) -> None:
        self._send(self.CMD_GET_IQ, b"")

    def set_input_pos(self, pos_turns: float, vel_ff_turns_s: float = 0.0, torque_ff_nm: float = 0.0) -> None:
        vel_ff_i16 = int(max(-32768, min(32767, round(float(vel_ff_turns_s) / 0.001))))
        torque_ff_i16 = int(max(-32768, min(32767, round(float(torque_ff_nm) / 0.001))))
        payload = struct.pack("<fhh", float(pos_turns), vel_ff_i16, torque_ff_i16)
        self._send(self.CMD_SET_INPUT_POS, payload)
        with self._lock:
            self._last_command_pos_turns = float(pos_turns)

    def set_input_vel(self, vel_turns_s: float, torque_ff_nm: float = 0.0) -> None:
        payload = struct.pack("<ff", float(vel_turns_s), float(torque_ff_nm))
        self._send(self.CMD_SET_INPUT_VEL, payload)
        with self._lock:
            self._last_command_vel_turns_s = float(vel_turns_s)
            self._last_command_torque_nm = float(torque_ff_nm)

    def set_input_torque(self, torque_nm: float) -> None:
        payload = struct.pack("<f", float(torque_nm))
        self._send(self.CMD_SET_INPUT_TORQUE, payload)
        with self._lock:
            self._last_command_torque_nm = float(torque_nm)


class ODriveS1Controller(Node):
    """
    ROS2 node that bridges incoming JointTrajectory commands to an ODrive S1.
    """

    def __init__(self):
        # Parse environment-provided node metadata
        parameters = self._load_json_env("POLYFLOW_PARAMETERS", {})
        configuration = self._load_json_env("POLYFLOW_CONFIGURATION", {})
        inbound = self._load_json_env("POLYFLOW_INBOUND_CONNECTIONS", [])
        outbound = self._load_json_env("POLYFLOW_OUTBOUND_CONNECTIONS", [])
        self.node_id = os.environ.get("POLYFLOW_NODE_ID", "odrive_s1")

        super().__init__("odrive_s1_node", namespace=configuration.get("namespace", ""))
        self.get_logger().set_level(rclpy.logging.LoggingSeverity.DEBUG)

        self.parameters: Dict[str, Any] = parameters
        self.configuration: Dict[str, Any] = configuration
        self.inbound_connections = inbound
        self.outbound_connections = outbound

        self.joint_id = parameters.get("joint")
        self.control_mode = parameters.get("control_mode", "position")
        self.transport = parameters.get("transport", "usb")

        # Gear ratio: motor_turns = output_turns * gear_ratio
        gear_ratio = float(parameters.get("gear_ratio", 1.0))

        units = str(parameters.get("units", "radians")).lower().strip()
        if units in ("turn", "turns"):
            default_cmd_pos_scale = 1.0 * gear_ratio
            default_cmd_vel_scale = 1.0 * gear_ratio
            default_state_pos_scale = 1.0 / gear_ratio
            default_state_vel_scale = 1.0 / gear_ratio
        else:
            default_cmd_pos_scale = (1.0 / float(math.tau)) * gear_ratio
            default_cmd_vel_scale = (1.0 / float(math.tau)) * gear_ratio
            default_state_pos_scale = float(math.tau) / gear_ratio
            default_state_vel_scale = float(math.tau) / gear_ratio

        self.cmd_position_scale = float(parameters.get("cmd_position_scale", default_cmd_pos_scale))
        self.cmd_velocity_scale = float(parameters.get("cmd_velocity_scale", default_cmd_vel_scale))
        self.state_position_scale = float(parameters.get("state_position_scale", default_state_pos_scale))
        self.state_velocity_scale = float(parameters.get("state_velocity_scale", default_state_vel_scale))

        # CAN transport parameters (only used when `transport="can"`)
        self.can_node_id = int(parameters.get("can.node_id", 0))
        self.can_interface = str(parameters.get("can.interface", "socketcan"))
        self.can_channel = str(parameters.get("can.channel", "can0"))
        self.can_bitrate = parameters.get("can.bitrate")
        if self.can_bitrate in (None, "", 0, "0"):
            self.can_bitrate = None
        self.can_poll_hz = float(parameters.get("can.poll_hz", 50.0))
        self.can_request_iq = bool(parameters.get("can.request_iq", False))
        self.can_heartbeat_timeout_s = float(parameters.get("can.heartbeat_timeout_s", 2.0))
        self.can_enable_closed_loop_on_start = bool(parameters.get("can.enable_closed_loop_on_start", True))
        self.can_torque_constant = parameters.get("can.torque_constant")
        if self.can_torque_constant is not None:
            self.can_torque_constant = float(self.can_torque_constant)

        self.can_command_ids = parameters.get("can.command_ids")
        if self.can_command_ids is not None and not isinstance(self.can_command_ids, dict):
            self.can_command_ids = None
        self.lower_position = self._first_param(["limit.lower_position", "lower_position"])
        self.upper_position = self._first_param(["limit.upper_position", "upper_position"])
        self.position_step = self._first_param(["limit.position_step", "position_step"])
        self.max_effort = self._first_param(["limit.max_effort", "max_effort"])
        self.effort_step = self._first_param(["limit.effort_step", "effort_step"])
        self.max_velocity = self._first_param(["limit.max_velocity", "max_velocity"])
        self.velocity_step = self._first_param(["limit.velocity_step", "velocity_step"])

        # Smoothing parameters
        self.smoothing_alpha = float(parameters.get("smoothing_alpha", 0.0))  # 0 = no smoothing, 0.9 = heavy smoothing
        self._last_commanded_position: Dict[str, float] = {}
        self._last_commanded_velocity: Dict[str, float] = {}

        self.axes: Dict[str, Any] = {}

        # Use system default QoS for maximum compatibility
        from rclpy.qos import qos_profile_system_default
        qos_profile = qos_profile_system_default

        topics = self._inbound_topics()
        self.get_logger().info(f"Creating subscriptions for topics: {topics}")
        self._trajectory_subscriptions = [
            self.create_subscription(JointTrajectory, topic, self._trajectory_callback, qos_profile) for topic in topics
        ]
        self.get_logger().info(f"Successfully subscribed to {len(self._trajectory_subscriptions)} topics: {topics}")
        self.get_logger().info(f"Node namespace: '{self.get_namespace()}', QoS: system_default")
        for i, (topic, sub) in enumerate(zip(topics, self._trajectory_subscriptions)):
            self.get_logger().info(f"  Subscription {i}: topic='{topic}', resolved='{sub.topic_name}', valid={sub is not None}")

        self.joint_state_pub = self.create_publisher(JointState, "joint/state", 10)

        rate_hz = configuration.get("rate_hz", 50)
        period = 1.0 / rate_hz if rate_hz else 0.02
        self.joint_state_timer = self.create_timer(period, self._publish_joint_state)

        # Get ROS domain ID and network config
        domain_id = os.environ.get('ROS_DOMAIN_ID', 'default (0)')
        ros_localhost_only = os.environ.get('ROS_LOCALHOST_ONLY', 'not set')
        cyclone_uri = os.environ.get('CYCLONEDDS_URI', 'not set')
        fastdds_profile = os.environ.get('FASTRTPS_DEFAULT_PROFILES_FILE', 'not set')

        self.get_logger().info(
            f"ODrive S1 node starting (id={self.node_id}) for joint={self.joint_id}, "
            f"mode={self.control_mode}, ROS_DOMAIN_ID={domain_id}"
        )
        self.get_logger().info(
            f"DDS config - ROS_LOCALHOST_ONLY={ros_localhost_only}, "
            f"CYCLONEDDS_URI={cyclone_uri}, FASTRTPS_DEFAULT_PROFILES_FILE={fastdds_profile}"
        )

    @staticmethod
    def _load_json_env(key: str, default: Any) -> Any:
        raw = os.environ.get(key)
        if not raw:
            return default
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default

    def _first_param(self, keys, default=None):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            if key in self.parameters:
                return self.parameters[key]
        return default

    @staticmethod
    def _clamp(value: float, min_v: Optional[float], max_v: Optional[float]) -> float:
        if min_v is not None:
            value = max(value, float(min_v))
        if max_v is not None:
            value = min(value, float(max_v))
        return value

    @staticmethod
    def _quantize(value: float, step: Optional[float]) -> float:
        if step is None or step == 0:
            return value
        return round(value / float(step)) * float(step)

    def _inbound_topics(self):
        topics = []
        for conn in self.inbound_connections:
            if isinstance(conn, str):
                topics.append(conn)

        # Default to the WebRTC JointTrajectory bridge topic
        for default_topic in ("/robot/joint/trajectory", "webrtc"):
            if default_topic not in topics:
                topics.append(default_topic)
        return topics

    def register_axis(self, joint_id: str, axis: Any) -> None:
        self.axes[joint_id] = axis
        if self.max_velocity is not None:
            try:
                axis.controller.config.vel_limit = float(self.max_velocity)
            except Exception as exc:  # Hardware config errors should not crash the node
                self.get_logger().warning(f"Failed to set velocity limit on {joint_id}: {exc}")

    def _trajectory_callback(self, msg: JointTrajectory) -> None:
        self.get_logger().info(
            f"_trajectory_callback invoked! joint_names={msg.joint_names}, "
            f"num_points={len(msg.points)}, configured_joint_id={self.joint_id}"
        )

        if not msg.joint_names or not msg.points or not self.joint_id:
            self.get_logger().debug("Received empty JointTrajectory")
            return

        try:
            idx = msg.joint_names.index(self.joint_id)
        except ValueError:
            self.get_logger().debug(f"Joint {self.joint_id} not present in trajectory command")
            return

        self.get_logger().info(f"Received JointTrajectory for joint {self.joint_id}")
        point = msg.points[-1]
        position = None
        velocity = None
        effort = None
        if point.positions and idx < len(point.positions):
            position = point.positions[idx]
        if point.velocities and idx < len(point.velocities):
            velocity = point.velocities[idx]
        if point.effort and idx < len(point.effort):
            effort = point.effort[idx]

        self._apply_command(self.joint_id, position, velocity, effort)

    def _apply_command(
        self, joint_id: str, position: Optional[float], velocity: Optional[float], effort: Optional[float]
    ) -> None:
        axis = self.axes.get(joint_id)
        if axis is None:
            self.get_logger().warning(f"No ODrive axis registered for joint_id={joint_id}")
            return

        try:
            if self.control_mode == "velocity" and velocity is not None:
                vel_cmd = float(velocity)
                vel_cmd = self._clamp(vel_cmd, None, self.max_velocity)
                vel_cmd = self._quantize(vel_cmd, self.velocity_step)

                # Apply exponential smoothing if enabled
                if self.smoothing_alpha > 0 and joint_id in self._last_commanded_velocity:
                    vel_cmd = self.smoothing_alpha * self._last_commanded_velocity[joint_id] + (1 - self.smoothing_alpha) * vel_cmd
                self._last_commanded_velocity[joint_id] = vel_cmd

                vel_scaled = vel_cmd * float(self.cmd_velocity_scale)
                axis.controller.input_vel = vel_scaled
                self.get_logger().debug(f"Set velocity for {joint_id}: {vel_cmd} (scaled={vel_scaled})")

            elif self.control_mode == "position" and position is not None:
                pos_cmd = float(position)
                pos_cmd = self._clamp(pos_cmd, self.lower_position, self.upper_position)
                pos_cmd = self._quantize(pos_cmd, self.position_step)

                # Apply exponential smoothing if enabled
                if self.smoothing_alpha > 0 and joint_id in self._last_commanded_position:
                    pos_cmd = self.smoothing_alpha * self._last_commanded_position[joint_id] + (1 - self.smoothing_alpha) * pos_cmd
                self._last_commanded_position[joint_id] = pos_cmd

                pos_scaled = pos_cmd * float(self.cmd_position_scale)
                axis.controller.input_pos = pos_scaled
                self.get_logger().debug(f"Set position for {joint_id}: {pos_cmd} (scaled={pos_scaled})")

            elif self.control_mode == "torque" and effort is not None:
                eff_cmd = float(effort)
                eff_cmd = self._clamp(eff_cmd, None, self.max_effort)
                eff_cmd = self._quantize(eff_cmd, self.effort_step)
                axis.controller.input_torque = eff_cmd
                self.get_logger().debug(f"Set torque for {joint_id}: {eff_cmd}")
            else:
                self.get_logger().debug(f"No valid command for {joint_id} in mode={self.control_mode}")
        except Exception as exc:
            self.get_logger().error(f"Failed to apply command for {joint_id}: {exc}")

    def _publish_joint_state(self) -> None:
        if not self.axes:
            return

        msg = JointState()
        msg.header.stamp = self.get_clock().now().to_msg()

        for joint_id, axis in self.axes.items():
            try:
                pos = float(axis.encoder.pos_estimate) * float(self.state_position_scale)
                vel = float(axis.encoder.vel_estimate) * float(self.state_velocity_scale)
                torque_constant = getattr(axis.motor, "torque_constant", None)
                iq_measured = getattr(axis.motor.foc, "Iq_measured", None)
                effort = None
                if torque_constant is not None and iq_measured is not None:
                    effort = float(torque_constant) * float(iq_measured)

                msg.name.append(joint_id)
                msg.position.append(pos)
                msg.velocity.append(vel)
                msg.effort.append(effort if effort is not None else 0.0)
            except Exception as exc:
                self.get_logger().warning(f"Failed to read state for {joint_id}: {exc}")
                continue

        if msg.name:
            self.joint_state_pub.publish(msg)


async def run_odrive(node: ODriveS1Controller):
    if str(node.transport).lower().strip() == "can":
        try:
            axis = CANSimpleAxis(
                node_id=node.can_node_id,
                can_interface=node.can_interface,
                can_channel=node.can_channel,
                can_bitrate=int(node.can_bitrate) if node.can_bitrate is not None else None,
                torque_constant=node.can_torque_constant,
                command_ids=node.can_command_ids,
                logger=node.get_logger(),
            )
        except ImportError:
            node.get_logger().error("CAN transport selected but `python-can` is not installed")
            while rclpy.ok():
                await asyncio.sleep(1.0)
            return
        except Exception as exc:
            node.get_logger().error(f"Failed to open CAN interface {node.can_interface}:{node.can_channel}: {exc}")
            return

        if not node.joint_id:
            node.get_logger().error("No joint_id configured; cannot register ODrive axis")
            axis.shutdown()
            return

        node.register_axis(node.joint_id, axis)
        node.get_logger().info(
            f"Connected to ODrive CANSimple (node_id={node.can_node_id}, {node.can_interface}:{node.can_channel}), bitrate={node.can_bitrate}"
        )

        if node.control_mode == "velocity":
            axis.set_controller_mode(axis.CONTROL_MODE_VELOCITY_CONTROL, axis.INPUT_MODE_PASSTHROUGH)
        elif node.control_mode == "torque":
            axis.set_controller_mode(axis.CONTROL_MODE_TORQUE_CONTROL, axis.INPUT_MODE_PASSTHROUGH)
        else:
            axis.set_controller_mode(axis.CONTROL_MODE_POSITION_CONTROL, axis.INPUT_MODE_PASSTHROUGH)

        heartbeat_deadline_s = _now_monotonic_s() + float(node.can_heartbeat_timeout_s)
        while rclpy.ok():
            age_s = axis.last_heartbeat_age_s()
            if age_s is not None and age_s <= float(node.can_heartbeat_timeout_s):
                break
            if _now_monotonic_s() >= heartbeat_deadline_s:
                node.get_logger().warning(
                    "No ODrive heartbeat received yet; verify CAN wiring/bitrate/node_id and that ODrive CAN is enabled"
                )
                heartbeat_deadline_s = _now_monotonic_s() + float(node.can_heartbeat_timeout_s)
            await asyncio.sleep(0.1)

        if node.can_enable_closed_loop_on_start:
            node.get_logger().info(f"Enabling closed-loop control on ODrive (node_id={node.can_node_id})")
            axis.set_axis_state(axis.AXIS_STATE_CLOSED_LOOP_CONTROL)
            node.get_logger().info("Closed-loop control command sent")

        poll_period = 1.0 / float(node.can_poll_hz) if node.can_poll_hz else 0.02
        try:
            while rclpy.ok():
                axis.request_encoder_estimates()
                if node.can_request_iq:
                    axis.request_iq()
                await asyncio.sleep(poll_period)
        finally:
            axis.shutdown()
        return

    try:
        import odrive  # type: ignore
        from odrive.enums import AxisState, ControlMode, InputMode  # type: ignore
    except ImportError:
        node.get_logger().error("The 'odrive' python package is not installed; cannot control hardware")
        # Keep the coroutine alive so ROS subscriptions continue working
        while rclpy.ok():
            await asyncio.sleep(1.0)
        return

    loop = asyncio.get_event_loop()
    node.get_logger().info("Searching for any ODrive S1...")
    try:
        odrv = await loop.run_in_executor(None, odrive.find_any)
    except Exception as exc:
        node.get_logger().error(f"Failed to find ODrive device: {exc}")
        return

    if odrv is None:
        node.get_logger().error("No ODrive device found")
        return

    node.get_logger().info(f"Connected to ODrive: {odrv.serial_number}")

    if not node.joint_id:
        node.get_logger().error("No joint_id configured; cannot register ODrive axis")
        return

    available_axes = [getattr(odrv, f"axis{i}") for i in range(2) if hasattr(odrv, f"axis{i}")]
    if not available_axes:
        node.get_logger().error("No axes found on connected ODrive")
        return

    axis = available_axes[0]
    node.register_axis(node.joint_id, axis)
    # Configure control mode and enable closed-loop
    if node.control_mode == "velocity":
        axis.controller.config.control_mode = ControlMode.VELOCITY_CONTROL
        axis.controller.config.input_mode = InputMode.PASSTHROUGH
    elif node.control_mode == "torque":
        axis.controller.config.control_mode = ControlMode.TORQUE_CONTROL
        axis.controller.config.input_mode = InputMode.PASSTHROUGH
    else:
        axis.controller.config.control_mode = ControlMode.POSITION_CONTROL
        axis.controller.config.input_mode = InputMode.PASSTHROUGH
    axis.requested_state = AxisState.CLOSED_LOOP_CONTROL
    node.get_logger().info(f"Joint {node.joint_id} registered to axis0 in mode={node.control_mode}")

    while rclpy.ok():
        await asyncio.sleep(0.05)


def main(args=None):
    try:
        print("NODE AMENT_PREFIX_PATH:", os.environ.get("AMENT_PREFIX_PATH", "<none>"), flush=True)
        rclpy.init(args=args)
        node = ODriveS1Controller()

        # Spin ROS in the background so subscriptions/timers actually run
        executor = SingleThreadedExecutor()
        executor.add_node(node)
        ros_thread = threading.Thread(target=executor.spin, daemon=True)
        ros_thread.start()
        node.get_logger().info("ROS executor started (background thread)")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            node.get_logger().info("Attempting to run ODrive S1 Controller…")
            loop.run_until_complete(run_odrive(node))
        except KeyboardInterrupt:
            node.get_logger().info("Keyboard interrupt received")
        except Exception as e:
            node.get_logger().error(f"ODrive loop crashed: {e}")
        finally:
            node.get_logger().info("Shutting down")
            executor.shutdown()
            loop.stop()
            loop.close()
            node.destroy_node()
            rclpy.shutdown()
    except Exception as e:
        print("=== ODRIVE_S1 NODE CRASH ===", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
