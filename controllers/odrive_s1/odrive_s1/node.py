# controllers/odrive-s1.py
import asyncio
import json
import os
import threading
from typing import Any, Dict, Optional

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from sensor_msgs.msg import JointState
from trajectory_msgs.msg import JointTrajectory


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

        self.joint_id = parameters.get("joint") or (joint if isinstance(joint_ids, list) and joint_ids else None)
        self.control_mode = parameters.get("control_mode", "position")
        self.lower_position = self._first_param(["limit.lower_position", "lower_position"])
        self.upper_position = self._first_param(["limit.upper_position", "upper_position"])
        self.position_step = self._first_param(["limit.position_step", "position_step"])
        self.max_effort = self._first_param(["limit.max_effort", "max_effort"])
        self.effort_step = self._first_param(["limit.effort_step", "effort_step"])
        self.max_velocity = self._first_param(["limit.max_velocity", "max_velocity"])
        self.velocity_step = self._first_param(["limit.velocity_step", "velocity_step"])
        self.axes: Dict[str, Any] = {}

        qos_depth = 10
        topics = self._inbound_topics()
        self.subscriptions = [
            self.create_subscription(JointTrajectory, topic, self._trajectory_callback, qos_depth) for topic in topics
        ]
        self.get_logger().info(f"Subscribing to inbound topics: {topics}")
        self.joint_state_pub = self.create_publisher(JointState, "joint/state", qos_depth)

        rate_hz = configuration.get("rate_hz", 50)
        period = 1.0 / rate_hz if rate_hz else 0.02
        self.joint_state_timer = self.create_timer(period, self._publish_joint_state)

        self.get_logger().info(
            f"ODrive S1 node starting (id={self.node_id}) for joint={self.joint_id}, mode={self.control_mode}"
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
        
        topics.append("webrtc")
        return topics

    def register_axis(self, joint_id: str, axis: Any) -> None:
        self.axes[joint_id] = axis
        if self.max_velocity is not None:
            try:
                axis.controller.config.vel_limit = float(self.max_velocity)
            except Exception as exc:  # Hardware config errors should not crash the node
                self.get_logger().warning(f"Failed to set velocity limit on {joint_id}: {exc}")

    def _trajectory_callback(self, msg: JointTrajectory) -> None:
        if not msg.joint_names or not msg.points or not self.joint_id:
            self.get_logger().debug("Received empty JointTrajectory")
            return

        try:
            idx = msg.joint_names.index(self.joint_id)
        except ValueError:
            self.get_logger().debug(f"Joint {self.joint_id} not present in trajectory command")
            return

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
                axis.controller.input_vel = vel_cmd
                self.get_logger().debug(f"Set velocity for {joint_id}: {vel_cmd}")
            elif self.control_mode == "position" and position is not None:
                pos_cmd = float(position)
                pos_cmd = self._clamp(pos_cmd, self.lower_position, self.upper_position)
                pos_cmd = self._quantize(pos_cmd, self.position_step)
                axis.controller.input_pos = pos_cmd
                self.get_logger().debug(f"Set position for {joint_id}: {pos_cmd}")
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
                pos = float(axis.encoder.pos_estimate)
                vel = float(axis.encoder.vel_estimate)
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


if __name__ == "__main__":
    main()
