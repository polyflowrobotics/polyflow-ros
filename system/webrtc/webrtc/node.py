"""WebRTC Bridge Node for ROS2.

This module provides a WebRTC bridge that connects ROS2 topics to remote clients
via WebRTC data channels, using Socket.IO for signaling.
"""

import asyncio
import json
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import rclpy
import socketio
from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription
from aiortc.rtcdatachannel import RTCDataChannel
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from rclpy.logging import LoggingSeverity
from std_msgs.msg import Float32


# ============================================================================
# Helper Classes
# ============================================================================


class ICECandidateShim:
    """Shim class to parse and represent ICE candidate data.

    This class parses an ICE candidate string and provides the necessary
    attributes for aiortc's addIceCandidate method.
    """

    __slots__ = (
        "candidate", "sdpMid", "sdpMLineIndex", "foundation", "component",
        "priority", "protocol", "ip", "port", "type", "tcpType",
        "relatedAddress", "relatedPort"
    )

    def __init__(self, candidate: str, mid: Optional[str], index: Optional[int]):
        self.candidate = candidate
        self.sdpMid = mid
        self.sdpMLineIndex = index
        self.foundation = None
        self.component = None
        self.priority = None
        self.protocol = None
        self.ip = None
        self.port = None
        self.type = None
        self.tcpType = None
        self.relatedAddress = None
        self.relatedPort = None
        self._parse_candidate(candidate)

    def _parse_candidate(self, value: str) -> None:
        """Parse ICE candidate string and extract fields."""
        try:
            parts = value.split()
            if not parts:
                return

            # Parse foundation
            foundation_part = parts[0]
            self.foundation = (
                foundation_part.split(":", 1)[1]
                if foundation_part.startswith("candidate:")
                else foundation_part
            )

            # Parse fixed-position fields
            if len(parts) > 1:
                self.component = int(parts[1])
            if len(parts) > 2:
                self.protocol = parts[2].lower()
            if len(parts) > 3:
                self.priority = int(parts[3])
            if len(parts) > 4:
                self.ip = parts[4]
            if len(parts) > 5:
                self.port = int(parts[5])

            # Parse variable key-value pairs
            i = 6
            while i < len(parts):
                label = parts[i]
                if label == "typ" and i + 1 < len(parts):
                    self.type = parts[i + 1]
                    i += 2
                elif label == "tcptype" and i + 1 < len(parts):
                    self.tcpType = parts[i + 1]
                    i += 2
                elif label == "raddr" and i + 1 < len(parts):
                    self.relatedAddress = parts[i + 1]
                    i += 2
                elif label == "rport" and i + 1 < len(parts):
                    try:
                        self.relatedPort = int(parts[i + 1])
                    except ValueError:
                        self.relatedPort = None
                    i += 2
                else:
                    i += 1
        except Exception:
            # Leave parsed fields as None
            pass


@dataclass
class SocketIOConfig:
    """Configuration for Socket.IO connection."""

    base_url: str
    namespace: str
    socketio_path: str
    connect_url: str


# ============================================================================
# Helper Functions
# ============================================================================


def parse_ice_servers(
    raw_servers: any,
    username: str = "",
    password: str = ""
) -> list[RTCIceServer]:
    """Parse ICE server configuration from various input formats.

    Args:
        raw_servers: ICE server configuration (string, list, or JSON)
        username: Optional username for TURN servers
        password: Optional password for TURN servers

    Returns:
        List of RTCIceServer objects
    """
    # Handle string input (JSON or comma-separated)
    if isinstance(raw_servers, str):
        try:
            parsed = json.loads(raw_servers)
            if isinstance(parsed, str):
                server_list = [parsed]
            elif isinstance(parsed, (list, tuple)):
                server_list = [str(s) for s in parsed if str(s).strip()]
            else:
                server_list = [str(parsed)]
        except json.JSONDecodeError:
            # Try parsing as comma-separated list
            cleaned = raw_servers.strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1]
            server_list = [
                s.strip().strip(" '\"")
                for s in cleaned.split(",")
                if s.strip().strip(" '\"")
            ]
    # Handle list/tuple input
    elif isinstance(raw_servers, (list, tuple)):
        server_list = [str(s) for s in raw_servers if str(s).strip()]
    else:
        server_list = []

    # Build RTCIceServer objects
    ice_servers = []
    for entry in server_list:
        entry = entry.strip()
        if entry:
            ice_servers.append(
                RTCIceServer(
                    urls=[entry],
                    username=username or None,
                    credential=password or None,
                )
            )

    return ice_servers


def parse_socketio_config(
    signaling_url: str,
    auth_token: str = "",
    socketio_namespace: str = "",
    socketio_path: str = ""
) -> SocketIOConfig:
    """Parse signaling URL and build Socket.IO configuration.

    Args:
        signaling_url: WebSocket URL for signaling server
        auth_token: Optional authentication token
        socketio_namespace: Optional Socket.IO namespace
        socketio_path: Optional Socket.IO path

    Returns:
        SocketIOConfig with parsed connection details
    """
    parsed = urlparse(signaling_url)

    # Convert ws:// to http:// for Socket.IO client
    scheme_map = {"ws": "http", "wss": "https"}
    scheme = scheme_map.get(parsed.scheme, parsed.scheme or "http")

    if not parsed.netloc:
        raise ValueError("signaling_url must include a host (e.g. ws://host:port/path)")

    base_url = urlunparse((scheme, parsed.netloc, "", "", "", ""))

    # Determine namespace
    namespace_config = socketio_namespace.strip()
    if namespace_config:
        namespace = namespace_config if namespace_config.startswith("/") else f"/{namespace_config}"
    else:
        namespace = "/"

    # Determine Socket.IO path
    raw_path = parsed.path or ""
    path_config = socketio_path.strip()
    if path_config:
        path = path_config.lstrip("/")
    elif raw_path and raw_path != "/":
        path = raw_path.lstrip("/")
    else:
        path = "socket.io"

    # Build connect URL with query parameters
    query_pairs = list(parse_qsl(parsed.query, keep_blank_values=True))
    if auth_token and not any(key == "token" for key, _ in query_pairs):
        query_pairs.append(("token", auth_token))

    query_string = urlencode(query_pairs)
    connect_url = f"{base_url}?{query_string}" if query_string else base_url

    return SocketIOConfig(
        base_url=base_url,
        namespace=namespace,
        socketio_path=path,
        connect_url=connect_url
    )


# ============================================================================
# Peer Connection Manager
# ============================================================================


class PeerConnectionManager:
    """Manages WebRTC peer connection lifecycle and event handlers."""

    def __init__(
        self,
        config: Optional[RTCConfiguration],
        robot_id: str,
        logger,
        on_control_message: Callable[[str], None],
        emit_signaling_message: Callable[[dict], asyncio.Future]
    ):
        self.config = config
        self.robot_id = robot_id
        self.logger = logger
        self.on_control_message = on_control_message
        self.emit_signaling_message = emit_signaling_message

        self.pc: Optional[RTCPeerConnection] = None
        self.state_channel: Optional[RTCDataChannel] = None

    def create(self) -> RTCPeerConnection:
        """Create a new peer connection with all event handlers."""
        self.pc = RTCPeerConnection(configuration=self.config)
        self.state_channel = None

        @self.pc.on("datachannel")
        def on_datachannel(channel: RTCDataChannel):
            self.logger.info(f"DataChannel opened: {channel.label}")

            @channel.on("close")
            def on_close():
                self.logger.info(f"DataChannel closed: {channel.label}")
                if self.state_channel is channel:
                    self.state_channel = None

            if channel.label == "control":
                @channel.on("message")
                def on_message(message):
                    if isinstance(message, bytes):
                        message = message.decode("utf-8", "ignore")
                    self.on_control_message(message)
            elif channel.label == "state":
                self.state_channel = channel

        @self.pc.on("icecandidate")
        def on_icecandidate(candidate):
            self.logger.debug(f"ICE candidate event: {candidate}")
            asyncio.ensure_future(self._send_ice_candidate(candidate))

        @self.pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = self.pc.connectionState
            self.logger.info(f"Peer connection state changed: {state}")
            if state in ("failed", "closed"):
                await self.reset(f"connection state {state}")

        @self.pc.on("iceconnectionstatechange")
        async def on_iceconnectionstatechange():
            state = self.pc.iceConnectionState
            self.logger.info(f"ICE connection state changed: {state}")
            if state in ("failed", "closed"):
                await self.reset(f"ICE state {state}")

        return self.pc

    async def _send_ice_candidate(self, candidate) -> None:
        """Send ICE candidate via signaling."""
        if candidate is None:
            payload = {
                "type": "candidate",
                "robotId": self.robot_id,
                "candidate": None,
            }
            self.logger.debug("Sent end-of-candidates marker")
        else:
            payload = {
                "type": "candidate",
                "robotId": self.robot_id,
                "candidate": {
                    "candidate": candidate.to_sdp(),
                    "sdpMid": candidate.sdpMid,
                    "sdpMLineIndex": candidate.sdpMLineIndex,
                },
            }
            self.logger.debug(f"Sent ICE candidate: {payload['candidate']}")

        await self.emit_signaling_message(payload)

    async def reset(self, reason: str) -> None:
        """Reset the peer connection."""
        if self.pc is None:
            return

        self.logger.info(f"Resetting peer connection ({reason}); awaiting new offers")
        self.state_channel = None

        try:
            if self.pc.connectionState != "closed":
                await self.pc.close()
        except Exception as exc:
            self.logger.warn(f"Error while closing peer connection: {exc}")

        self.create()

    async def handle_offer(self, sdp: str, from_id: Optional[str] = None) -> None:
        """Handle incoming offer and create answer."""
        if self.pc is None:
            self.logger.error("No peer connection available")
            return

        # Check signaling state
        signaling_state = self.pc.signalingState
        if signaling_state not in ("stable", "have-remote-offer"):
            self.logger.info(
                f"Peer connection in state '{signaling_state}' before new offer; resetting"
            )
            await self.reset(f"signaling state {signaling_state}")
            if self.pc is None:
                self.logger.error("Peer connection unavailable after reset")
                return

        try:
            await self.pc.setRemoteDescription(
                RTCSessionDescription(sdp=sdp, type="offer")
            )
        except Exception as exc:
            self.logger.error(f"Failed to set remote description: {exc}")
            return

        try:
            answer = await self.pc.createAnswer()
            await self.pc.setLocalDescription(answer)
        except Exception as exc:
            self.logger.error(f"Failed to create local answer: {exc}")
            return

        response = {
            "type": "answer",
            "robotId": self.robot_id,
            "sdp": self.pc.localDescription.sdp,
            "to": from_id,
        }
        await self.emit_signaling_message(response)

    async def handle_ice_candidate(
        self,
        candidate_data: any,
        sdp_mid: Optional[str] = None,
        sdp_mline_index: Optional[int] = None
    ) -> None:
        """Handle incoming ICE candidate."""
        if self.pc is None:
            self.logger.warn("No active peer connection to receive ICE candidate")
            return

        # Handle end-of-candidates marker
        if candidate_data in (None, "null"):
            self.logger.debug("Received end-of-candidates marker")
            try:
                await self.pc.addIceCandidate(None)
                self.logger.debug("Signaled end-of-candidates to peer connection")
            except Exception as exc:
                self.logger.warn(f"Failed to signal end-of-candidates: {exc}")
            return

        # Extract candidate fields
        if isinstance(candidate_data, dict):
            candidate_str = candidate_data.get("candidate")
            mid = candidate_data.get("sdpMid", sdp_mid)
            index = candidate_data.get("sdpMLineIndex", sdp_mline_index)
        elif isinstance(candidate_data, str):
            candidate_str = candidate_data
            mid = sdp_mid
            index = sdp_mline_index
        else:
            self.logger.warn("Ignoring ICE candidate with unexpected payload type")
            return

        if not candidate_str:
            self.logger.debug("ICE candidate payload missing 'candidate' data")
            return

        try:
            await self.pc.addIceCandidate(ICECandidateShim(candidate_str, mid, index))
            self.logger.debug(f"Added ICE candidate (mid={mid}, mline={index}): {candidate_str}")
        except Exception as exc:
            self.logger.warn(f"Failed to add ICE candidate: {exc}")


# ============================================================================
# Signaling Client
# ============================================================================


class SignalingClient:
    """Manages Socket.IO signaling connection."""

    def __init__(
        self,
        config: SocketIOConfig,
        robot_id: str,
        auth_token: str,
        logger,
        peer_manager: PeerConnectionManager
    ):
        self.config = config
        self.robot_id = robot_id
        self.auth_token = auth_token
        self.logger = logger
        self.peer_manager = peer_manager

        self.sio = socketio.AsyncClient(reconnection=True)
        self.pending_messages: list[dict] = []

        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Setup Socket.IO event handlers."""

        @self.sio.event
        async def connect():
            self.logger.info("Connected to signaling server")
            # Reset peer connection on reconnect
            await self.peer_manager.reset("signaling connect/reconnect")

            hello = {"type": "hello", "role": "robot", "robotId": self.robot_id}
            if self.auth_token:
                hello["token"] = self.auth_token

            try:
                await self._emit_now(hello)
            except Exception as exc:
                self.logger.error(f"Failed to emit hello during connect: {exc}")
                self.pending_messages.insert(0, hello)

            await self._flush_pending()

        @self.sio.event
        async def connect_error(data):
            self.logger.error(f"Socket.IO connection failed: {data}")

        @self.sio.event
        async def disconnect():
            self.logger.warn("Disconnected from signaling server")
            await self.peer_manager.reset("Socket.IO disconnect")

        @self.sio.on("message", namespace=self.config.namespace)
        async def on_message(data):
            await self._handle_signaling_message(data)

    async def _emit_now(self, payload: dict) -> None:
        """Emit a message immediately."""
        msg_type = payload.get('type', '<unknown>')
        self.logger.debug(f"Emitting signaling message: {msg_type} -> {payload}")
        await self.sio.emit("message", payload, namespace=self.config.namespace)

    async def emit_message(self, payload: dict) -> None:
        """Queue or send a signaling message."""
        if not self.sio.connected:
            msg_type = payload.get('type', '<unknown>')
            self.logger.debug(f"Queueing signaling message (offline): {msg_type}")
            self.pending_messages.append(payload)
            return

        try:
            await self._emit_now(payload)
        except Exception as exc:
            msg_type = payload.get('type', '<unknown>')
            self.logger.error(f"Failed to emit signaling message '{msg_type}': {exc}")

    async def _flush_pending(self) -> None:
        """Flush pending messages after reconnect."""
        if not self.pending_messages or not self.sio.connected:
            return

        while self.pending_messages:
            message = self.pending_messages.pop(0)
            try:
                await self._emit_now(message)
            except Exception as exc:
                msg_type = message.get('type', '<unknown>')
                self.logger.error(f"Failed to flush signaling message '{msg_type}': {exc}")
                self.pending_messages.insert(0, message)
                break

    async def _handle_signaling_message(self, data: any) -> None:
        """Handle incoming signaling message."""
        self.logger.debug(f"Received signaling payload: {data}")

        # Parse JSON if needed
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                self.logger.warn("Ignoring non-JSON signaling payload")
                return

        if not isinstance(data, dict):
            self.logger.warn("Ignoring unexpected signaling payload type")
            return

        # Ensure peer connection exists
        if self.peer_manager.pc is None:
            self.logger.info("Peer connection missing; rebuilding")
            self.peer_manager.create()

        msg_type = data.get("type")

        if msg_type == "offer":
            await self.peer_manager.handle_offer(
                sdp=data["sdp"],
                from_id=data.get("from")
            )
        elif msg_type == "candidate":
            await self.peer_manager.handle_ice_candidate(
                candidate_data=data.get("candidate"),
                sdp_mid=data.get("sdpMid"),
                sdp_mline_index=data.get("sdpMLineIndex")
            )
        else:
            self.logger.debug(f"Ignoring unsupported signaling message type: {msg_type}")

    async def connect_and_run(self) -> None:
        """Connect to signaling server and run until disconnected."""
        try:
            await self.sio.connect(
                self.config.connect_url,
                transports=["websocket"],
                namespaces=[self.config.namespace],
                socketio_path=self.config.socketio_path,
                auth={"token": self.auth_token} if self.auth_token else None,
            )
            await self.sio.wait()
        finally:
            await self.disconnect()

    async def disconnect(self) -> None:
        """Disconnect from signaling server."""
        if self.sio.connected:
            await self.sio.disconnect()


# ============================================================================
# WebRTC Bridge Node
# ============================================================================


class WebRTCBridge(Node):
    """ROS2 node that bridges topics to WebRTC data channels."""

    def __init__(self):
        super().__init__("webrtc_client")
        self.get_logger().set_level(LoggingSeverity.DEBUG)

        # Declare and retrieve parameters
        self._declare_parameters()
        self._load_parameters()

        self.get_logger().info(
            f"WebRTC client starting for robot_id={self.robot_id}, "
            f"signaling={self.signaling_url}"
        )

        # Setup ROS publishers and subscribers
        self._setup_ros_interface()

        # Systemd service for NixOS rebuild
        self.rebuild_service_name = "polyflow-rebuild.service"
        self._rebuild_lock = threading.Lock()

    def _declare_parameters(self) -> None:
        """Declare all ROS parameters with defaults."""
        self.declare_parameter("robot_id", "robot-001")
        self.declare_parameter("signaling_url", "ws://polyflow.studio/signal")
        self.declare_parameter("auth_token", "")
        self.declare_parameter("socketio_namespace", "")
        self.declare_parameter("socketio_path", "")
        self.declare_parameter("ice_servers", "stun:stun.l.google.com:19302")
        self.declare_parameter("ice_username", "")
        self.declare_parameter("ice_password", "")

    def _load_parameters(self) -> None:
        """Load parameter values."""
        self.robot_id = self.get_parameter("robot_id").get_parameter_value().string_value
        self.signaling_url = self.get_parameter("signaling_url").get_parameter_value().string_value
        self.auth_token = self.get_parameter("auth_token").get_parameter_value().string_value
        self.socketio_namespace = self.get_parameter("socketio_namespace").get_parameter_value().string_value
        self.socketio_path = self.get_parameter("socketio_path").get_parameter_value().string_value
        self.ice_servers = self.get_parameter("ice_servers").value
        self.ice_username = self.get_parameter("ice_username").get_parameter_value().string_value
        self.ice_password = self.get_parameter("ice_password").get_parameter_value().string_value

    def _setup_ros_interface(self) -> None:
        """Setup ROS publishers and subscribers."""
        self.j1_cmd_pub = self.create_publisher(Float32, "/arm/j1/cmd/position", 10)
        self.j1_state_sub = self.create_subscription(
            Float32, "/arm/j1/state/position", self._on_j1_state, 10
        )
        self.state_channel: Optional[RTCDataChannel] = None

    def _on_j1_state(self, msg: Float32) -> None:
        """Callback when robot publishes joint state."""
        if self.state_channel and self.state_channel.readyState == "open":
            envelope = {
                "topic": "robot/arm/j1/state/position",
                "qos": "state",
                "tUnixNanos": int(time.time() * 1e9),
                "payload": {"positionRad": float(msg.data)},
            }
            try:
                self.state_channel.send(json.dumps(envelope))
            except Exception as exc:
                self.get_logger().warn(f"Failed to send state: {exc}")

    def on_control_message(self, data: str) -> None:
        """Handle incoming control messages from WebRTC."""
        try:
            envelope = json.loads(data)
        except json.JSONDecodeError:
            return

        topic = envelope.get("topic", "")

        if topic == "robot/arm/j1/cmd/position":
            pos = float(envelope["payload"]["positionRad"])
            msg = Float32()
            msg.data = pos
            self.j1_cmd_pub.publish(msg)
            self.get_logger().info(f"Received control: j1 position={pos}")
        elif topic == "system/nixos/rebuild":
            threading.Thread(target=self.trigger_nixos_rebuild, daemon=True).start()

    def trigger_nixos_rebuild(self) -> None:
        """Trigger NixOS rebuild via systemd service."""
        service_name = self.rebuild_service_name
        cmd = ["systemctl", "start", "--no-block", service_name]

        with self._rebuild_lock:
            try:
                subprocess.run(cmd, check=True)
                self.get_logger().info(f"Triggered polyflow-rebuild service {service_name}")
            except subprocess.CalledProcessError as exc:
                self.get_logger().error(f"Failed to start {service_name}: {exc}")


# ============================================================================
# Main WebRTC Loop
# ============================================================================


async def run_webrtc(node: WebRTCBridge) -> None:
    """Main async WebRTC loop using Socket.IO for signaling."""

    # Parse ICE servers
    ice_servers = parse_ice_servers(
        node.ice_servers,
        node.ice_username,
        node.ice_password
    )

    rtc_config = RTCConfiguration(iceServers=ice_servers) if ice_servers else None

    if ice_servers:
        server_urls = [server.urls[0] for server in ice_servers]
        node.get_logger().debug(
            f"Using ICE servers: {server_urls} "
            f"(username set: {bool(node.ice_username)})"
        )
    else:
        node.get_logger().debug("No ICE servers configured; relying on host candidates only")

    # Parse Socket.IO configuration
    socketio_config = parse_socketio_config(
        node.signaling_url,
        node.auth_token,
        node.socketio_namespace,
        node.socketio_path
    )

    # Create peer connection manager
    # We need to define emit_signaling_message first as a placeholder
    signaling_client = None

    async def emit_signaling_message(payload: dict):
        if signaling_client:
            await signaling_client.emit_message(payload)

    peer_manager = PeerConnectionManager(
        config=rtc_config,
        robot_id=node.robot_id,
        logger=node.get_logger(),
        on_control_message=node.on_control_message,
        emit_signaling_message=emit_signaling_message
    )

    # Create initial peer connection
    peer_manager.create()

    # Link state channel to node
    def update_state_channel():
        node.state_channel = peer_manager.state_channel

    # Patch peer manager to update node's state channel reference
    original_create = peer_manager.create
    def patched_create():
        pc = original_create()
        update_state_channel()
        return pc
    peer_manager.create = patched_create

    # Create signaling client
    signaling_client = SignalingClient(
        config=socketio_config,
        robot_id=node.robot_id,
        auth_token=node.auth_token,
        logger=node.get_logger(),
        peer_manager=peer_manager
    )

    # Connect state channel updates
    original_datachannel_handler = None
    if peer_manager.pc:
        # Update state channel reference when channels are created
        @peer_manager.pc.on("datachannel")
        def on_datachannel_wrapper(channel):
            if channel.label == "state":
                node.state_channel = channel

    try:
        await signaling_client.connect_and_run()
    finally:
        if peer_manager.pc:
            try:
                await peer_manager.pc.close()
            except Exception as exc:
                node.get_logger().warn(
                    f"Error while closing peer connection during shutdown: {exc}"
                )


# ============================================================================
# Entry Point
# ============================================================================


def main(args=None):
    """Main entry point for the WebRTC bridge node."""
    rclpy.init(args=args)
    node = WebRTCBridge()

    # Spin ROS in background thread
    executor = SingleThreadedExecutor()
    executor.add_node(node)
    ros_thread = threading.Thread(target=executor.spin, daemon=True)
    ros_thread.start()
    node.get_logger().info("ROS executor started (background thread)")

    # Run async WebRTC client
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        node.get_logger().info("Attempting to run the WebRTC client…")
        loop.run_until_complete(run_webrtc(node))
    except KeyboardInterrupt:
        node.get_logger().info("Keyboard interrupt received")
    except Exception as exc:
        node.get_logger().error(f"WebRTC loop crashed: {exc}")
    finally:
        node.get_logger().info("Shutting down")
        executor.shutdown()
        loop.stop()
        loop.close()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
