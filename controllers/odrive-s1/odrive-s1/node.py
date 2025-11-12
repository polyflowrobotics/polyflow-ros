# controllers/odrive-s1.py
import json
import threading

import rclpy
from rclpy.node import Node
from rclpy.executors import SingleThreadedExecutor
from std_msgs.msg import Float32

class ODriveS1Controller(Node):
    def __init__(self):
        super().__init__("odrive_s1_node")
        self.get_logger().set_level(rclpy.logging.LoggingSeverity.DEBUG)

        # Declare ROS params with defaults
        self.declare_parameter("joint_ids", "joint1")

        self.get_logger().info(f"ODrive S1 node starting for joints={self.joint_ids}")

async def run_odrive(node: ODriveS1Controller):
    node.get_logger().info("Started ODrive S1 Controller node")

def main(args=None):
    rclpy.init(args=args)
    node = ODriveS1Controller()

    # Spin ROS in the background so subscriptions/timers actually run
    executor = SingleThreadedExecutor()
    executor.add_node(node)
    ros_thread = threading.Thread(target=executor.spin, daemon=True)
    ros_thread.start()
    node.get_logger().info("ROS executor started (background thread)")

    # Run the async WebRTC client
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        node.get_logger().info("Attempting to run ODrive S1 Controller…")
        loop.run_until_complete(run_odrive(node))
    except KeyboardInterrupt:
        node.get_logger().info("Keyboard interrupt received")
    except Exception as e:
        node.get_logger().error(f"WebRTC loop crashed: {e}")
    finally:
        node.get_logger().info("Shutting down")
        executor.shutdown()
        loop.stop()
        loop.close()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
