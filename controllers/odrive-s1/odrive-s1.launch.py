# launch/odrive-s1.launch.py
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node
from launch_ros.parameter_descriptions import ParameterValue


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument(
            "joint_ids",
            default_value="{{JOINT_IDS}}",
            description="output|multi|/joint/state|sensor_msgs/JointState"
        ),
        DeclareLaunchArgument(
            "coordinator_id",
            default_value="{{COORDINATOR_ID}}",
            description="input|single|/joint_controller/command|trajectory_msgs/JointTrajectory"
        ),
        Node(
            package="odrive-s1",
            executable="odrive-s1_node",
            name="odrive-s1_client",
            output="screen",
            parameters=[{
                "joint_ids": LaunchConfiguration("joint_ids"),
                "coordinator_id": LaunchConfiguration("coordinator_id"),
            }],
        ),
    ])
