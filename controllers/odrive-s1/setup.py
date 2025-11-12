from setuptools import setup

package_name = 'odrive-s1'

setup(
    name=package_name,
    version='0.0.1',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name, ['odrive-s1.launch.py'])
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Drew Swinney',
    maintainer_email='drew@polyflowrobotics.com',
    description='ODrive S1 controller node for Polyflow robots',
    license='MIT',
    entry_points={
        'console_scripts': [
            'odrive-s1_node = odrive-s1.node:main',
        ],
    },
)
