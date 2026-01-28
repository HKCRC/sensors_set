cmake_minimum_required(VERSION 3.0.2)
project(udp_2_imu)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  sensor_msgs
)

catkin_package()

include_directories(
  ${catkin_INCLUDE_DIRS}
)

add_executable(udp_2_imu_node src/main.cpp)
target_link_libraries(udp_2_imu_node
  ${catkin_LIBRARIES}
)
