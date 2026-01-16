cmake_minimum_required(VERSION 3.5)
project(crane_msg)

message(=============================================================)
message("-- ROS_VERSION is $ENV{ROS_VERSION}")
message(=============================================================)

if($ENV{ROS_VERSION} MATCHES "1")
  find_package(catkin REQUIRED COMPONENTS
    std_msgs
    message_generation
  )
  add_message_files(
    FILES
    CustomPoint.msg
    CustomMsg.msg
  )
  generate_messages(
    DEPENDENCIES std_msgs
  )
  catkin_package(
    CATKIN_DEPENDS std_msgs message_runtime
  )
elseif($ENV{ROS_VERSION} MATCHES "2")
  find_package(ament_cmake REQUIRED)
  find_package(rosidl_default_generators REQUIRED)
  find_package(std_msgs REQUIRED)
  rosidl_generate_interfaces(${PROJECT_NAME}
    "msg/CustomPoint.msg"
    "msg/CustomMsg.msg"
    DEPENDENCIES std_msgs
  )
  ament_export_dependencies(rosidl_default_runtime)
  ament_package()
endif()

