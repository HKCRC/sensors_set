#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <vector>

#include <ros/ros.h>
#include <sensor_msgs/Imu.h>

namespace {

bool parseImuPacket(const uint8_t *data, size_t length, sensor_msgs::Imu &imu_msg) {
  constexpr size_t kPacketSize = 20;  // sec(4) + nsec(4) + accel(6) + gyro(6)
  if (length < kPacketSize) {
    return false;
  }

  size_t offset = 0;
  auto read_u32_be = [&](uint32_t &out) {
    uint32_t be = 0;
    std::memcpy(&be, data + offset, sizeof(be));
    offset += sizeof(be);
    out = ntohl(be);
  };
  auto read_u16_be = [&](uint16_t &out) {
    uint16_t be = 0;
    std::memcpy(&be, data + offset, sizeof(be));
    offset += sizeof(be);
    out = ntohs(be);
  };

  uint32_t sec = 0;
  uint32_t nsec = 0;
  read_u32_be(sec);
  read_u32_be(nsec);

  uint16_t ax_u = 0;
  uint16_t ay_u = 0;
  uint16_t az_u = 0;
  uint16_t gx_u = 0;
  uint16_t gy_u = 0;
  uint16_t gz_u = 0;
  read_u16_be(ax_u);
  read_u16_be(ay_u);
  read_u16_be(az_u);
  read_u16_be(gx_u);
  read_u16_be(gy_u);
  read_u16_be(gz_u);

  const int16_t ax = static_cast<int16_t>(ax_u);
  const int16_t ay = static_cast<int16_t>(ay_u);
  const int16_t az = static_cast<int16_t>(az_u);
  const int16_t gx = static_cast<int16_t>(gx_u);
  const int16_t gy = static_cast<int16_t>(gy_u);
  const int16_t gz = static_cast<int16_t>(gz_u);

  imu_msg.header.stamp = ros::Time(sec, nsec);
  // TODO: 如果有量纲/比例系数，请在此处转换为 m/s^2 和 rad/s
  imu_msg.linear_acceleration.x = static_cast<double>(ax);
  imu_msg.linear_acceleration.y = static_cast<double>(ay);
  imu_msg.linear_acceleration.z = static_cast<double>(az);
  imu_msg.angular_velocity.x = static_cast<double>(gx);
  imu_msg.angular_velocity.y = static_cast<double>(gy);
  imu_msg.angular_velocity.z = static_cast<double>(gz);

  return true;
}

}  // namespace

int main(int argc, char **argv) {
  ros::init(argc, argv, "udp_2_imu");
  ros::NodeHandle nh;
  ros::NodeHandle pnh("~");

  std::string local_ip;
  int local_port = 9000;
  int recv_buffer_size = 2048;
  std::string imu_topic;
  std::string frame_id;
  int select_timeout_ms = 100;

  pnh.param<std::string>("local_ip", local_ip, std::string("0.0.0.0"));
  pnh.param<int>("local_port", local_port, 9000);
  pnh.param<int>("recv_buffer_size", recv_buffer_size, 2048);
  pnh.param<std::string>("imu_topic", imu_topic, std::string("imu/data_raw"));
  pnh.param<std::string>("frame_id", frame_id, std::string("imu_link"));
  pnh.param<int>("select_timeout_ms", select_timeout_ms, 100);

  ros::Publisher imu_pub = nh.advertise<sensor_msgs::Imu>(imu_topic, 10);

  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    ROS_FATAL("Failed to create UDP socket");
    return 1;
  }

  sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(local_port));
  if (inet_pton(AF_INET, local_ip.c_str(), &addr.sin_addr) != 1) {
    ROS_FATAL("Invalid local_ip: %s", local_ip.c_str());
    close(sockfd);
    return 1;
  }

  if (bind(sockfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ROS_FATAL("Failed to bind UDP socket on %s:%d", local_ip.c_str(), local_port);
    close(sockfd);
    return 1;
  }

  std::vector<uint8_t> buffer(static_cast<size_t>(recv_buffer_size));

  ROS_INFO("udp_2_imu listening on %s:%d, publish to %s",
           local_ip.c_str(), local_port, imu_topic.c_str());

  while (ros::ok()) {
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(sockfd, &readfds);

    timeval timeout;
    timeout.tv_sec = select_timeout_ms / 1000;
    timeout.tv_usec = (select_timeout_ms % 1000) * 1000;

    int ret = select(sockfd + 1, &readfds, nullptr, nullptr, &timeout);
    if (ret < 0) {
      ROS_WARN("select() error");
      continue;
    }

    if (ret == 0) {
      ros::spinOnce();
      continue;
    }

    if (FD_ISSET(sockfd, &readfds)) {
      ssize_t recv_len = recvfrom(sockfd, buffer.data(), buffer.size(), 0, nullptr, nullptr);
      if (recv_len <= 0) {
        ROS_WARN("recvfrom() failed");
        continue;
      }

      sensor_msgs::Imu imu_msg;
      imu_msg.header.frame_id = frame_id;

      if (parseImuPacket(buffer.data(), static_cast<size_t>(recv_len), imu_msg)) {
        imu_pub.publish(imu_msg);
      }
    }

    ros::spinOnce();
  }

  close(sockfd);
  return 0;
}
