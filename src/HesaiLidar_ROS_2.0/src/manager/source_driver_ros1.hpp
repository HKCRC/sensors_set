/************************************************************************************************
  Copyright(C)2023 Hesai Technology Co., Ltd.
  All code in this repository is released under the terms of the following [Modified BSD License.]
  Modified BSD License:
  Redistribution and use in source and binary forms,with or without modification,are permitted 
  provided that the following conditions are met:
  *Redistributions of source code must retain the above copyright notice,this list of conditions 
   and the following disclaimer.
  *Redistributions in binary form must reproduce the above copyright notice,this list of conditions and 
   the following disclaimer in the documentation and/or other materials provided with the distribution.
  *Neither the names of the University of Texas at Austin,nor Austin Robot Technology,nor the names of 
   other contributors maybe used to endorse or promote products derived from this software without 
   specific prior written permission.
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGH THOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
  WARRANTIES,INCLUDING,BUT NOT LIMITED TO,THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
  PARTICULAR PURPOSE ARE DISCLAIMED.IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
  ANY DIRECT,INDIRECT,INCIDENTAL,SPECIAL,EXEMPLARY,OR CONSEQUENTIAL DAMAGES(INCLUDING,BUT NOT LIMITED TO,
  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;LOSS OF USE,DATA,OR PROFITS;OR BUSINESS INTERRUPTION)HOWEVER 
  CAUSED AND ON ANY THEORY OF LIABILITY,WHETHER IN CONTRACT,STRICT LIABILITY,OR TORT(INCLUDING NEGLIGENCE 
  OR OTHERWISE)ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,EVEN IF ADVISED OF THE POSSIBILITY OF 
  SUCHDAMAGE.
************************************************************************************************/

/*
 * File: source_driver_ros1.hpp
 * Author: Zhang Yu <zhangyu@hesaitech.com>
 * Description: Source Driver for ROS1
 * Created on June 12, 2023, 10:46 AM
 */

#pragma once
#include <ros/ros.h>
#include "std_msgs/UInt8MultiArray.h"
#include <sensor_msgs/point_cloud2_iterator.h>
#include "hesai_ros_driver/UdpFrame.h"
#include "hesai_ros_driver/UdpPacket.h"
#include "hesai_ros_driver/LossPacket.h"
#include "hesai_ros_driver/Ptp.h"
#include "hesai_ros_driver/Firetime.h"
#include "crane_msg/CustomMsg.h"
#include "crane_msg/CustomPoint.h"
#include <sensor_msgs/Imu.h>
#include <limits>
#include <cmath>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iomanip>
#include <boost/thread.hpp>
#include "source_drive_common.hpp"

class SourceDriver
{
public:
  typedef std::shared_ptr<SourceDriver> Ptr;
  // Initialize some necessary configuration parameters, create ROS nodes, and register callback functions
  virtual void Init(const YAML::Node& config);
  // Start working
  virtual void Start();
  // Stop working
  virtual void Stop();
  virtual ~SourceDriver();
  SourceDriver(SourceType src_type) {};
  void SpinRos1() {
    ros::MultiThreadedSpinner spinner(2); 
    spinner.spin();
  }
  std::shared_ptr<HesaiLidarSdk<LidarPointXYZICRTT>> driver_ptr_;
protected:
  // Save Correction file subscribed by "ros_recv_correction_topic"
  void ReceiveCorrection(const std_msgs::UInt8MultiArray& msg);
  // Save packets subscribed by 'ros_recv_packet_topic'
  void ReceivePacket(const hesai_ros_driver::UdpFrame& msg);
  // Used to publish point clouds through 'ros_send_point_cloud_topic'
  void SendPointCloud(const LidarDecodedFrame<LidarPointXYZICRTT>& msg);
  // Used to publish the original pcake through 'ros_send_packet_topic'
  void SendPacket(const UdpFrame_t&  ros_msg, double);
  // Used to publish the Correction file through 'ros_send_correction_topic'
  void SendCorrection(const u8Array_t& msg);
  // Used to publish the Packet loss condition
  void SendPacketLoss(const uint32_t& total_packet_count, const uint32_t& total_packet_loss_count);
  // Used to publish the Packet loss condition
  void SendPTP(const uint8_t& ptp_lock_offset, const u8Array_t& ptp_status);
  // Used to publish the firetime correction 
  void SendFiretime(const double *firetime_correction_);
  // Used to publish the imu packet
  void SendImuConfig(const LidarImuData& msg);
  // Convert ptp lock offset, status into ROS message
  hesai_ros_driver::Ptp ToRosMsg(const uint8_t& ptp_lock_offset, const u8Array_t& ptp_status);
  // Convert packet loss condition into ROS message
  hesai_ros_driver::LossPacket ToRosMsg(const uint32_t& total_packet_count, const uint32_t& total_packet_loss_count);
  // Convert correction string into ROS messages
  std_msgs::UInt8MultiArray ToRosMsg(const u8Array_t& correction_string);
  // Convert point clouds into ROS messages
  sensor_msgs::PointCloud2 ToRosMsg(const LidarDecodedFrame<LidarPointXYZICRTT>& frame, const std::string& frame_id);
  // Convert packets into ROS messages
  hesai_ros_driver::UdpFrame ToRosMsg(const UdpFrame_t& ros_msg, double timestamp);
  // Convert double[512] to float64[512]
  hesai_ros_driver::Firetime ToRosMsg(const double *firetime_correction_);
  // Convert imu, imu into ROS message
  sensor_msgs::Imu ToRosMsg(const LidarImuData& firetime_correction_);
  // Convert Linear Acceleration from g to m/s^2
  double From_g_To_ms2(double g);
  // Convert Angular Velocity from degree/s to radian/s
  double From_degs_To_rads(double degree);
  // publish point
  std::shared_ptr<ros::NodeHandle> nh_;
  ros::Publisher pub_;
  ros::Publisher custom_pub_;
  std::string frame_id_;
  // publish packet 
  ros::Publisher pkt_pub_;
  // packet sub
  ros::Subscriber pkt_sub_;
  //spin thread while Receive data from ROS topic
  boost::thread* subscription_spin_thread_;

  ros::Publisher crt_pub_;
  ros::Publisher firetime_pub_;
  ros::Publisher loss_pub_;
  ros::Publisher ptp_pub_;
  ros::Subscriber crt_sub_;
  ros::Publisher imu_pub_;

  // config for async verifier
  bool verify_enable_ = false;
  bool verify_only_at128p_single_ = true;

  // Async verifier: compare ROS1 PointCloud2 and CustomMsg without blocking publisher
  std::thread verify_thread_;
  std::mutex verify_mutex_;
  std::condition_variable verify_cv_;
  bool verify_shutdown_ = false;
  static constexpr size_t kVerifyQueueMaxSize = 2;
  static constexpr uint16_t kReturnModeMulti = 0x39;
  static constexpr uint16_t kReturnModeMultiTriple = 0x3D;
  std::deque<std::pair<sensor_msgs::PointCloud2, crane_msg::CustomMsg>> verify_queue_;
  std::string custom_dump_path_;
  bool custom_dump_enabled_ = false;
  bool custom_dump_warned_ = false;
  std::mutex custom_dump_mutex_;
  std::string packet_dump_path_;
  bool packet_dump_enabled_ = false;
  bool packet_dump_warned_ = false;
  std::mutex packet_dump_mutex_;

  // Start/stop background thread
  inline void StartVerifyThread_() {
    if (verify_thread_.joinable()) return;
    verify_shutdown_ = false;
    verify_thread_ = std::thread([this]() { this->VerifyLoop_(); });
  }
  inline void StopVerifyThread_() {
    {
      std::lock_guard<std::mutex> lk(verify_mutex_);
      verify_shutdown_ = true;
    }
    verify_cv_.notify_all();
    if (verify_thread_.joinable()) verify_thread_.join();
  }
  inline void EnqueueVerify_(const sensor_msgs::PointCloud2& pc2, const crane_msg::CustomMsg& custom) {
    if (!verify_enable_) return;
    StartVerifyThread_();
    std::unique_lock<std::mutex> lk(verify_mutex_);
    if (verify_queue_.size() >= kVerifyQueueMaxSize) {
      verify_queue_.pop_front();
    }
    verify_queue_.emplace_back(pc2, custom);
    lk.unlock();
    verify_cv_.notify_one();
  }
  inline static bool NearlyEqualFloat_(float a, float b, float eps = 1e-5f) {
    return std::fabs(a - b) <= eps;
  }
  inline static bool NearlyEqualDouble_(double a, double b, double eps = 1e-6) {
    return std::fabs(a - b) <= eps;
  }
  inline bool VerifyPair_(const sensor_msgs::PointCloud2& pc2, const crane_msg::CustomMsg& custom) {
    // Only verify frame-level header timestamp and point count.
    if (pc2.height != 1) return false;
    if (pc2.width != custom.point_num) return false;
    if (custom.points.size() != custom.point_num) return false;
    if (pc2.header.stamp.sec != custom.header.stamp.sec) return false;
    if (pc2.header.stamp.nsec != custom.header.stamp.nsec) return false;
    return true;
  }
  inline void VerifyLoop_() {
    while (true) {
      std::pair<sensor_msgs::PointCloud2, crane_msg::CustomMsg> item;
      {
        std::unique_lock<std::mutex> lk(verify_mutex_);
        verify_cv_.wait(lk, [&]{ return verify_shutdown_ || !verify_queue_.empty(); });
        if (verify_shutdown_) return;
        item = std::move(verify_queue_.front());
        verify_queue_.pop_front();
      }
      bool ok = false;
      try { ok = VerifyPair_(item.first, item.second); } catch (...) { ok = false; }
      double pc2_header_time = static_cast<double>(item.first.header.stamp.sec) +
                               static_cast<double>(item.first.header.stamp.nsec) * 1e-9;
      double custom_header_time = static_cast<double>(item.second.header.stamp.sec) +
                                  static_cast<double>(item.second.header.stamp.nsec) * 1e-9;
      printf("[verify_compare] seq=%u pc2_ts=%.9f custom_ts=%.9f points=%u\n",
             item.second.header.seq,
             pc2_header_time,
             custom_header_time,
             item.first.width);
      if (ok) {
        printf("OK PASSED (seq=%u, points=%u)\n", item.second.header.seq, item.second.point_num);
      } else {
        printf("VERIFY FAILED (seq=%u)\n", item.second.header.seq);
      }
      std::cout.flush();
    }
  }

  inline void DumpPacketTimesForFrame_(const LidarDecodedFrame<LidarPointXYZICRTT>& frame,
                                       int required_frame_index = -1) {
    if (!packet_dump_enabled_) {
      return;
    }
    // Only log for AT128P dual-return frames (block_num==2 and return_mode==kReturnModeMulti)
    if (!(frame.block_num == 2 && frame.return_mode == kReturnModeMulti)) {
      return;
    }
    const bool is_multi_freq = frame.fParam.IsMultiFrameFrequency() != 0;
    const uint32_t point_num = is_multi_freq ? frame.multi_points_num : frame.points_num;
    const int frame_index = is_multi_freq ? frame.multi_frame_index : frame.frame_index;
    if (required_frame_index >= 0 && frame_index != required_frame_index) {
      return;
    }
    const uint32_t packet_num = frame.packet_num;
    const LidarPointXYZICRTT* points = is_multi_freq ? frame.multi_points : frame.points;
    if (point_num == 0 || points == nullptr || packet_num == 0 || frame.packetData == nullptr) {
      return;
    }
    std::lock_guard<std::mutex> lk(packet_dump_mutex_);
    std::ofstream ofs(packet_dump_path_, std::ios::trunc);
    if (!ofs.is_open()) {
      if (!packet_dump_warned_) {
        ROS_WARN_STREAM("Failed to open packet_time_dump_path: " << packet_dump_path_);
        packet_dump_warned_ = true;
      }
      return;
    }
    ofs << "frame_index=" << frame_index
        << " packet_num=" << packet_num
        << " point_num=" << point_num << '\n';
    ofs << "packet_time_ns";
    constexpr uint64_t kUsPerNs = 1000ULL;
    for (uint32_t i = 0; i < packet_num; ++i) {
      uint64_t t_ns = static_cast<uint64_t>(frame.packetData[i].t.sensor_timestamp) * kUsPerNs;
      ofs << ' ' << t_ns;
    }
    ofs << '\n';
    ofs << "point_time_ns";
    constexpr uint64_t kNsPerSec = 1000000000ULL;
    for (uint32_t i = 0; i < point_num; ++i) {
      uint64_t t_ns = points[i].timeSecond * kNsPerSec +
                      static_cast<uint64_t>(points[i].timeNanosecond);
      ofs << ' ' << t_ns;
    }
    ofs << '\n';
    // only record once then disable further dumps
    packet_dump_enabled_ = false;
  }

  inline bool DumpCustomMsgToFile_(const crane_msg::CustomMsg& custom) {
    if (!custom_dump_enabled_) {
      return false;
    }
    std::lock_guard<std::mutex> lk(custom_dump_mutex_);
    // Overwrite file each time so that it only keeps one frame
    std::ofstream ofs(custom_dump_path_, std::ios::trunc);
    if (!ofs.is_open()) {
      if (!custom_dump_warned_) {
        ROS_WARN_STREAM("Failed to open custom_msg_dump_path: " << custom_dump_path_);
        custom_dump_warned_ = true;
      }
      return false;
    }
    double header_time = custom.header.stamp.toSec();
    ofs << std::fixed << std::setprecision(9);
    ofs << "seq=" << custom.header.seq
        << " header_time=" << header_time
        << " (sec=" << custom.header.stamp.sec
        << ", nsec=" << custom.header.stamp.nsec << ")"
        << " point_num=" << custom.point_num
        << " timebase_ns=" << custom.timebase << '\n';
    ofs << "offset_time_ns";
    for (const auto& pt : custom.points) {
      ofs << ' ' << static_cast<uint32_t>(pt.offset_time);
    }
    ofs << "\n";
    // only record once then disable further dumps
    custom_dump_enabled_ = false;
    return true;
  }
};


inline void SourceDriver::Init(const YAML::Node& config)
{
  
  DriverParam driver_param;
  DriveYamlParam yaml_param;
  yaml_param.GetDriveYamlParam(config, driver_param);
  frame_id_ = driver_param.input_param.frame_id;
  custom_dump_enabled_ = false;
  custom_dump_warned_ = false;
  custom_dump_path_.clear();
  packet_dump_enabled_ = false;
  packet_dump_warned_ = false;
  packet_dump_path_.clear();

  nh_ = std::unique_ptr<ros::NodeHandle>(new ros::NodeHandle());
  if (driver_param.input_param.send_point_cloud_ros) {
    pub_ = nh_->advertise<sensor_msgs::PointCloud2>(driver_param.input_param.ros_send_point_topic, 10);
    // also publish custom point cloud message for ROS1 if enabled
    if (driver_param.input_param.send_custom_msg && driver_param.input_param.ros_send_custom_msg_topic != NULL_TOPIC) {
      custom_pub_ = nh_->advertise<crane_msg::CustomMsg>(driver_param.input_param.ros_send_custom_msg_topic, 10);
      custom_dump_path_ = driver_param.input_param.custom_msg_dump_path;
      custom_dump_enabled_ = !custom_dump_path_.empty();
      custom_dump_warned_ = false;
    }
  }
  packet_dump_path_ = driver_param.input_param.packet_time_dump_path;
  packet_dump_enabled_ = !packet_dump_path_.empty();
  packet_dump_warned_ = false;

  if (driver_param.input_param.send_imu_ros) {
    imu_pub_ = nh_->advertise<sensor_msgs::Imu>(driver_param.input_param.ros_send_imu_topic, 10);
  }
  
  if (driver_param.input_param.ros_send_packet_loss_topic != NULL_TOPIC) {
    loss_pub_ = nh_->advertise<hesai_ros_driver::LossPacket>(driver_param.input_param.ros_send_packet_loss_topic, 10);
  } 

  if (driver_param.input_param.source_type == DATA_FROM_LIDAR) {
    if (driver_param.input_param.ros_send_ptp_topic != NULL_TOPIC) {
      ptp_pub_ = nh_->advertise<hesai_ros_driver::Ptp>(driver_param.input_param.ros_send_ptp_topic, 10);
    } 

    if (driver_param.input_param.ros_send_correction_topic != NULL_TOPIC) {
      crt_pub_ = nh_->advertise<std_msgs::UInt8MultiArray>(driver_param.input_param.ros_send_correction_topic, 10);
    } 
  }
  if (! driver_param.input_param.firetimes_path.empty() ) {
    if (driver_param.input_param.ros_send_firetime_topic != NULL_TOPIC) {
      firetime_pub_ = nh_->advertise<hesai_ros_driver::Firetime>(driver_param.input_param.ros_send_firetime_topic, 10);
    } 
  }

  if (driver_param.input_param.send_packet_ros) {
    pkt_pub_ = nh_->advertise<hesai_ros_driver::UdpFrame>(driver_param.input_param.ros_send_packet_topic, 10);
  }

  if (driver_param.input_param.source_type == DATA_FROM_ROS_PACKET) {
    pkt_sub_ = nh_->subscribe(driver_param.input_param.ros_recv_packet_topic, 100, &SourceDriver::ReceivePacket, this);

    if (driver_param.input_param.ros_recv_correction_topic != NULL_TOPIC) {
      crt_sub_ = nh_->subscribe(driver_param.input_param.ros_recv_correction_topic, 10, &SourceDriver::ReceiveCorrection, this);
    }

    driver_param.decoder_param.enable_udp_thread = false;
    subscription_spin_thread_ = new boost::thread(boost::bind(&SourceDriver::SpinRos1,this));
  }

  // init verifier config
  verify_enable_ = driver_param.input_param.ros_verify_enable;
  verify_only_at128p_single_ = driver_param.input_param.ros_verify_only_at128p_single;

  driver_ptr_.reset(new HesaiLidarSdk<LidarPointXYZICRTT>());
  driver_param.decoder_param.enable_parser_thread = true;
  if (driver_param.input_param.send_point_cloud_ros) {
    driver_ptr_->RegRecvCallback([this](const hesai::lidar::LidarDecodedFrame<hesai::lidar::LidarPointXYZICRTT>& frame) {  
      this->SendPointCloud(frame);  
    }); 
  }
  if (driver_param.input_param.send_imu_ros) {
    driver_ptr_->RegRecvCallback(std::bind(&SourceDriver::SendImuConfig, this, std::placeholders::_1));
  }
  if (driver_param.input_param.send_packet_ros) {
    driver_ptr_->RegRecvCallback(std::bind(&SourceDriver::SendPacket, this, std::placeholders::_1, std::placeholders::_2)) ;
  }
  if (driver_param.input_param.ros_send_packet_loss_topic != NULL_TOPIC) {
    driver_ptr_->RegRecvCallback(std::bind(&SourceDriver::SendPacketLoss, this, std::placeholders::_1, std::placeholders::_2));
  }
  if (driver_param.input_param.source_type == DATA_FROM_LIDAR) {
    if (driver_param.input_param.ros_send_correction_topic != NULL_TOPIC) {
      driver_ptr_->RegRecvCallback(std::bind(&SourceDriver::SendCorrection, this, std::placeholders::_1));
    }
    if (driver_param.input_param.ros_send_ptp_topic != NULL_TOPIC) {
      driver_ptr_->RegRecvCallback(std::bind(&SourceDriver::SendPTP, this, std::placeholders::_1, std::placeholders::_2));
    }
  } 
  if (!driver_ptr_->Init(driver_param))
  {
    std::cout << "Driver Initialize Error...." << std::endl;
    exit(-1);
  }
}

inline void SourceDriver::Start()
{
  driver_ptr_->Start();
}

inline SourceDriver::~SourceDriver()
{
  Stop();
}

inline void SourceDriver::Stop()
{
  driver_ptr_->Stop();
  StopVerifyThread_();
}

inline void SourceDriver::SendPacket(const UdpFrame_t& msg, double timestamp)
{
  pkt_pub_.publish(ToRosMsg(msg, timestamp));
}

inline void SourceDriver::SendPointCloud(const LidarDecodedFrame<LidarPointXYZICRTT>& msg)
{
  auto pc2 = ToRosMsg(msg, frame_id_);
  pub_.publish(pc2);
  // build and publish custom message alongside
  int packet_dump_expect_frame = -1;
  if (custom_pub_) {
    crane_msg::CustomMsg custom;
    // header will be set after we compute the earliest per-point time
    custom.point_num = (msg.fParam.IsMultiFrameFrequency() == 0) ? msg.points_num : msg.multi_points_num;
    custom.lidar_id = 0;
    custom.rsvd[0] = 0; custom.rsvd[1] = 0; custom.rsvd[2] = 0;
    custom.points.reserve(custom.point_num);
    const LidarPointXYZICRTT *pPoints = (msg.fParam.IsMultiFrameFrequency() == 0) ? msg.points : msg.multi_points;
    // frame index for stable pairing across topics
    int frame_index = (msg.fParam.IsMultiFrameFrequency() == 0) ? msg.frame_index : msg.multi_frame_index;
    // find earliest (sec,nsec)
    bool found_min = false;
    uint64_t min_sec = std::numeric_limits<uint64_t>::max();
    uint32_t min_nsec = std::numeric_limits<uint32_t>::max();
    for (uint32_t i = 0; i < custom.point_num; ++i) {
      const auto &pt = pPoints[i];
      if (!found_min || pt.timeSecond < min_sec || (pt.timeSecond == min_sec && pt.timeNanosecond < min_nsec)) {
        min_sec = pt.timeSecond;
        min_nsec = pt.timeNanosecond;
        found_min = true;
      }
    }
    // fill header/timebase from earliest timestamp
    if (found_min) {
      // If earliest per-point timestamp is zero, fallback to frame_start_timestamp
      if (min_sec == 0 && min_nsec == 0 && msg.frame_start_timestamp > 0.0) {
        custom.header.stamp = ros::Time().fromSec(msg.frame_start_timestamp);
        custom.timebase = static_cast<uint64_t>(msg.frame_start_timestamp * 1e9);
      } else {
        if (min_sec <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
          custom.header.stamp = ros::Time(static_cast<uint32_t>(min_sec), static_cast<uint32_t>(min_nsec));
        } else {
          // fallback to fromSec if overflow (unlikely within 2038 window)
          custom.header.stamp = ros::Time().fromSec(static_cast<double>(min_sec) + static_cast<double>(min_nsec) * 1e-9);
        }
        custom.timebase = min_sec * 1000000000ULL + static_cast<uint64_t>(min_nsec);
      }
      custom.header.frame_id = frame_id_;
      custom.header.seq = static_cast<uint32_t>(frame_index);
      // // print header timestamps for both ROS PointCloud2 and CustomMsg (disabled)
      // printf("publish seq=%u ts(pc2)=%.9f ts(custom)=%.9f points(pc2)=%u points(custom)=%u\n",
      //        custom.header.seq,
      //        pc2.header.stamp.toSec(),
      //        custom.header.stamp.toSec(),
      //        pc2.width,
      //        custom.point_num);
    }
    // compose points and offsets based on earliest timestamp
    for (uint32_t i = 0; i < custom.point_num; ++i) {
      const auto &pt = pPoints[i];
      crane_msg::CustomPoint cp;
      // offset_time in nanoseconds relative to earliest (sec,nsec)
      int64_t dt_ns = static_cast<int64_t>((pt.timeSecond - min_sec) * 1000000000ULL)
                    + static_cast<int64_t>(pt.timeNanosecond) - static_cast<int64_t>(min_nsec);
      if (dt_ns < 0) dt_ns = 0;
      if (dt_ns > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) dt_ns = std::numeric_limits<uint32_t>::max();
      cp.offset_time = static_cast<uint32_t>(dt_ns);
      cp.x = pt.x; cp.y = pt.y; cp.z = pt.z;
      cp.reflectivity = pt.intensity;
      cp.tag = 0;
      cp.line = static_cast<uint8_t>(pt.ring);
      custom.points.push_back(std::move(cp));
    }
    bool custom_dumped = DumpCustomMsgToFile_(custom);
    if (custom_dumped) {
      packet_dump_expect_frame = static_cast<int>(custom.header.seq);
    }
    custom_pub_.publish(custom);
    // enqueue for async verification without blocking publisher
    bool allow_verify = verify_enable_;
    if (allow_verify && verify_only_at128p_single_) {
      // Treat as AT128P single-return when either block_num==2 or the AT128P block timestamp
      // feature is enabled (config hint), and the return mode is not multi-return.
      bool is_at128_mode = (msg.block_num == 2) || msg.fParam.at128p_block_ts_enable;
      if (is_at128_mode) {
        uint16_t rm = msg.return_mode;
        if (rm == kReturnModeMulti || rm == kReturnModeMultiTriple) {
          is_at128_mode = false;
        }
      }
      // printf("[verify_gate] block_num=%u return_mode=0x%02x at128_ts=%d allow=%d\n",
      //        msg.block_num,
      //        static_cast<unsigned>(msg.return_mode),
      //        msg.fParam.at128p_block_ts_enable ? 1 : 0,
      //        is_at128_mode ? 1 : 0);
      allow_verify = is_at128_mode;
    }
    if (allow_verify) {
      EnqueueVerify_(pc2, custom);
    }
  }
  // dump per-point absolute timestamps for the same frame recorded in CustomMsg (if available)
  DumpPacketTimesForFrame_(msg, packet_dump_expect_frame);
}

inline void SourceDriver::SendCorrection(const u8Array_t& msg)
{
  crt_pub_.publish(ToRosMsg(msg));
}

inline void SourceDriver::SendPacketLoss(const uint32_t& total_packet_count, const uint32_t& total_packet_loss_count)
{
  loss_pub_.publish(ToRosMsg(total_packet_count, total_packet_loss_count));
}

inline void SourceDriver::SendPTP(const uint8_t& ptp_lock_offset, const u8Array_t& ptp_status)
{
  ptp_pub_.publish(ToRosMsg(ptp_lock_offset, ptp_status));
}

inline void SourceDriver::SendFiretime(const double *firetime_correction_)
{
  firetime_pub_.publish(ToRosMsg(firetime_correction_));
}

inline void SourceDriver::SendImuConfig(const LidarImuData& msg)
{
  imu_pub_.publish(ToRosMsg(msg));
}

inline sensor_msgs::PointCloud2 SourceDriver::ToRosMsg(const LidarDecodedFrame<LidarPointXYZICRTT>& frame, const std::string& frame_id)
{
  sensor_msgs::PointCloud2 ros_msg;
  uint32_t points_number = (frame.fParam.IsMultiFrameFrequency() == 0) ? frame.points_num : frame.multi_points_num;
  uint32_t packet_number = (frame.fParam.IsMultiFrameFrequency() == 0) ? frame.packet_num : frame.multi_packet_num;
  LidarPointXYZICRTT *pPoints = (frame.fParam.IsMultiFrameFrequency() == 0) ? frame.points : frame.multi_points;
  int frame_index = (frame.fParam.IsMultiFrameFrequency() == 0) ? frame.frame_index : frame.multi_frame_index;
  double frame_start_timestamp = (frame.fParam.IsMultiFrameFrequency() == 0) ? frame.frame_start_timestamp : frame.multi_frame_start_timestamp;
  double frame_end_timestamp = (frame.fParam.IsMultiFrameFrequency() == 0) ? frame.frame_end_timestamp : frame.multi_frame_end_timestamp;
  const char *prefix = (frame.fParam.IsMultiFrameFrequency() == 0) ? "raw" : "multi";
  int fields = 6;
  ros_msg.fields.clear();
  ros_msg.fields.reserve(fields);
  ros_msg.width = points_number;
  ros_msg.height = 1; 

  int offset = 0;
  offset = addPointField(ros_msg, "x", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "y", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "z", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "ring", 1, sensor_msgs::PointField::UINT16, offset);
  offset = addPointField(ros_msg, "timestamp", 1, sensor_msgs::PointField::FLOAT64, offset);

  ros_msg.point_step = offset;
  ros_msg.row_step = ros_msg.width * ros_msg.point_step;
  ros_msg.is_dense = false;
  ros_msg.data.resize(points_number * ros_msg.point_step);

  sensor_msgs::PointCloud2Iterator<float> iter_x_(ros_msg, "x");
  sensor_msgs::PointCloud2Iterator<float> iter_y_(ros_msg, "y");
  sensor_msgs::PointCloud2Iterator<float> iter_z_(ros_msg, "z");
  sensor_msgs::PointCloud2Iterator<float> iter_intensity_(ros_msg, "intensity");
  sensor_msgs::PointCloud2Iterator<uint16_t> iter_ring_(ros_msg, "ring");
  sensor_msgs::PointCloud2Iterator<double> iter_timestamp_(ros_msg, "timestamp");
  bool found_min = false;
  uint64_t min_sec = std::numeric_limits<uint64_t>::max();
  uint32_t min_nsec = std::numeric_limits<uint32_t>::max();
  for (size_t i = 0; i < points_number; i++)
  {
    LidarPointXYZICRTT point = pPoints[i];
    *iter_x_ = point.x;
    *iter_y_ = point.y;
    *iter_z_ = point.z;
    *iter_intensity_ = point.intensity;
    *iter_ring_ = point.ring;
    // compose double timestamp from (sec,nsec)
    *iter_timestamp_ = static_cast<double>(point.timeSecond) + static_cast<double>(point.timeNanosecond) * 1e-9;
    if (!found_min || point.timeSecond < min_sec || (point.timeSecond == min_sec && point.timeNanosecond < min_nsec)) {
      min_sec = point.timeSecond;
      min_nsec = point.timeNanosecond;
      found_min = true;
    }
    ++iter_x_;
    ++iter_y_;
    ++iter_z_;
    ++iter_intensity_;
    ++iter_ring_;
    ++iter_timestamp_;   
  }
  // printf("%s frame:%d points:%u packet:%d start time:%lf end time:%lf\n", prefix, frame_index, points_number, packet_number, frame_start_timestamp, frame_end_timestamp) ;
  // ros_msg.header.seq = s; set header to earliest per-point time if available
  if (found_min && min_sec <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
    // If earliest per-point timestamp is zero, fallback to frame_start_timestamp
    if (min_sec == 0 && min_nsec == 0 && frame_start_timestamp > 0.0) {
      ros_msg.header.stamp = ros::Time().fromSec(frame_start_timestamp);
    } else {
      ros_msg.header.stamp = ros::Time(static_cast<uint32_t>(min_sec), static_cast<uint32_t>(min_nsec));
    }
  } else {
    int64_t sec = static_cast<int64_t>(frame_start_timestamp);  
    if (sec <= std::numeric_limits<int32_t>::max()) {
      ros_msg.header.stamp = ros::Time().fromSec(frame_start_timestamp);
    } else {
      printf("ros1 does not support timestamps greater than 19 January 2038 03:14:07 (now %lf)\n", frame_start_timestamp);
    }
  }
  // carry frame index for stable cross-topic pairing
  ros_msg.header.seq = static_cast<uint32_t>(frame_index);
  ros_msg.header.frame_id = frame_id_;
  return ros_msg;
}

inline hesai_ros_driver::UdpFrame SourceDriver::ToRosMsg(const UdpFrame_t& ros_msg, double timestamp) {
  hesai_ros_driver::UdpFrame rs_msg;
  for (size_t i = 0 ; i < ros_msg.size(); i++) {
    hesai_ros_driver::UdpPacket rawpacket;
    rawpacket.size = ros_msg[i].packet_len;
    rawpacket.data.resize(ros_msg[i].packet_len);
    memcpy(&rawpacket.data[0], &ros_msg[i].buffer[0], ros_msg[i].packet_len);
    rs_msg.packets.push_back(rawpacket);
  }
  int64_t sec = static_cast<int64_t>(timestamp);  
  if (sec <= std::numeric_limits<int32_t>::max()) {
    rs_msg.header.stamp = ros::Time().fromSec(timestamp);
  } else {
    printf("ros1 does not support timestamps greater than 19 January 2038 03:14:07 (now %lf)\n", timestamp);
  }
  rs_msg.header.frame_id = frame_id_;
  return rs_msg;
}

inline std_msgs::UInt8MultiArray SourceDriver::ToRosMsg(const u8Array_t& correction_string) {
  std_msgs::UInt8MultiArray msg;
  msg.data.resize(correction_string.size());
  std::copy(correction_string.begin(), correction_string.end(), msg.data.begin());
  return msg;
}

inline hesai_ros_driver::LossPacket SourceDriver::ToRosMsg(const uint32_t& total_packet_count, const uint32_t& total_packet_loss_count)
{
  hesai_ros_driver::LossPacket msg;
  msg.total_packet_count = total_packet_count;
  msg.total_packet_loss_count = total_packet_loss_count;  
  return msg;
}

inline hesai_ros_driver::Ptp SourceDriver::ToRosMsg(const uint8_t& ptp_lock_offset, const u8Array_t& ptp_status)
{
  hesai_ros_driver::Ptp msg;
  msg.ptp_lock_offset = ptp_lock_offset;
  std::copy(ptp_status.begin(), ptp_status.begin() + std::min(16ul, ptp_status.size()), msg.ptp_status.begin());
  return msg;
}

inline hesai_ros_driver::Firetime SourceDriver::ToRosMsg(const double *firetime_correction_)
{
  hesai_ros_driver::Firetime msg;
  std::copy(firetime_correction_, firetime_correction_ + 512, msg.data.begin());
  return msg;
}

inline sensor_msgs::Imu SourceDriver::ToRosMsg(const LidarImuData &imu_config_)
{
  sensor_msgs::Imu ros_msg;
  int64_t sec = static_cast<int64_t>(imu_config_.timestamp);  
  if (sec <= std::numeric_limits<int32_t>::max()) {
    ros_msg.header.stamp = ros::Time().fromSec(imu_config_.timestamp);
  } else {
    printf("ros1 does not support timestamps greater than 19 January 2038 03:14:07 (now %lf)\n", imu_config_.timestamp);
  }
  ros_msg.header.frame_id = frame_id_;
  ros_msg.linear_acceleration.x = From_g_To_ms2(imu_config_.imu_accel_x);
  ros_msg.linear_acceleration.y = From_g_To_ms2(imu_config_.imu_accel_y);
  ros_msg.linear_acceleration.z = From_g_To_ms2(imu_config_.imu_accel_z);
  ros_msg.angular_velocity.x = From_degs_To_rads(imu_config_.imu_ang_vel_x);
  ros_msg.angular_velocity.y = From_degs_To_rads(imu_config_.imu_ang_vel_y);
  ros_msg.angular_velocity.z = From_degs_To_rads(imu_config_.imu_ang_vel_z);
  return ros_msg;
}

inline void SourceDriver::ReceivePacket(const hesai_ros_driver::UdpFrame& msg)
{
  for (size_t i = 0; i < msg.packets.size(); i++) {
    if(driver_ptr_->lidar_ptr_->origin_packets_buffer_.full()) std::this_thread::sleep_for(std::chrono::microseconds(10000));
    driver_ptr_->lidar_ptr_->origin_packets_buffer_.emplace_back(&msg.packets[i].data[0], msg.packets[i].size);
  }
}

inline void SourceDriver::ReceiveCorrection(const std_msgs::UInt8MultiArray& msg)
{
  driver_ptr_->lidar_ptr_->correction_string_.resize(msg.data.size());
  std::copy(msg.data.begin(), msg.data.end(), driver_ptr_->lidar_ptr_->correction_string_.begin());
  while (1) {
    if (! driver_ptr_->lidar_ptr_->LoadCorrectionFromROSbag()) {
      break;
    }
  }
}
inline double SourceDriver::From_g_To_ms2(double g)
{
  return g * 9.80665;
}
inline double SourceDriver::From_degs_To_rads(double degree)
{
  return degree * M_PI / 180.0;
}
