#!/usr/bin/env python3
import argparse
import os
import socket
import struct
import time

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def build_packet(ax, ay, az, gx, gy, gz):
    now = time.time()
    sec = int(now)
    nsec = int((now - sec) * 1e9)
    # Packet: sec(u32 BE), nsec(u32 BE), accel(int16 BE) x3, gyro(int16 BE) x3
    return struct.pack(
        ">IIhhhhhh",
        sec,
        nsec,
        int(ax),
        int(ay),
        int(az),
        int(gx),
        int(gy),
        int(gz),
    )


def load_config_rate(config_path):
    if not config_path or not os.path.exists(config_path):
        return None
    if yaml is not None:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        udp_cfg = cfg.get("udp_2_imu", {})
        rate = udp_cfg.get("sim_rate_hz")
        return float(rate) if rate is not None else None
    # Fallback: minimal parser for `udp_2_imu: ... sim_rate_hz: value`
    in_block = False
    rate_value = None
    with open(config_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line == "udp_2_imu:":
                in_block = True
                continue
            if not in_block:
                continue
            if ":" not in line:
                continue
            key, val = [p.strip() for p in line.split(":", 1)]
            if key == "sim_rate_hz":
                try:
                    rate_value = float(val)
                except ValueError:
                    rate_value = None
                break
    return rate_value


def main():
    parser = argparse.ArgumentParser(description="UDP IMU packet simulator")
    parser.add_argument("--ip", default="127.0.0.1", help="Target IP")
    parser.add_argument("--port", type=int, default=9000, help="Target UDP port")
    default_cfg = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    parser.add_argument("--config", default=default_cfg, help="Config YAML path")
    parser.add_argument("--rate", type=float, default=None, help="Send rate (Hz)")
    parser.add_argument("--ax", type=int, default=0, help="Accel X (int16 raw)")
    parser.add_argument("--ay", type=int, default=0, help="Accel Y (int16 raw)")
    parser.add_argument("--az", type=int, default=0, help="Accel Z (int16 raw)")
    parser.add_argument("--gx", type=int, default=0, help="Gyro X (int16 raw)")
    parser.add_argument("--gy", type=int, default=0, help="Gyro Y (int16 raw)")
    parser.add_argument("--gz", type=int, default=0, help="Gyro Z (int16 raw)")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = (args.ip, args.port)
    config_rate = load_config_rate(args.config)
    rate_hz = args.rate if args.rate is not None else (config_rate or 50.0)
    period = 1.0 / rate_hz if rate_hz > 0 else 0.02

    try:
        while True:
            pkt = build_packet(args.ax, args.ay, args.az, args.gx, args.gy, args.gz)
            sock.sendto(pkt, target)
            time.sleep(period)
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()
