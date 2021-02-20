# Copyright 2020 Box Robotics, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Joins multiple bags into a single bag file"""

import argparse
import glob
import os
import sys
import cv2
import datetime
import rclpy
from rclpy.time import Time
from cv_bridge import CvBridge
from baggie import BagReader, BagWriter

def get_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Joins several ROS 2 bag files into a single combined bag")

    parser.add_argument("-o", "--outfile", type=str, required=True,
                        help="The output bag file name to create")
    parser.add_argument("--start_unixtime", required=True, type=float,
                        help="Earliest message stamp in output bag (seconds)")
    parser.add_argument("--topic_name", required=True, type=str,
                        help="output topic name")
    parser.add_argument("infiles", metavar="INFILE", type=str, nargs="+",
                        help="The input mp4 files to join")
    compression_group = parser.add_mutually_exclusive_group()
    compression_group.add_argument("--compress", required=False,
                                   action="store_true", default=False,
                                   help="Compress the output file")
    compression_group.add_argument("--uncompress", required=False,
                                   action="store_true", default=False,
                                   help="Do not compress the output file")

    args = parser.parse_args(sys.argv[1:])
    return args

def main(args=None):
    args = get_args()

    infiles_ = []
    for infile in args.infiles:
        paths = glob.glob(infile)
        for path in paths:
            infiles_.append(path)

    infiles = sorted(set(infiles_))

    def _calculate_current_time(cap, start_unixtime) -> Time:
        current_msec = cap.get(cv2.CAP_PROP_POS_MSEC)
        return Time(seconds=current_msec / 1000 + start_unixtime)

    def _parse_unixtime_from_file_name(file_name) -> float:
        dt = datetime.datetime.strptime(file_name+"+09:00", "camera_%Y_%m_%d-%H_%M_%S.mp4%z")
        stamp = dt.timestamp()
        return stamp

    with BagWriter(args.outfile, compress=args.compress) as outbag:
        topic = args.topic_name
        for infile in infiles:
            start_unixtime = _parse_unixtime_from_file_name(infile)
            cap = cv2.VideoCapture(infile)
            bridge = CvBridge()
            while cap.isOpened():
                status, frame = cap.read()
                t = _calculate_current_time(cap, start_unixtime)
                if not status:
                    break
                msg = bridge.cv2_to_imgmsg(frame)
                msg.header.stamp = t.to_msg()
                msg.header.frame_id = 'camera'
                outbag.write(topic, msg, t)

    return 0

if __name__ == '__main__':
    sys.exit(main())
