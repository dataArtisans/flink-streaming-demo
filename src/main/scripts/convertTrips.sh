#!/bin/bash

# Copyright 2015 data Artisans GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script converts a public data set about taxi rides in New York City into a data set of
# taxi ride events. The original data is published by the NYC Taxi and Limousine Commission (TLC) at
#
#   http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
#
# The script parses CSV input and converts each line (which represents a taxi trip) into two
# taxi ride events: a start event and an end event.
# The resulting events have the following schema:
#   rideId: Long        // unique id for each ride
#   time: DateTime      // timestamp of the start/end event
#   isStart: Boolean    // true = ride start, false = ride end
#   lon: Float          // longitude of pick-up/drop-off location
#   lat: Float          // latitude of pick-up/drop-off location
#   passengerCnt: Int   // number of passengers
#   travelDist: Float   // total travel distance, -1 on start events
#
# usage: just pipe raw data into script

tr -d '\r' | awk -F "," '{print NR "," $2 ",START," $6 "," $7 "," $4 ",-1"}{print NR "," $3 ",END," $10 "," $11 "," $4 "," $5}' | sort -t ',' -k 2 -S "2G" < /dev/stdin