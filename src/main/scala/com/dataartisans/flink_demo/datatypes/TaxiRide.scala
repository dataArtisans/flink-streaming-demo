/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.datatypes

import java.util.Locale

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
 * A TaxiRide describes a taxi ride event.
 * There are two types of events, a taxi ride start event and a taxi ride end event.
 * The isStart flag specifies the type of the event.
 *
 * @param rideId The id of the ride. There are two events for each id. A start and an end event.
 * @param time The time at which the event occured
 * @param isStart Flag indicating the type of the event (start or end)
 * @param location The location at which the event occurred. Either pick-up or drop-off location.
 * @param passengerCnt The number of passengers on the taxi ride
 * @param travelDist The total traveled distance for end events, -1 for start events.
 */
class TaxiRide(
               var rideId: Long,
               var time: DateTime,
               var isStart: Boolean,
               var location: GeoPoint,
               var passengerCnt: Short,
               var travelDist: Float) {

  def this() {
    this(0, new DateTime(0), false, new GeoPoint(0.0, 0.0), 0, 0.0f)
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(rideId).append(",")
    sb.append(time.toString(TaxiRide.TimeFormatter)).append(",")
    sb.append(if (isStart) "START" else "END").append(",")
    sb.append(location.lon).append(",")
    sb.append(location.lat).append(",")
    sb.append(passengerCnt).append(",")
    sb.append(travelDist)
    sb.toString()
  }

}

object TaxiRide {

  @transient
  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC

  def fromString(line: String): TaxiRide = {

    val tokens: Array[String] = line.split(",")
    if (tokens.length != 7) {
      throw new RuntimeException("Invalid record: " + line)
    }

    try {
      val rideId = tokens(0).toLong
      val time = DateTime.parse(tokens(1), TimeFormatter)
      val isStart = tokens(2) == "START"
      val lon = if (tokens(3).length > 0) tokens(3).toDouble else 0.0
      val lat = if (tokens(4).length > 0) tokens(4).toDouble else 0.0
      val passengerCnt = tokens(5).toShort
      val travelDistance = if (tokens(6).length > 0) tokens(6).toFloat else 0.0f

      new TaxiRide(rideId, time, isStart, new GeoPoint(lon, lat), passengerCnt, travelDistance)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}

/**
 * A geo point defined by a longitude and a latitude value.
 *
 * @param lon The longitude of the point.
 * @param lat The latitude of the point.
 */
case class GeoPoint(lon: Double, lat: Double)
