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

package com.dataartisans.flink_demo.examples

import com.dataartisans.flink_demo.datatypes.{TaxiRide, GeoPoint}
import com.dataartisans.flink_demo.sinks.ElasticsearchUpsertSink
import com.dataartisans.flink_demo.sources.TaxiRideSource
import com.dataartisans.flink_demo.utils.{DemoStreamEnvironment, NycGeoUtils}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger.{TriggerResult, TriggerContext}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Apache Flink DataStream API demo application.
 *
 * The program processes a stream of taxi ride events from the New York City Taxi and Limousine
 * Commission (TLC).
 * It computes every five minutes for each location the total number of persons that arrived
 * within the last 15 minutes by taxi. The program emits early partial count results whenever more
 * than 50 persons (or a multitude of 50 persons) arrive at a location within 15 minutes.
 *
 * See
 *   http://github.com/dataartisans/flink-streaming-demo
 * for more detail.
 *
 */
object EarlyArrivalCount {

  def main(args: Array[String]) {

    // input parameters
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // window parameters
    val countWindowLength = 15 // window size in min
    val countWindowFrequency =  5 // window trigger interval in min
    val earlyCountThreshold = 50

    // Elasticsearch parameters
    val writeToElasticsearch = false
    val elasticsearchHost = ""
    val elasticsearchPort = -1


    // set up streaming execution environment
    val env: StreamExecutionEnvironment = DemoStreamEnvironment.env
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Define the data source
    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(
      data, maxServingDelay, servingSpeedFactor))

    val cleansedRides = rides
      // filter for trip end events
      .filter( !_.isStart )
      // filter for events in NYC
      .filter( r => NycGeoUtils.isInNYC(r.location) )

    val cellIds: DataStream[(Int, Short)] = cleansedRides
      // map location coordinates to cell Id and
      .map( r => ( NycGeoUtils.mapToGridCell(r.location), r.passengerCnt ) )

    val passengerCnts: DataStream[(Int, Long, Int)] = cellIds
      // key stream by cell Id
      .keyBy( _._1 )
      // define sliding window on keyed streams
      .timeWindow(Time.minutes(countWindowLength), Time.minutes(countWindowFrequency))
      .trigger(new EarlyCountTrigger(earlyCountThreshold))
      // count events in window
      .apply( (cell, window, events, out: Collector[(Int, Long, Int)]) => {
      out.collect((cell, window.getEnd, events.map( _._2 ).sum ))
    })

    val cntByLocation: DataStream[(Int, Long, GeoPoint, Int)] = passengerCnts
      // map cell Id back to GeoPoint
      .map( r => ( r._1, r._2, NycGeoUtils.getGridCellCenter(r._1), r._3 ) )

    // print to console
    cntByLocation
      .print()

    if (writeToElasticsearch) {
      // write to Elasticsearch
      cntByLocation
        .addSink(new CntByLocTimeUpsert(elasticsearchHost, elasticsearchPort))
    }

    env.execute("Early arrival counts per location")

  }

  class EarlyCountTrigger(triggerCnt: Int) extends Trigger[(Int, Short), TimeWindow] {

    override def onElement(
      event: (Int, Short),
      timestamp: Long,
      window: TimeWindow,
      ctx: TriggerContext): TriggerResult = {

      // register event time timer for end of window
      ctx.registerEventTimeTimer(window.getEnd)
      
      // get current count
      val personCnt = ctx.getKeyValueState[Integer]("personCnt", 0)
      // update count by passenger cnt of new event
      personCnt.update(personCnt.value() + event._2)
      // check if count is high enough for early notification
      if (personCnt.value() < triggerCnt) {
        // not yet
        TriggerResult.CONTINUE
      }
      else {
        // trigger count is reached
        personCnt.update(0)
        TriggerResult.FIRE
      }
    }

    override def onEventTime(
      time: Long,
      window: TimeWindow,
      ctx: TriggerContext): TriggerResult = {

      // trigger final computation
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(
      time: Long,
      window: TimeWindow,
      ctx: TriggerContext): TriggerResult = {

      throw new UnsupportedOperationException("I am not a processing time trigger")
    }
  }


  class CntByLocTimeUpsert(host: String, port: Int)
    extends ElasticsearchUpsertSink[(Int, Long, GeoPoint, Int)](
      host,
      port,
      "elasticsearch",
      "nyc-idx",
      "popular-locations") {

    override def insertJson(r: (Int, Long, GeoPoint, Int)): Map[String, AnyRef] = {
      Map(
        "location" -> (r._3.lat+","+r._3.lon).asInstanceOf[AnyRef],
        "time" -> r._2.asInstanceOf[AnyRef],
        "cnt" -> r._4.asInstanceOf[AnyRef]
      )
    }

    override def updateJson(r: (Int, Long, GeoPoint, Int)): Map[String, AnyRef] = {
      Map[String, AnyRef] (
        "cnt" -> r._4.asInstanceOf[AnyRef]
      )
    }

    override def indexKey(r: (Int, Long, GeoPoint, Int)): String = {
      // index by location and time
      r._1.toString + "/" + r._2.toString
    }
  }

 }

