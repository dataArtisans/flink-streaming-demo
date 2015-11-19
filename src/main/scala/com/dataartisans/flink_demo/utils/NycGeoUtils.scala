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

package com.dataartisans.flink_demo.utils

import com.dataartisans.flink_demo.datatypes.GeoPoint

/**
 * GeoUtils provides utility methods to deal with GeoPoints with locations in New York City.
 */
object NycGeoUtils {

  val LonEast: Double = -73.7
  val LonWest: Double = -74.05
  val LatNorth: Double = 41.0
  val LatSouth: Double = 40.5

  val LonWidth: Double = 74.05 - 73.7
  val LatHeight: Double = 41.0 - 40.5

  val DeltaLon: Double = 0.0014
  val DeltaLat: Double = 0.00125

  val CellCntX: Int = 250
  val CellCntY: Int = 400

  /**
   * Checks if a location specified by longitude and latitude values is
   * within the geo boundaries of New York City.
   *
   * @param point the geo point to check
   *
   * @return true if the location is within NYC boundaries, otherwise false.
   */
  def isInNYC(point: GeoPoint): Boolean = {
    if(point.lon > LonEast || point.lon < LonWest)
      false
    if(point.lat > LatNorth || point.lat < LatSouth)
      false
    true
  }

  /**
   * Maps a location specified as GeoPoint to a cell of a grid covering the area of NYC.
   * The grid cells are roughly 100 x 100 m and sequentially number from north-west
   * to south-east starting by zero.
   *
   * @param point the geo point to map
   *
   * @return id of mapped grid cell.
   */
  def mapToGridCell(point: GeoPoint): Int = {
    val xIndex: Int = Math.floor((Math.abs(LonWest) - Math.abs(point.lon)) / DeltaLon).toInt
    val yIndex: Int = Math.floor((LatNorth - point.lat) / DeltaLat).toInt
    xIndex + (yIndex * CellCntX)
  }

  /**
   * Returns the center of a grid cell as a GeoPoint
   *
   * @param gridCellId The grid cell.
   *
   * @return The cell's center as GeoPoint
   */
  def getGridCellCenter(gridCellId: Int): GeoPoint = {
    val xIndex: Int = gridCellId % CellCntX
    val lon = (Math.abs(LonWest) - (xIndex * DeltaLon) - (DeltaLon / 2)).toFloat * -1.0f

    val yIndex: Int = (gridCellId - xIndex) / CellCntX
    val lat = (LatNorth - (yIndex * DeltaLat) - (DeltaLat / 2)).toFloat

    new GeoPoint(lon, lat)
  }

}
