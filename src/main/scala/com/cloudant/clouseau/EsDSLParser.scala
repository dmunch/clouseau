// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.cloudant.clouseau

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import org.apache.lucene.search.Query
import org.apache.lucene.document._

object EsDSLParser {
  case class GeoDistanceQuery(distance: Double, fields: List[(String, GeoPoint)])
  case class GeoPoint(lon: Double, lat: Double)

  implicit val formats = DefaultFormats
  val latLonRegExp = """^([-+]?[0-9]*\.?[0-9]+),\s*([-+]?[0-9]*\.?[0-9]+)$""".r
  val kmRegExp = """^([-+]?[0-9]*\.?[0-9]+)\s*km$""".r
  val mRegExp = """^([-+]?[0-9]*\.?[0-9]+)\s*m$""".r
  val cmRegExp = """^([-+]?[0-9]*\.?[0-9]+)\s*cm$""".r

  val geoDistanceKnownFields = List[String]("distance", "distance_type", "optimize_bbox", "_name", "ignore_malformed")

  def parseGeoQuery(geoQuery: String): GeoDistanceQuery = {
    val json = parse(geoQuery, true, true)

    val map = (json \ "geo_distance").extract[Map[String, Any]]

    val distance = map.getOrElse("distance", "").asInstanceOf[String]

    val fields = map
      .filterKeys(key => !geoDistanceKnownFields.contains(key))
      .mapValues(v => parseGeoPoint(v))

    new GeoDistanceQuery(parseDistance(distance), fields.toList)
  }

  def parseGeoPoint(geoPoint: Any): GeoPoint = geoPoint match {
    case list: List[scala.math.BigInt] => new GeoPoint(list.head.toDouble, list.tail.head.toDouble)
    case list: List[scala.math.BigDecimal] => new GeoPoint(list.head.toDouble, list.tail.head.toDouble)
    case map: Map[String, Any] => {
      val lat = map.get("lat") match {
        case Some(value: scala.math.BigInt) => value.toDouble
        case Some(value: scala.math.BigDecimal) => value.toDouble
        case _ => throw new IllegalArgumentException("Can't parse lat")
      }

      val lon = map.get("lon") match {
        case Some(value: scala.math.BigInt) => value.toDouble
        case Some(value: scala.math.BigDecimal) => value.toDouble
        case _ => throw new IllegalArgumentException("Can't parse lon")
      }
      new GeoPoint(lon, lat)
    }
    case latLonRegExp(lon, lat) => new GeoPoint(lon.toDouble, lat.toDouble)
    case _ => throw new IllegalArgumentException("Can't parse geopoint")
  }

  def parseDistance(distance: String) : Double = distance match {
    case kmRegExp(km) => km.toDouble * 1000
    case mRegExp(m) => m.toDouble
    case cmRegExp(cm) => cm.toDouble * 0.01
    case _ => throw new IllegalArgumentException("Can't parse distance")
  }

  def toQuery(geoDistanceQuery: GeoDistanceQuery) : Query = {
    val (name:String, point: GeoPoint) = geoDistanceQuery.fields.head
    LatLonPoint.newDistanceQuery(name, point.lat, point.lon, geoDistanceQuery.distance)
  }

  def toQuery(geoQuery: String) : Option[Query] = {
    try
    {
      Some(toQuery(parseGeoQuery((geoQuery))))
    } catch
    {
      case e: Exception => None
    }
  }
}
