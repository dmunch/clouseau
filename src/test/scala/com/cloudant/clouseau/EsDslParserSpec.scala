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

import org.specs2.mutable.SpecificationWithJUnit

class EsDslParserSpec extends SpecificationWithJUnit {
  "the DSL parser" should {

    import com.cloudant.clouseau.EsDSLParser._

    "parse geo_distance queries with lat lon as properties" in {
      val query = parseGeoQuery(List(("geo_distance", List(("distance", "12km"), ("fieldName", List(("lat", 40), ("lon", -70)))))))
      query must be like {
        case GeoDistanceQuery(12000, List(("fieldName", GeoPoint(-70, 40)))) => ok
      }
    }

    "parse geo_distance queries with lat lon as array" in {
      val query = parseGeoQuery(List(("geo_distance", List(("distance", "12km"), ("fieldName", List(-70.1, 40))))))
      query must be like {
        case GeoDistanceQuery(12000, List(("fieldName", GeoPoint(-70.1, 40)))) => ok
      }
    }

    "parse geo_distance queries with lat lon as string" in {
      val query = parseGeoQuery(List(("geo_distance", List(("distance", "12km"), ("fieldName", "-70.1, 40")))))
      query must be like {
        case GeoDistanceQuery(12000, List(("fieldName", GeoPoint(-70.1, 40)))) => ok
      }
    }

    "parse geo_distance queries with multiple fields" in {
      val query = parseGeoQuery(List(("geo_distance", List(
        ("distance", "12km"),
        ("fieldName1", "-70, 40"),
        ("fieldName2", List(("lat", 41.1), ("lon", -70.2))),
        ("fieldName3", List(-73.1, 40.4))
      ))))

      query must be like {
        case GeoDistanceQuery(12000, List(
          ("fieldName1", GeoPoint(-70, 40)),
          ("fieldName2", GeoPoint(-70.2, 41.1)),
          ("fieldName3", GeoPoint(-73.1, 40.4))
          )) => ok
      }
    }

    "parse distance in km" in {
      parseDistance("1km") must be equalTo 1000
    }

    "parse distance in m" in {
      parseDistance("1m") must be equalTo 1
    }

    "parse distance in cm" in {
      parseDistance("1cm") must be equalTo 0.01
    }
  }
}
