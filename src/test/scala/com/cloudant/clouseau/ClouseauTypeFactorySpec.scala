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
import org.apache.lucene.document.Field._
import org.apache.lucene.document.{ LatLonPoint, _ }
import org.apache.lucene.index.IndexOptions

import scala.collection.JavaConverters._

class ClouseauTypeFactorySpec extends SpecificationWithJUnit {

  "the type factory" should {

    import ClouseauTypeFactory._

    "support true for store" in {
      toStore(Map("store" -> true)) must be equalTo Store.YES
    }
    "support false for store" in {
      toStore(Map("store" -> false)) must be equalTo Store.NO
    }

    "support all enumeration values for store" in {
      for (store <- Store.values) {
        (toStore(Map("store" -> store.name)) must be equalTo
          Store.valueOf(store.name))
      }
      ok
    }

    "support all enumeration values for store (case insensitively)" in {
      for (store <- Store.values) {
        (toStore(Map("store" -> store.name.toLowerCase)) must be equalTo
          Store.valueOf(store.name))
      }
      ok
    }

    "use the default if store string is not recognized" in {
      toStore(Map("store" -> "hello")) must be equalTo Store.NO
    }

    "use the default if store value is not recognized" in {
      toStore(Map("store" -> 12)) must be equalTo Store.NO
    }

    "support true for index" in {
      toIndexOptions(Map("index" -> true)) must be equalTo IndexOptions.DOCS_AND_FREQS_AND_POSITIONS
    }

    "support false for index" in {
      toIndexOptions(Map("index" -> false)) must be equalTo IndexOptions.NONE
    }

    "support all enumeration values for index" in {
      for (indexOption <- IndexOptions.values) {
        (toIndexOptions(Map("index" -> indexOption.name)) must be equalTo
          IndexOptions.valueOf(indexOption.name))
      }
      ok
    }

    "support all enumeration values for index (case insensitively)" in {
      for (indexOption <- IndexOptions.values) {
        (toIndexOptions(Map("index" -> indexOption.name.toLowerCase)) must be equalTo
          IndexOptions.valueOf(indexOption.name))
      }
      ok
    }

    "use the default if index string is not recognized" in {
      toIndexOptions(Map("index" -> "hello")) must be equalTo IndexOptions.DOCS_AND_FREQS_AND_POSITIONS
    }

    "use the default if index value is not recognized" in {
      toIndexOptions(Map("index" -> 12)) must be equalTo IndexOptions.DOCS_AND_FREQS_AND_POSITIONS
    }

    "index two element array as LatLonPoint" in {
      val doc = new Document()
      val field0 = ("fieldName", List(1.2, 2), List[(String, Any)]())
      addFields(doc, field0)

      val fields = doc.asScala.toList
      fields.length must be equalTo 1
      fields.head must haveClass[LatLonPoint]
      ok
    }

    "index two element array as LatLonPoint, first element as longitude, second as latitude" in {
      val doc = new Document()
      val field0 = ("fieldName", List(1.2, 2), List[(String, Any)]())
      addFields(doc, field0)

      val fields = doc.asScala.toList
      fields.head.toString must be equalTo "LatLonPoint <fieldName:1.9999999646097422,1.1999999452382326>"
    }
  }
}
