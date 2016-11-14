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

import java.util.{ Set => JSet }
import org.apache.log4j.Logger
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.CharArraySet
import scala.collection.JavaConversions._

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.ar.ArabicAnalyzer
import org.apache.lucene.analysis.bg.BulgarianAnalyzer
import org.apache.lucene.analysis.br.BrazilianAnalyzer
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer
import org.apache.lucene.analysis.cz.CzechAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.el.GreekAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.eu.BasqueAnalyzer
import org.apache.lucene.analysis.fa.PersianAnalyzer
import org.apache.lucene.analysis.fi.FinnishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.ga.IrishAnalyzer
import org.apache.lucene.analysis.gl.GalicianAnalyzer
import org.apache.lucene.analysis.hi.HindiAnalyzer
import org.apache.lucene.analysis.hu.HungarianAnalyzer
import org.apache.lucene.analysis.hy.ArmenianAnalyzer
import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.ja.JapaneseAnalyzer
import org.apache.lucene.analysis.lv.LatvianAnalyzer
import org.apache.lucene.analysis.nl.DutchAnalyzer
import org.apache.lucene.analysis.no.NorwegianAnalyzer
import org.apache.lucene.analysis.pl.PolishAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.ro.RomanianAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.standard.ClassicAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer
import org.apache.lucene.analysis.sv.SwedishAnalyzer
import org.apache.lucene.analysis.th.ThaiAnalyzer
import org.apache.lucene.analysis.tr.TurkishAnalyzer

// Extras
import org.apache.lucene.analysis.ja.JapaneseTokenizer

object SupportedAnalyzers {

  val logger = Logger.getLogger("clouseau.analyzers")

  def createAnalyzer(options: Any): Option[Analyzer] = {
    createAnalyzerInt(options) match {
      case Some(perfield: PerFieldAnalyzer) =>
        Some(perfield)
      case Some(analyzer: Analyzer) =>
        Some(new PerFieldAnalyzer(analyzer,
          Map("_id" -> new KeywordAnalyzer())))
      case None =>
        None
    }
  }

  def createAnalyzerInt(options: Any): Option[Analyzer] = options match {
    case name: String =>
      createAnalyzerInt(Map("name" -> name))
    case list: List[(String, Any)] =>
      try {
        createAnalyzerInt(list.toMap)
      } catch {
        case e: ClassCastException => None
      }
    case map: Map[String, Any] =>
      map.get("name") match {
        case Some(name: String) =>
          createAnalyzerInt(name, map)
        case None =>
          None
      }
    case _ =>
      None
  }

  def createAnalyzerInt(name: String, options: Map[String, Any]): Option[Analyzer] = name match {
    case "keyword" =>
      Some(new KeywordAnalyzer())
    case "simple" =>
      Some(new SimpleAnalyzer())
    case "whitespace" =>
      Some(new WhitespaceAnalyzer())
    case "arabic" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new ArabicAnalyzer(stopwords))
        case _ =>
          Some(new ArabicAnalyzer())
      }
    case "bulgarian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new BulgarianAnalyzer(stopwords))
        case _ =>
          Some(new BulgarianAnalyzer())
      }
    case "brazilian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new BrazilianAnalyzer(stopwords))
        case _ =>
          Some(new BrazilianAnalyzer())
      }
    case "catalan" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new CatalanAnalyzer(stopwords))
        case _ =>
          Some(new CatalanAnalyzer())
      }
    case "cjk" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new CJKAnalyzer(stopwords))
        case _ =>
          Some(new CJKAnalyzer())
      }
    case "chinese" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new SmartChineseAnalyzer(stopwords))
        case _ =>
          Some(new SmartChineseAnalyzer())
      }
    case "czech" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new CzechAnalyzer(stopwords))
        case _ =>
          Some(new CzechAnalyzer())
      }
    case "danish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new DanishAnalyzer(stopwords))
        case _ =>
          Some(new DanishAnalyzer())
      }
    case "german" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new GermanAnalyzer(stopwords))
        case _ =>
          Some(new GermanAnalyzer())
      }
    case "greek" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new GreekAnalyzer(stopwords))
        case _ =>
          Some(new GreekAnalyzer())
      }
    case "english" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new EnglishAnalyzer(stopwords))
        case _ =>
          Some(new EnglishAnalyzer())
      }
    case "spanish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new SpanishAnalyzer(stopwords))
        case _ =>
          Some(new SpanishAnalyzer())
      }
    case "basque" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new BasqueAnalyzer(stopwords))
        case _ =>
          Some(new BasqueAnalyzer())
      }
    case "persian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new PersianAnalyzer(stopwords))
        case _ =>
          Some(new PersianAnalyzer())
      }
    case "finnish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new FinnishAnalyzer(stopwords))
        case _ =>
          Some(new FinnishAnalyzer())
      }
    case "french" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new FrenchAnalyzer(stopwords))
        case _ =>
          Some(new FrenchAnalyzer())
      }
    case "irish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new IrishAnalyzer(stopwords))
        case _ =>
          Some(new IrishAnalyzer())
      }
    case "galician" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new GalicianAnalyzer(stopwords))
        case _ =>
          Some(new GalicianAnalyzer())
      }
    case "hindi" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new HindiAnalyzer(stopwords))
        case _ =>
          Some(new HindiAnalyzer())
      }
    case "hungarian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new HungarianAnalyzer(stopwords))
        case _ =>
          Some(new HungarianAnalyzer())
      }
    case "armenian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new ArmenianAnalyzer(stopwords))
        case _ =>
          Some(new ArmenianAnalyzer())
      }
    case "indonesian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new IndonesianAnalyzer(stopwords))
        case _ =>
          Some(new IndonesianAnalyzer())
      }
    case "italian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new ItalianAnalyzer(stopwords))
        case _ =>
          Some(new ItalianAnalyzer())
      }
    case "japanese" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new JapaneseAnalyzer(null, JapaneseTokenizer.DEFAULT_MODE, stopwords, JapaneseAnalyzer.getDefaultStopTags))
        case _ =>
          Some(new JapaneseAnalyzer())
      }
    case "latvian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new LatvianAnalyzer(stopwords))
        case _ =>
          Some(new LatvianAnalyzer())
      }
    case "dutch" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new DutchAnalyzer(stopwords))
        case _ =>
          Some(new DutchAnalyzer())
      }
    case "norwegian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new NorwegianAnalyzer(stopwords))
        case _ =>
          Some(new NorwegianAnalyzer())
      }
    case "polish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new PolishAnalyzer(stopwords))
        case _ =>
          Some(new PolishAnalyzer())
      }
    case "portuguese" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new PortugueseAnalyzer(stopwords))
        case _ =>
          Some(new PortugueseAnalyzer())
      }
    case "romanian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new RomanianAnalyzer(stopwords))
        case _ =>
          Some(new RomanianAnalyzer())
      }
    case "russian" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new RussianAnalyzer(stopwords))
        case _ =>
          Some(new RussianAnalyzer())
      }
    case "classic" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new ClassicAnalyzer(stopwords))
        case _ =>
          Some(new ClassicAnalyzer())
      }
    case "standard" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new StandardAnalyzer(stopwords))
        case _ =>
          Some(new StandardAnalyzer())
      }
    case "email" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new UAX29URLEmailAnalyzer(stopwords))
        case _ =>
          Some(new UAX29URLEmailAnalyzer())
      }
    case "perfield" =>
      val fallbackAnalyzer = new StandardAnalyzer()
      val defaultAnalyzer: Analyzer = options.get("default") match {
        case Some(defaultOptions) =>
          createAnalyzerInt(defaultOptions) match {
            case Some(defaultAnalyzer1) =>
              defaultAnalyzer1
            case None =>
              fallbackAnalyzer
          }
        case None =>
          fallbackAnalyzer
      }
      var fieldMap: Map[String, Analyzer] = options.get("fields") match {
        case Some(fields: List[(String, Any)]) =>
          fields map { kv =>
            createAnalyzerInt(kv._2) match {
              case Some(fieldAnalyzer) =>
                (kv._1, fieldAnalyzer)
              case None =>
                (kv._1, defaultAnalyzer)
            }
          } toMap
        case _ =>
          Map.empty
      }
      fieldMap += ("_id" -> new KeywordAnalyzer())
      Some(new PerFieldAnalyzer(defaultAnalyzer, fieldMap))
    case "swedish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new SwedishAnalyzer(stopwords))
        case _ =>
          Some(new SwedishAnalyzer())
      }
    case "thai" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new ThaiAnalyzer(stopwords))
        case _ =>
          Some(new ThaiAnalyzer())
      }
    case "turkish" =>
      options.get("stopwords") match {
        case Some(stopwords: List[String]) =>
          Some(new TurkishAnalyzer(stopwords))
        case _ =>
          Some(new TurkishAnalyzer())
      }
    case _ =>
      None
  }

  implicit def listToJavaSet(list: List[String]): JSet[String] = {
    Set() ++ list
  }

  implicit def listToCharArraySet(list: List[String]): CharArraySet = {
    CharArraySet.unmodifiableSet(CharArraySet.copy(Set() ++ list))
  }

}
