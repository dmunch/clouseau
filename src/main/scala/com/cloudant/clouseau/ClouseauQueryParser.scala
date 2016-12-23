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

import java.util.regex.Pattern

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.LegacyNumericRangeQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.TermQuery
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.document.DoublePoint

class ClouseauQueryParser(defaultField: String,
                          analyzer: Analyzer)
    extends QueryParser(defaultField, analyzer) {

  // regexp from java.lang.Double
  val Digits = "(\\p{Digit}+)"
  val HexDigits = "(\\p{XDigit}+)"
  val Exp = "[eE][+-]?" + Digits
  val fpRegexStr = ("[\\x00-\\x20]*" + "[+-]?(" + "NaN|"
    + "Infinity|" + "((("
    + Digits
    + "(\\.)?("
    + Digits
    + "?)("
    + Exp
    + ")?)|"
    + "(\\.("
    + Digits
    + ")("
    + Exp
    + ")?)|"
    + "(("
    + "(0[xX]"
    + HexDigits
    + "(\\.)?)|"
    + "(0[xX]"
    + HexDigits
    + "?(\\.)"
    + HexDigits
    + ")"
    + ")[pP][+-]?" + Digits + "))" + "[fFdD]?))" + "[\\x00-\\x20]*")
  val fpRegex = Pattern.compile(fpRegexStr)

  override def getRangeQuery(field: String,
                             lowerStr: String,
                             upperStr: String,
                             startInclusive: Boolean,
                             endInclusive: Boolean): Query = {
    if (isNumber(lowerStr) && isNumber(upperStr)) {
      val lower = if (startInclusive) lowerStr.toDouble else Math.nextUp(lowerStr.toDouble)
      val upper = if (endInclusive) upperStr.toDouble else Math.nextDown(upperStr.toDouble)

      DoublePoint.newRangeQuery(field, lower, upper)
    } else {
      setLowercaseExpandedTerms(field)
      super.getRangeQuery(field, lowerStr, upperStr, startInclusive, endInclusive)
    }
  }

  override def getFieldQuery(field: String,
                             queryText: String,
                             quoted: Boolean): Query = {
    if (!quoted && isNumber(queryText)) {
      new TermQuery(Utils.doubleToTerm(field, queryText.toDouble))
    } else {
      super.getFieldQuery(field, queryText, quoted)
    }
  }

  override def getFuzzyQuery(field: String, termStr: String,
                             minSimilarity: Float): Query = {
    setLowercaseExpandedTerms(field)
    super.getFuzzyQuery(field, termStr, minSimilarity)
  }

  override def getPrefixQuery(field: String, termStr: String): Query = {
    setLowercaseExpandedTerms(field)
    super.getPrefixQuery(field, termStr)
  }

  override def getRegexpQuery(field: String, termStr: String): Query = {
    setLowercaseExpandedTerms(field)
    super.getRegexpQuery(field, termStr)
  }

  override def getWildcardQuery(field: String, termStr: String): Query = {
    setLowercaseExpandedTerms(field)
    super.getWildcardQuery(field, termStr)
  }

  protected def isNumber(str: String): Boolean = {
    fpRegex.matcher(str).matches()
  }

  protected def setLowercaseExpandedTerms(field: String) {
    getAnalyzer match {
      case a: PerFieldAnalyzer =>
        setLowercaseExpandedTerms(a.getWrappedAnalyzer(field))
      case _: Analyzer =>
        setLowercaseExpandedTerms(analyzer)
    }
  }

  private def setLowercaseExpandedTerms(analyzer: Analyzer) {
    setLowercaseExpandedTerms(!analyzer.isInstanceOf[KeywordAnalyzer])
  }

}

//Currently needed to transport lower, upper, startInclusive and endInclusive
//to facet range query. 
class LegacyClouseauQueryParser(defaultField: String,
                                analyzer: Analyzer)
    extends ClouseauQueryParser(defaultField, analyzer) {

  override def getRangeQuery(field: String,
                             lowerStr: String,
                             upperStr: String,
                             startInclusive: Boolean,
                             endInclusive: Boolean): Query = {
    if (isNumber(lowerStr) && isNumber(upperStr)) {
      LegacyNumericRangeQuery.newDoubleRange(field, 8, lowerStr.toDouble,
        upperStr.toDouble, startInclusive, endInclusive)
    } else {
      setLowercaseExpandedTerms(field)
      super.getRangeQuery(field, lowerStr, upperStr, startInclusive, endInclusive)
    }
  }
}