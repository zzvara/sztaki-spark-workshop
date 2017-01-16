package hu.sztaki.tutorial.spark

import hu.sztaki.tutorial.spark.AdvancedRDD.RichRDD
import hu.sztaki.tutorial.spark.model.{BigramsWithSameStart, Bigram}
import org.apache.spark.{SparkConf, SparkContext}

object BigramAnalysis{
  def main (args: Array[String]){
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("Bigram analysis")
        .setMaster("local[5]")
    )

    val file = sc.textFile(args(0))

    /**
     * @todo[1] Transform to `Bigram`s and filter invalid items.
     * @hint Use the companion object of `model.Bigram`.
     */
    //val bigramsRDD = file.map(line => Bigram(line))
    val bigramsRDD = file.flatMap(Bigram(_))

    /**
     * @todo[2] Total number of bigrams
     */

    /**
     * @todo[3] Cache bigrams.
     */

    /**
     * @todo[4] How many unique bigrams do we have?
     */

    /**
     * @todo[5] Count each element.
     * @hint Use the AdvancedRDD (implicitly).
     */
    // val bgOccrCount = _

    /**
     * @todo[6] Number of bigrams that appear only once.
     */

    /**
     * @todo[7] List the top ten most frequent bigrams and their counts.
     */
    // val bgOccrCountSorted = _

    /**
     * @todo[8] What fraction of all bigrams occurrences does the top ten bigrams account for?
     *          That is, what is the cumulative frequency of the top ten bigrams?
     */
    // val totalBgCount = _

    // val topTenBgOccrCount = _

    // val fractionTopTenBigramOccurance = _

    /**
     * @todo[9*] Determine the frequency of bigrams with the same start.
     * @hint Use `BigramsWithSameStart` and aggregateByKey also.
     */
    // val startingWordBigram = _

    // val startingWordAllBigrams = _

    // val startWordBGCount = _

    // val startWordBGAndBGCount = _

    //  [(String, ((String, Int)), Int)]
    //  startWord - BG           - BG count    - bgs starting with word
    //  a._1      - a._2._1._1   - a._2._1._2  - a._2._2
    // val startWordBGbGCountStartWordBgsCount = _

    /**
     * @todo[10] What are the five most frequent words following the word "light"?
     * @todo[11] What is the frequency of observing each word?
     */

    /**
     * If there are a total of N words in your vocabulary,
     * then there are a total of N2 possible values for F(Wn|Wn-1)—in theory,
     * every word can follow every other word (including itself).
     * @todo[12] What fraction of these values are non-zero?
     * In other words, what proportion of all possible events are actually observed?
     * To give a concrete example, let's say that following the word "happy",
     * you only observe 100 different words in the text collection.
     * This means that N-100 words are never seen after "happy".
     * (Perhaps the distribution of happiness is quite limited?).
     */
    // val allWords = _
    // val allDistinctWordsCount = _
    // val totalPossibleCombinations = _

    // val allDistinctBigrams = _

    // val fractionOfBgsFoundOutOFTotalPossible = _
  }
}


