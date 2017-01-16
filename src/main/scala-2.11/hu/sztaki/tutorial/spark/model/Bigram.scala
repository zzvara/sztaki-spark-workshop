package hu.sztaki.tutorial.spark.model

case class Bigram(firstWord: String, secondWord: String) {
  def isValidBigram: Boolean = !firstWord.equals(" ") && !secondWord.equals(" ")
}

case class BigramsWithSameStart(firstWord: String, bigrams: List[String]) {
  def bigramsCount = bigrams.size

  def merge(bg2: BigramsWithSameStart): BigramsWithSameStart = {
    new BigramsWithSameStart(firstWord, bigrams ++ bg2.bigrams)
  }
}

object BigramsWithSameStart {
  def apply(bigram: Bigram) = {
    new BigramsWithSameStart(bigram.firstWord, List(bigram.secondWord))
  }
}

object Bigram {
  /**
   * @todo Complete function.
   */
  def apply(line: String): List[Bigram] = {
    val words = line.split(" ")
    if (words.size < 2) List()
    else words.sliding(2).map {
           array => Bigram(array(0), array(1))
         }.toList
  }
}