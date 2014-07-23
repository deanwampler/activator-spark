package spark.solns

import spark.util.Timestamp.now
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** Variation of Word Count with GroupBy 
  * To run: `sbt 'runMain spark.solns.WordCount2GroupBy'` */
object WordCount2GroupBy extends App {
    val sc = new SparkContext("local", "Word Count 2 with GroupBy")
    try {
      val wc: RDD[(Int, Iterable[(String, Int)])] = (for {
        line: String <- sc.textFile("data/kjvdat.txt")
        word: String <- line.split("""\W+""")
      } yield (word.toLowerCase, 1))
        .cache()
        .reduceByKey(_ + _) // sum values, which are all ones
        .groupBy(_._2)   // group by the counts
        .sortByKey(ascending=true)

      // Write Hadoop-style output; to a directory, with a _SUCCESS marker (empty) file, the data as a "part" file, and checksum files.
      // The output has very long lines for the many words that have very low counts (e.g., 1),
      // while very frequent words (counts in the 1000s) usually don't overlap, e.g., "for" has 8971 occurrences, while "unto" has 8997, so they have
      // nearly the same frequency, but not the exact same, so they aren't grouped together.
      val out = s"output/kjv-wc2-group-by-count-$now"
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }

    // Exercise: See the Scaladoc page for `OrderedRDDFunctions`:
    //   http://spark.apache.org/docs/0.9.0/api/core/index.html#org.apache.spark.rdd.OrderedRDDFunctions
    //   Sort the output by word, try both ascending and descending.
    //   Note this can be expensive!
    // Exercise: Take the output from the previous exercise and count the number
    //   of words that start with each letter of the alphabet and each digit.
    // Exercise (Hard): Sort the output by count. You can't use the same
    //   approach as in the previous exercise. Hint: See RDD.keyBy
    //   (http://spark.apache.org/docs/0.9.0/api/core/index.html#org.apache.spark.rdd.RDD)
    //   What's the most frequent word that isn't a "stop word".
    // Exercise (Hard): Group the word-count pairs by count. In other words,
    //   All pairs where the count is 1 are together (i.e., just one occurrence
    //   of those words was found), all pairs where the count is 2, etc. Sort
    //   ascending or descending. Hint: Is there a method for grouping?
    // Exercise (Thought Experiment): Consider the size of each group created
    //   in the previous exercise and the distribution of those sizes vs. counts.
    //   What characteristics would you expect for this distribution? That is,
    //   which words (or kinds of words) would you expect to occur most
    //   frequently? What kind of distribution fits the counts (numbers)?
}
