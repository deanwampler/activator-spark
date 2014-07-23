package spark

import spark.util.Timestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** First implementation of Word Count. */
object WordCount2 extends App {
    val sc = new SparkContext("local", "Word Count 2")
    try {
      val wc: RDD[(String, Int)] = (for {
        lineMixedCase: String <- sc.textFile("data/kjvdat.txt") // Load the King James Version of the Bible
        word <- lineMixedCase.toLowerCase.split("""\W+""")
      } yield (word, 1)).reduceByKey(_ + _)

      // Write Hadoop-style output to a directory, with a _SUCCESS marker (empty) file, the data as a "part" file, and checksum files.
      // Append a timestamp because Spark, like Hadoop, won't overwrite an existing directory, e.g., from a prior run.
      val out = s"output/kjv-wc2-${Timestamp.now}"
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }

    // Exercise: Use other versions of the Bible:
    //   The data directory contains similar files for the Tanach (t3utf.dat - in Hebrew),
    //   the Latin Vulgate (vuldat.txt), the Septuagint (sept.txt - Greek)
    // Exercise: See the Scaladoc page for `OrderedRDDFunctions`:
    //   http://spark.apache.org/docs/0.9.0/api/core/index.html#org.apache.spark.rdd.OrderedRDDFunctions
    //   Sort the output by word, try both ascending and descending.
    //   Note this can be expensive for large data sets!
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
