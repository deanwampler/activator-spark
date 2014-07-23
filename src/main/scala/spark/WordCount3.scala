package spark

import collection.Map
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.util.CommandLineOptions
import spark.util.Timestamp.now

/** Second implementation of Word Count that makes the following changes:
 * <ol>
 * <li>A simpler approach is used for the algorithm.</li>
 * <li>A CommandLineOptions library is used.</li>
 * <li>The handling of the per-line data format is refined.</li>
 * </ol> */
object WordCount3 extends App {
    val argz = CommandLineOptions(
      getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-wc3"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)(args.toList)

    val sc = new SparkContext(argz("master").toString, "Word Count 3")
    try {
      val text: RDD[String] = (for {
        line: String <- sc.textFile(argz("input-path").toString)
        word: String = line.split("\\s*\\|\\s*").last
      } yield word.toLowerCase).cache()

      // Split on non-alphanumeric sequences of character as before.
      // Rather than transform into "(word, 1)" tuples, handle the words by collation value and count the unique occurrences.
      val wc2: Map[String, Long] = (for {
          token: String <- text
          word: String <- token.split("""\W+""")
        } yield word).countByValue()

      // Save to a file, but because we no longer have an RDD, we have to use Java File IO.
      // Because that the output specifier is now a file, not a directory as before, the format of each line will be different,
      // and the order of the output will not be the same, either.
      val outpath = s"${argz("output-path")}-$now"
      if (!argz("quiet").toBoolean)
        println(s"Writing output (${wc2.size} records) to: $outpath")

      val out = new java.io.PrintWriter(outpath)
      try {
        wc2 foreach {
          case (word, count) => out.println("%20s\t%d".format(word, count))
        }
      } finally {
        out.close() // flush out buffer
      }
    } finally {
      sc.stop()
    }

    // Exercise: Try different arguments for the input and output.
    //   NOTE: I've observed 0 output for some small input files!
    // Exercise: Don't discard the book names.
}
