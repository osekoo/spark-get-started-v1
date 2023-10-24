import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.deepFunctorOps
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.allText
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
 * Implements Word Count using Spark SQL
 */
object Main {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  @transient private lazy val logger: Logger = Logger.getLogger("$")

  def main(args: Array[String]): Unit = {
    logger.info("Initializing spark context...")
    val spark: SparkSession = SparkSession.builder()
      .appName(s"WordCount")
      .getOrCreate()

    val filePaths = System.getenv("APP_FILES")
      .split(",")
    var allContent = ""
    for (filePath <- filePaths) {
      logger.info(s"loading text from $filePath ...")
      allContent += readContent(filePath) + "\n"
    }
    val data = spark.createDataFrame(Seq((0, allContent)))
      .toDF("id", "sentence")

    logger.info("tokenizing the input text...")
    val tokenizer = new RegexTokenizer().setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("[a-z]+").setGaps(false) // Set up words filtering regex. We only keep words

    logger.info("removing stop words from the dataframe...")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("word")

    // transformer pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover))
    logger.info("transforming the input text...")
    val model = pipeline.fit(data)
    val words = model.transform(data)
    words.show(100, truncate = true)

    logger.info("exploding words into rows...")
    val tokens = words.select(explode(col("word")).as("token"))
      .where(length(col("token")) > 1)
    logger.info("Nb words: " + tokens.count())
    tokens.persist()
    tokens.show(false)

    logger.info("counting the occurrence of each word...")
    val wordCountDf = tokens
      .groupBy("token") // counting the words using groupBy() instruction
      .count()
      .orderBy(desc("count"))
      .persist()
    logger.info("Nb of tokens: " + wordCountDf.count())

    logger.info("displaying the dataframe...")
    wordCountDf.show(100, truncate = false)

    logger.info("saving the dataframe in csv format...")
    wordCountDf.write.mode(SaveMode.Overwrite)
      .csv("./tokens.csv")

    logger.info("saving the dataframe in parquet format...")
    wordCountDf.write.mode(SaveMode.Overwrite)
      .parquet("./tokens.parquet")
  }

  private def readContent(path: String): String = {
    if (path.startsWith("http")) {
      val s = Source.fromURL(path, "UTF-8")
      JsoupBrowser().parseString(s.mkString) >> allText
    } else {
      val s = Source.fromFile(path, "UTF-8")
      s.mkString
    }
  }
}
