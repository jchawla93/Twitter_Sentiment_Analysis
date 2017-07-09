package main.scala
import java.io.FileWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import scala.collection.mutable.ListBuffer
//import org.apache.spark-project.guava.collect.MapMaker
import scala.collection.mutable.ArrayBuffer
/**
  * Created by jiteshchawla on 9/19/16.
  */
object TFDF {
  def main(args: Array[String]): Unit = {

    //    val inputFile = "/Users/jiteshchawla/IdeaProjects/abc/src/main/scala/trial_1.chawla_jitesh_tweets_first20.txt.rtf"
    //    val outputFile = "/Users/jiteshchawla/IdeaProjects/abc/src/main/scala/chawla_jitesh_tweets_sentiment_first20.txt"
    val sparkConf = new SparkConf().setAppName("TFDF").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(sparkConf)
    // Load our input data.
    val sql_cn = new SQLContext(sc)
    val input = sc.textFile(args(0))
    val temp1 = sql_cn.read.json(input)
    temp1.registerTempTable("tweets_text")
    val tweets_20 = sql_cn.sql("SELECT text FROM tweets_text ").collectAsList()
    val tweets_20_complete = sql_cn.sql("SELECT * FROM tweets_text ").collectAsList()
    val cw = new FileWriter("chawla_jitesh_tweets_tfdf_first20.txt")
    val raw = ArrayBuffer[String]()
    val tweets = ArrayBuffer[List[String]]()
    val tweets_2 = ArrayBuffer[List[String]]()
    for (h <- 0 until tweets_20.size()) {
      raw.insert(h, tweets_20.get(h).toString())
    }
    var regularEx = "^\\_|…|(#(.+?)(?=[\\s.,:,]|$))|(@RT | RT|\\[RT(.+?)(?=[\\s.,:,]|$))|((([A-Za-z]{3,9}:(?:\\/\\/)?)(?:[-;:&=\\+\\$,\\w]+@)?[A-Za-z0-9.-]+|(?:www.|[-;:&=\\+\\$,\\w]+@)[A-Za-z0-9.-]+)((?:\\/[\\+~%\\/.\\w-_]*)?\\??(?:[-\\+=&;%@.\\w_]*)#?(?:[\\w]*))?)|(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9]+)|[$&+,:;=?@#|'\"\\/<>.^*()%!-]|\\[|\\]".r
    for (i <- 0 until raw.length) {
      raw(i) = regularEx.replaceAllIn(raw(i), "")
      val tweet_split = raw(i).toLowerCase.split("""\s""")
      tweets.insert(i, tweet_split.toList)
      tweets.filter(_.nonEmpty)
      tweets_2.insert(i, tweet_split.toList)
      tweets_2.filter(_.nonEmpty)
    }
    var count_tf = 0
    var count_df = 0
    var word = new String
    var indexArray = new ArrayBuffer[Int]()
    var countArray = new ArrayBuffer[Int]()
    var listTF: List[(String, Int)] = List()
    for (q <- 0 until tweets.length) {
      for (w <- 0 until tweets(q).length) {
        word = tweets(q)(w).toLowerCase
        for (e <- 0 until tweets(q).length) {
          if (word == tweets_2(q)(e)) {
            count_tf += 1
            indexArray += q
            listTF ++= List((word, q + 1))
          }
        }
        countArray += count_tf
        count_tf = 0
      }
    }
    var listTF1: List[(Any, Int)] = List()
    listTF = listTF.distinct
    for (m <- 0 to listTF.length - 1) {

      listTF1 ++= List((listTF(m), countArray(m)))
    }
    val finalCount = new ArrayBuffer[String]()
    for (q <- 0 until tweets.length) {
      for (w <- 0 until tweets(q).length) {
        word = tweets(q)(w).toLowerCase
        for (e <- 0 until tweets.length) {
          if (tweets_2(e).contains(word)) {
            count_df += 1
          }
        }
        if (!word.isEmpty) {
          finalCount += ("(" + word + "," + count_df + ")")
        }
        count_df = 0
      }
    }
    var finalArr = new ArrayBuffer[String]()
    var finalOutput = new ArrayBuffer[String]()
    for (a <- finalCount.distinct.sorted.toList) {
      val array_word = new ArrayBuffer[String]()
      var dfWord = (a.split(",")(0).stripPrefix("("))
      var dfCount = (a.split(",")(1).stripSuffix(")"))

      var newl: List[(Int, Int)] = List()
      for (l <- listTF1) {
        var wrd = l._1.toString.split(",")(0).stripPrefix("(")
        var tweetNo = l._1.toString.split(",")(1).stripSuffix(")").toInt
        var freq = l._2
        if (dfWord.compare(wrd) == 0) {
          array_word += (dfWord, dfCount).toString()
          newl ++= List((tweetNo, freq))
        }
      }
      cw.write((array_word.distinct.toString().stripSuffix("))"),newl.toString()).toString().stripPrefix("(ArrayBuffer(") + "\n")
    }
    cw.close()
  }
}


