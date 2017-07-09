package main.scala
import java.io.FileWriter
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
/**
  * Created by jiteshchawla on 9/15/16.
  */
object Sentiment {
  def main(args: Array[String]): Unit = {
    //    val inputFile = "/Users/jiteshchawla/IdeaProjects/abc/src/main/scala/trial_1.chawla_jitesh_tweets_first20.txt.rtf"
    //    val outputFile = "/Users/jiteshchawla/IdeaProjects/abc/src/main/scala/chawla_jitesh_tweets_sentiment_first20.txt"
    val sparkConf = new SparkConf().setAppName("Twitter_Sentiment").setMaster("local[*]")
    // Create a Scala Spark Context.
    val  sc= new SparkContext(sparkConf)
    // Load our input data.
    val sql_cn = new SQLContext(sc)
    val input =  sc.textFile(args(0))
    val temp1 = sql_cn.read.json(input)
    temp1.registerTempTable("tweets_text")
    val tweets_20 = sql_cn.sql("SELECT text FROM tweets_text ").collectAsList()
    val tweets_20_complete = sql_cn.sql("SELECT * FROM tweets_text ").collectAsList()
//    val cw = new FileWriter("chawla_jitesh_first20.txt")
    val cde = ArrayBuffer[String]()
    for (h <- 0 until tweets_20.size()){
      cde.insert(h,tweets_20_complete.get(h).toString())
    }
//    for(p<-0 until tweets_20.size()) {
//      cw.write(cde(p) + "\n")
//
//    }
//    cw.close()
    val abc = ArrayBuffer[String]()
    for (c<-0 until tweets_20.size()){
      abc.insert(c,tweets_20.get(c).toString())
    }
    var fw = new FileWriter("chawla_jitesh_tweets_sentiment_first20.txt")
    val lines = scala.io.Source.fromFile(args(1), "utf-8").getLines().toList
    var words = scala.collection.mutable.Map[String, Int]()
    for ( l <- lines)
    {
      val y = l.split("\t")
      words += (y(0) -> y(1).toInt)
    }
    var score_sum = new Array[Int](abc.length)
    var word = new String
    val raw = ArrayBuffer[String]()
    val tweets = ArrayBuffer[List[String]]()
    val tweets_2 = ArrayBuffer[List[String]]()
    for (h <- 0 until tweets_20.size()) {
      raw.insert(h, tweets_20.get(h).toString())
      var regularEx = "…|(#(.+?)(?=[\\s.,:,]|$))|(@RT | RT|\\[RT(.+?)(?=[\\s.,:,]|$))|((([A-Za-z]{3,9}:(?:\\/\\/)?)(?:[-;:&=\\+\\$,\\w]+@)?[A-Za-z0-9.-]+|(?:www.|[-;:&=\\+\\$,\\w]+@)[A-Za-z0-9.-]+)((?:\\/[\\+~%\\/.\\w-_]*)?\\??(?:[-\\+=&;%@.\\w_]*)#?(?:[\\w]*))?)|(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9]+)|[$&+,:;=?@#|'\"\\/<>.^*()%!-]|\\[|\\]".r
      for (i <- 0 until raw.length) {
        raw(i) = regularEx.replaceAllIn(raw(i),"")

    }}
    for (i <- 0 until tweets_20.size()) {
      val tweet_split = raw(i).toLowerCase.split("""\s""")
      tweets.insert(i, tweet_split.toList)
      tweets_2.insert(i, tweet_split.toList)
    }

    for (q <- 0 until tweets.length) {

      for (w <- 0 until tweets(q).length) {
var score = words getOrElse(tweets(q)(w).toLowerCase,0)
        score_sum(q)+=score
      }}
    for( k<- 0 until score_sum.length) {
        fw.write("("+ (k+1)+","+score_sum(k)+")"+"\n")
    }
    fw.close()
  }
}
