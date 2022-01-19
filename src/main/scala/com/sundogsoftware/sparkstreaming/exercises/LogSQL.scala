
package com.sundogsoftware.sparkstreaming.exercises

import com.sundogsoftware.sparkstreaming.Utilities.{apacheLogPattern, setupLogging}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.rdd.RDD

import java.util.regex.Pattern
import java.util.regex.Matcher

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
 *  Apache log data extracted from a stream on port 9999.
 */
object LogSQL {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setAppName("LogSQL").setMaster("local[*]").set("spark.sql.warehouse.dir", "file:///C:/tmp")
    val ssc = new StreamingContext(conf, Seconds(10))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })
 
    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {
      // So we'll demonstrate using SparkSQL in order to query each RDD
      // using SQL queries.

      val spark = SparkSession
         .builder()
         .appName("LogSQL")
         .getOrCreate()
         
      import spark.implicits._

      // SparkSQL can automatically create DataFrames from Scala "case classes".
      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

      // Only write out frames that have at least one record
      if (requestsDataFrame.count() > 0) {
        requestsDataFrame.write.mode(SaveMode.Append).json("./tmp/")
      }
    })
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Record(url: String, status: Int, agent: String)


