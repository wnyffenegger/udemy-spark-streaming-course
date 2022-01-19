
package com.sundogsoftware.sparkstreaming.exercises

import com.sundogsoftware.sparkstreaming.Utilities.{apacheLogPattern, setupLogging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.regex.Matcher

/** Maintains top URL's visited over a 5 minute window, from a stream
 *  of Apache access logs on port 9999.
 */
object LogParser {
 
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(8)})
      .filter(x => x != null).filter(x => x.toString.nonEmpty).filter(x => !x.equals("-"))
//    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches() && matcher.group(8).nonEmpty) matcher.group(8)})


    // Get HTTP Code
//    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(0) else "[error]"})

    // Get HTTP version
//    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(2) else "[error]"})

    // Extract the URL from the request
//    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    val urls = requests.map(x => x.toString)
    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

