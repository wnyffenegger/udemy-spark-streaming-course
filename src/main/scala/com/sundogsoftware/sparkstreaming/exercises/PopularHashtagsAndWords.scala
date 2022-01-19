package com.sundogsoftware.sparkstreaming.exercises

import com.sundogsoftware.sparkstreaming.Utilities.{setupLogging, setupTwitter}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PopularHashtagsAndWords {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())

    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    // Now eliminate anything that's not a hashtag
//    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val wordKeyValues = tweetwords.map(word => (word, 1))

    // Now count them up over a 5 minute window sliding every one second
    val wordCounts = wordKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(10))
    //  You will often see this written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))

    // Sort the results by the count values
    val sortedResults = wordCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    val hashtags = sortedResults.filter(word => word._1.startsWith("#"))

    // Only get meaningful words
    // be better to filter out using dictionary of common words in multiple languages to find more meaningful words
    val words = sortedResults.filter(word => !word._1.startsWith("#")).filter(word => word._1.length > 4)

    // Print the top 10
    // Common hashtags returns many languages
    hashtags.print

    // Common words returns mostly English
    // which seems to indicate a difference in volume by language or
    // artifacts of other languages or when I ran it or who knows
    words.print
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
