package com.semanticAnalaysis.twitter

import java.io.File
import java.util.Properties

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.convert.wrapAll._

/**
  * Collect at least the specified number of tweets into json text files.
  */
object question4 {
  private var totalTweets = 0L
  private var partition = 0
  private var gson = new Gson()

  val property = new Properties()
  property.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(property)

  def main(args: Array[String]) {
   
    //set the output file path
    if (args.length < 1) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<output>")
      System.exit(1)
    }
    
    //create the output directory
    val output = new File(args(0))
    if (output.exists()) {
      output.delete()
    }
    output.mkdirs()
    
    //set the keys
    System.setProperty("twitter4j.oauth.consumerKey", "fEEt6U4bfHWYNC5fPJU9ViPaf")
    System.setProperty("twitter4j.oauth.consumerSecret", "OCPKb2zLYoSVAhC6HFqWkqkVaLOb4rZsyxow5iI52HRjN6uoIp")
    System.setProperty("twitter4j.oauth.accessToken", "937880095993278464-jPc5NdR9EhBu1qCMiOlOh0aUmcapAHe")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "09uuZObFH9AUKkYsUoWLEgefPaEnWRRmsJxw4ORNC9495")
 
    //create spark contexts
    val sparkConf = new SparkConf().setAppName("#sentimentAnalysis").setMaster("local[2]")
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(60))
 
    // pass the filters and the language
    val filters = Array("data leak","security breach","information stolen","password stolen","hacker stole","DDoS attack","slow internet","network infiltrated","malicious activity","vulnerable exploit","phishing attack","unauthorized access","stolen identity","hacked","malware","trojan","spam","spoofing","ransomware","intrusion","decipher","virus","pishing","bot","forensics")
    val stream = TwitterUtils.createStream(sparkStreamingContext, None, filters)
    val langFilter = stream.filter(_.getLang() == "en")
    langFilter.saveAsTextFiles(args(0) + "/analysis_")
    
    //perform the sentiment analysis on the incoming tweet stream and write it to a file appended with keyword analaysis
    def getSentiment(text: String): Sentiment = {
    val (_, sentiment) = getSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
    }
  
  def getSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val lines = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    lines
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString,Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

    langFilter.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        val sentimentsD = outputRDD.map {
          string =>
            val sentiment = Vector(string.getText, getSentiment(string.getText))
            sentiment
        }

        sentimentsD.saveAsTextFile(args(0) + "/analysis_" + time.milliseconds.toString)
        totalTweets += count
        
        //Number of tweets collected is 100 so we keep it as the exiting criteria
        
        if (totalTweets > 100) {
          System.exit(0)
        }
      }
    })

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  } 
}
