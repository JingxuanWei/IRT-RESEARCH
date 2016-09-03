package ParallelInsert2

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{WriteConfig, ReadConfig}
import org.apache.spark.examples.ml.Document
import org.apache.spark._
import org.bson._
import org.apache.spark.streaming._
import org.joda.time
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import com.mongodb.spark.config._
import com.mongodb.util.JSON

/**
  * Created by think on 30/08/2016.
  */
object PInsert2 {
  val record = Array[String]()

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Parallel data input").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    //    val writeConfigCars = WriteConfig(Map("uri" -> "mongodb://127.0.0.1:27017/largeDB.40G"))
    val writeConfigCars = WriteConfig(Map("uri" -> "mongodb://127.0.0.1:27017/testDB.test111"))
    val mongodata = sc.textFile("C:\\Users\\think\\Desktop\\Data\\640K.CSV", 3)
    //        val mongodata = sc.textFile("E:\\8G.CSV", 3)
    def getJsonFormat(unit: String): String = {
      val docStr: Array[String] = unit.split(",")
      return "{ \"iSegment\":" + docStr.apply(0).trim() +
        ", \"TrackKm\":" + docStr.apply(1).trim() +
        ", \"SomatTime\":" + docStr.apply(2).trim() +
        ", \"kmh\":" + docStr.apply(3).trim() +
        ", \"CarOrient\":" + docStr.apply(4).trim() +
        ", \"EorL\":" + docStr.apply(5).trim() +
        ", \"minSND\":" + docStr.apply(6).trim() +
        ", \"maxSND\":" + docStr.apply(7).trim() +
        ", \"Rock\":" + docStr.apply(8).trim() +
        ", \"Bounce\":" + docStr.apply(9).trim() +
        ", \"minCFA\":" + docStr.apply(10).trim() +
        ", \"maxCFA\":" + docStr.apply(11).trim() +
        ", \"accR3\":" + docStr.apply(12).trim() +
        ", \"accR4\":" + docStr.apply(13).trim() +
        //        ", \"TrackName\":" + docStr.apply(14).trim() +
        //        ", \"Direction\":" + docStr.apply(15).trim() +
        ", \"minCFB\":" + docStr.apply(16).trim() +
        ", \"maxCFB\":" + docStr.apply(17).trim() +
        ", \"LATACC\":" + docStr.apply(18).trim() +
        ", \"maxBounce\":" + docStr.apply(19).trim() +
        ", \"PipeA\":" + docStr.apply(20).trim() +
        ", \"PipeB\":" + docStr.apply(21).trim() +
        ", \"GPSLat\":" + docStr.apply(22).trim() +
        ", \"GPSLon\":" + docStr.apply(23).trim() + "}"
    }
    val mongoDocs = mongodata.map(t => org.bson.Document.parse(getJsonFormat(t)))
    //        val mongoDocs = mongodata.map(t => org.bson.Document.parse(getJsonFormat(t))).repartition(3)
    val startTime = DateTime.now()
    println(startTime)
    MongoSpark.save(mongoDocs, writeConfigCars)
    val endTime = DateTime.now()
    println(endTime)
    displayTimeConsuming(startTime, endTime)
    sc.stop()
    def displayTimeConsuming(startTime: DateTime, endTime: DateTime) = {
      val timeSpend = new time.Duration(endTime, startTime)
      println("The total process spend: " + timeSpend.getStandardSeconds.toString + " second ")
      println("The total process spend: " + timeSpend.getStandardMinutes.toString + " Minutes ")
      println("The total process spend: " + timeSpend.getStandardHours.toString + " Hours ")
    }
  }
}
