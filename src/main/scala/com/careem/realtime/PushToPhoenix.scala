package com.careem.realtime

import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PushToPhoenix {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass.getName)
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.es.nodes", "localhost")
    sparkConf.set("spark.es.port", "9200")
    sparkConf.set("spark.es.nodes.wan.only", "true")

    val spark = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("Creating tripsDf")
    val tripsDf = Utils.getDfFromKafka(spark)("trips", Schemas.SCHEMA_TRIPS)
      .withWatermark("timestamp", Utils.WATERMARK_DELAY)
      .filter($"data.dropoff_epoch".gt($"data.pickup_epoch"))
      .filter($"data.pickup_latitude".notEqual(0.0) || $"data.pickup_longitude".notEqual(0.0))
      .filter($"data.dropoff_latitude".notEqual(0.0) || $"data.dropoff_longitude".notEqual(0.0))

    println("Starting the streaming query...")
    val tripsQuery: StreamingQuery = tripsDf
      .writeStream
      .trigger(Trigger.ProcessingTime(Utils.TRIGGER_INTERVAL))  // 30 seconds
      .option("checkpointLocation", s"${Utils.CKPT_DIR}/trips")
      .outputMode("append")
      .foreachBatch(new SurgeWriter())
      .start

    tripsQuery.awaitTermination()
  }
}

class SurgeWriter extends ((DataFrame, Long) => Unit) {
  def apply(data: DataFrame, batchId: Long): Unit = {
    data.sparkSession.sparkContext.setLogLevel("WARN")
    import data.sparkSession.implicits._
    println(s"################  ${new Date}: [ SurgeWriter ] Started ################ ")
    val surgeData = data.groupBy($"timestamp".cast("long").plus(lit(10).minus($"timestamp".cast("long").mod(10))).multiply(1000).as("reporting_ts"), $"geohash")
      .agg(countDistinct($"booking_id").as("num_requests"), countDistinct($"driver_id").as("num_drivers"))

    surgeData.saveToPhoenix(Map("table" -> "grab_surge", "zkUrl" -> Utils.PHOENIX_ZK_URL))
    println("[ SurgeWriter ] Saved to phoenix")
  }
}
