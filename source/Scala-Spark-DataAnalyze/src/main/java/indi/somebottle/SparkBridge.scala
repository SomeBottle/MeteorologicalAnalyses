package indi.somebottle

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkConf

object SparkBridge {
  // 创建spark会话
  // val conf: SparkConf = new SparkConf().setAppName("analysis").setMaster("local[*]")
  val conf: SparkConf = new SparkConf().setAppName("analysis").setMaster("spark://master:7077")
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark SQL")
    .config(conf)
    .getOrCreate()

  def getData(file_path: String): DataFrame = {
    // 从HDFS读取CSV数据
    val df = spark.read.option("header", "true")
      .csv(file_path)
    df
  }
}
