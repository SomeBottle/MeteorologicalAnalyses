package indi.somebottle

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, greatest, lit, udf, when}

/**
 * 分析任务4：计算每天的空气质量等级AQI
 */
object Analysis4 {
  // 构建转换规则
  val conversions: Map[String, List[(Double, Double, Double, Double)]] = Map(
    "SO2" -> List((0.0, 50.0, 0.0, 50.0), (50.0, 150.0, 50.0, 100.0), (150.0, 475.0, 100.0, 150.0), (475.0, 800.0, 150.0, 200.0), (800.0, 1600.0, 200.0, 300.0), (1600.0, 2100.0, 300.0, 400.0), (2100.0, 2620.0, 400.0, 500.0)),
    "NO2" -> List((0.0, 40.0, 0.0, 50.0), (40.0, 80.0, 50.0, 100.0), (80.0, 180.0, 100.0, 150.0), (180.0, 280.0, 150.0, 200.0), (280.0, 565.0, 200.0, 300.0), (565.0, 750.0, 300.0, 400.0), (750.0, 940.0, 400.0, 500.0)),
    "PM10" -> List((0.0, 50.0, 0.0, 50.0), (50.0, 150.0, 50.0, 100.0), (150.0, 250.0, 100.0, 150.0), (250.0, 350.0, 150.0, 200.0), (350.0, 420.0, 200.0, 300.0), (420.0, 500.0, 300.0, 400.0), (500.0, 600.0, 400.0, 500.0)),
    "PM2_5" -> List((0.0, 35.0, 0.0, 50.0), (35.0, 75.0, 50.0, 100.0), (75.0, 115.0, 100.0, 150.0), (115.0, 150.0, 150.0, 200.0), (150.0, 250.0, 200.0, 300.0), (250.0, 350.0, 300.0, 400.0), (350.0, 500.0, 400.0, 500.0)),
    "O3" -> List((0.0, 100.0, 0.0, 50.0), (100.0, 160.0, 50.0, 100.0), (160.0, 215.0, 100.0, 150.0), (215.0, 265.0, 150.0, 200.0), (265.0, 800.0, 200.0, 300.0), (800.0, 1000.0, 300.0, 400.0), (1000.0, 1200.0, 400.0, 500.0)),
    "CO" -> List((0.0, 2.0, 0.0, 50.0), (2.0, 4.0, 50.0, 100.0), (4.0, 14.0, 100.0, 150.0), (14.0, 24.0, 150.0, 200.0), (24.0, 36.0, 200.0, 300.0), (36.0, 48.0, 300.0, 400.0), (48.0, 60.0, 400.0, 500.0))
  )

  // 定义自定义函数来计算IAQI
  val udfIAQI: UserDefinedFunction = udf((value: Double, pollutant: String) => {
    // 读取这个污染物的浓度限值区间
    val ranges = conversions(pollutant)
    // 寻找当前污染物的浓度处在哪个区间
    val maybeRange = ranges.find(range => value > range._1 && value <= range._2)
    maybeRange match {
      case Some((rangeLow, rangeHigh, aqiLow, aqiHigh)) =>
        // 根据公式计算iaqi
        val iaqi = ((aqiHigh - aqiLow) / (rangeHigh - rangeLow)) * (value - rangeLow) + aqiLow
        Math.round(iaqi) // IAQI值为整数
      case None => 0 // 如果没有落在任何区间就返回0
    }
  })

  def main(args: Array[String]): Unit = {
    // 从HDFS读取CSV数据
    var df = SparkBridge.getData("hdfs://master:8020/input/preprocessed_by_day.csv")
    // 重命名列名，便于处理
    df = df.withColumnRenamed("SO2监测浓度(μg/m3)", "SO2")
      .withColumnRenamed("NO2监测浓度(μg/m3)", "NO2")
      .withColumnRenamed("PM10监测浓度(μg/m3)", "PM10")
      .withColumnRenamed("CO监测浓度(mg/m3)", "CO")
      .withColumnRenamed("O3监测浓度(μg/m3)", "O3")
      .withColumnRenamed("PM2.5监测浓度(μg/m3)", "PM2_5")

    for (pollutant <- conversions.keys) {
      val iaqiCol = s"${pollutant}_IAQI"
      // 计算出各污染物的IAQI并添加为新的列
      df = df.withColumn(iaqiCol, udfIAQI(col(pollutant), lit(pollutant)))
    }
    // 计算AQI并添加为新的列
    df = df.withColumn(
      "AQI",
      greatest(df.columns.filter(_.contains("_IAQI")).map(col): _*)
    )
      // 添加AQI类别
      .withColumn("AQI_Category", when(col("AQI").between(0, 50), "优")
        .when(col("AQI").between(51, 100), "良")
        .when(col("AQI").between(101, 150), "轻度污染")
        .when(col("AQI").between(151, 200), "中度污染")
        .when(col("AQI").between(201, 300), "重度污染")
        .otherwise("严重污染"))
    df.show()
    // 保存到HDFS
    df.write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .csv("hdfs://master:8020/output/aqi_calc_res")
  }
}
