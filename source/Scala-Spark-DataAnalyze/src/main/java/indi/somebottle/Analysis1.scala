package indi.somebottle

/**
 * 分析任务1：各污染物浓度的平均值
 */
object Analysis1 {
  def main(args: Array[String]): Unit = {
    // 获得spark会话
    val spark = SparkBridge.spark
    // 从HDFS读取CSV数据
    val df = SparkBridge.getData("hdfs://master:8020/input/preprocessed.csv")
    // 先创建临时视图tmp_view用于查询
    df.createOrReplaceTempView("tmp_view")
    // 待查询的字段列表
    val fields = List(
      "SO2监测浓度(μg/m3)",
      "NO2监测浓度(μg/m3)",
      "PM10监测浓度(μg/m3)",
      "PM2.5监测浓度(μg/m3)",
      "O3监测浓度(μg/m3)",
      "CO监测浓度(mg/m3)"
    )
    // 计算各字段平均值，构造SELECT查询字符串
    val avgSelectStr = fields.map(
      field => s"AVG(DOUBLE(`${field}`)) AS `${field}` "
    ).mkString(", ")
    // 查询结果
    val avgRes = spark.sql(
      s"SELECT ${avgSelectStr} " +
        "FROM tmp_view "
    )
    avgRes.show()
    avgRes.write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .csv(s"hdfs://master:8020/output/pollutant_average")
  }
}
