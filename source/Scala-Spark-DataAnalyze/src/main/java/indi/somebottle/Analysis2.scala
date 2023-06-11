package indi.somebottle

/**
 * 分析任务2：各污染物浓度最高的前10个时段
 */
object Analysis2 {
  def main(args: Array[String]): Unit = {
    // 获得spark会话
    val spark = SparkBridge.spark
    // 从HDFS读取CSV数据
    val df = SparkBridge.getData("hdfs://master:8020/input/preprocessed.csv")
    // 从CSV数据中筛选出各污染物浓度最高的前10个时段
    // 先创建临时视图tmp_view用于查询
    df.createOrReplaceTempView("tmp_view")
    // 待查询的字段列表
    val fields = List(
      ("SO2监测浓度(μg/m3)", "SO2"),
      ("NO2监测浓度(μg/m3)", "NO2"),
      ("PM10监测浓度(μg/m3)", "PM10"),
      ("PM2.5监测浓度(μg/m3)", "PM2.5"),
      ("O3监测浓度(μg/m3)", "O3"),
      ("CO监测浓度(mg/m3)", "CO")
    )
    // 遍历字段进行查询
    for ((field, alias) <- fields) {
      // 利用scala的模板字符串进行字符串拼接
      // 然后利用Spark SQL查询
      val currTop10 = spark.sql(
        s"SELECT `监测时间`,DOUBLE(`${field}`) " +
          "FROM tmp_view " +
          s"ORDER BY `${field}`" +
          "DESC LIMIT 10"
      )
      currTop10.show()
      currTop10.write
        .option("header", "true")
        .option("encoding", "UTF-8")
        .mode("overwrite")
        .csv(s"hdfs://master:8020/output/top10_concentration_${alias}")
    }
  }
}
