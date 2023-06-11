package indi.somebottle

/**
 * 分析任务5：关联分析
 */
object Analysis5 {
  def main(args: Array[String]): Unit = {
    // 获得spark会话
    val spark = SparkBridge.spark
    // 从HDFS读取CSV数据
    val df = SparkBridge.getData("hdfs://master:8020/input/preprocessed.csv")
    // 先创建临时视图tmp_view用于查询
    df.createOrReplaceTempView("tmp_view")
    // 计算温度和湿度的相关系数（皮尔逊Pearson相关系数）
    val corrRes = spark.sql(
      "SELECT CORR(`温度(℃)`, `湿度(%)`) AS `TEM_HUM_CORR`, " +
        "  CORR(`PM2.5监测浓度(μg/m3)`, `PM10监测浓度(μg/m3)`) AS `PM25_PM10_CORR`," +
        "  CORR(`SO2监测浓度(μg/m3)`, `NO2监测浓度(μg/m3)`) AS `SO2_NO2_CORR`," +
        "  CORR( `云量`,`温度(℃)`) AS `CLOUD_TEM_CORR`," +
        "  CORR(`长波辐射（W/m2）`,`温度(℃)`) AS `WAVE_TEM_CORR`" +
        "FROM tmp_view "
    )
    corrRes.show()
    corrRes.write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .csv(s"hdfs://master:8020/output/correlation_res")
  }
}
