package indi.somebottle

/**
 * 分析任务3：统计PM2.5和PM10浓度在各IAQI区间的时段数
 */
object Analysis3 {
  def main(args: Array[String]): Unit = {
    // 获得spark会话
    val spark = SparkBridge.spark
    // 从HDFS读取CSV数据
    val df = SparkBridge.getData("hdfs://master:8020/input/preprocessed.csv")
    // 先创建临时视图tmp_view用于查询
    df.createOrReplaceTempView("tmp_view")
    // PM2.5 IAQI区间
    val pm25Sections = List(
      (-1, 0, 0), // IAQI 0
      (0, 35, 50), // IAQI 50
      (35, 75, 100), // 100
      (75, 115, 150), // 150
      (115, 150, 200), // 200
      (150, 250, 300), // 300
      (250, 350, 400), // 400
      (350, 500, 500) // 500
    )
    // PM10 IAQI区间
    val pm10Sections = List(
      (-1, 0, 0), // IAQI 0
      (0, 50, 50), // IAQI 50
      (50, 150, 100), // 100
      (150, 250, 150), // 150
      (250, 350, 200), // 200
      (350, 420, 300), // 300
      (420, 500, 400), // 400
      (500, 600, 500) // 500
    )
    // PM2.5统计结果，用一个列表进行存放 (IAQI值, 在这个区间内的时段数)
    var pm25Counts = List.empty[(Int, Long)]
    // PM10统计结果，用一个列表进行存放
    var pm10Counts = List.empty[(Int, Long)]
    // 查询PM2.5数据
    for ((low, high, iaqi:Int) <- pm25Sections) {
      // 查询
      val res = spark.sql(
        s"SELECT DOUBLE(`PM2.5监测浓度(μg/m3)`) " +
          s"FROM tmp_view " +
          s"WHERE `PM2.5监测浓度(μg/m3)` > ${low} " +
          s"AND `PM2.5监测浓度(μg/m3)` <= ${high}"
      ).count() // 对结果数目进行统计
      // 将结果添加到列表中
      pm25Counts = pm25Counts ++ List((iaqi, res))
    }
    // 查询PM10数据
    for ((low, high, iaqi:Int) <- pm10Sections) {
      // 查询
      val res = spark.sql(
        s"SELECT DOUBLE(`PM10监测浓度(μg/m3)`) " +
          s"FROM tmp_view " +
          s"WHERE `PM10监测浓度(μg/m3)` > ${low} " +
          s"AND `PM10监测浓度(μg/m3)` <= ${high}"
      ).count() // 对结果数目进行统计
      // 将结果添加到列表中
      pm10Counts = pm10Counts ++ List((iaqi, res))
    }
    // 转换为DataFrame
    // 导入Spark SQL的隐式转换库
    import spark.implicits._
    // 将列表转换为DataFrame
    val pm25Res = pm25Counts.toDF("IAQI", "COUNT")
    val pm10Res = pm10Counts.toDF("IAQI", "COUNT")
    pm25Res.show()
    pm10Res.show()
    // 将结果写入HDFS
    pm25Res.write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .csv("hdfs://master:8020/output/pm25_iaqi_count")
    pm10Res.write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .mode("overwrite")
      .csv("hdfs://master:8020/output/pm10_iaqi_count")
  }
}
