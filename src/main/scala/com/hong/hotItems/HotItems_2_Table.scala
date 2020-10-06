package com.hong.hotItems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._

object HotItems_2_Table {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\UserBehavior.csv"
        val inputStream: DataStream[String] = env.readTextFile(path)

        val dataStream: DataStream[UserBehavior] = inputStream
            .map(data => {
                val arr: Array[String] = data.split(",")
                UserBehavior(
                    arr(0).toLong,
                    arr(1).toLong,
                    arr(2).toInt,
                    arr(3),
                    arr(4).toLong
                )
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        // 调用TableAPI，创建表的执行环境
        val settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        //将DataStream装换成表，进行处理
        val dataTable = tableEnv.fromDataStream(dataStream,
        'itemId, 'behavior, 'timestamp.rowtime as 'ts)

        // 分组开窗增量聚合
        val aggTable = dataTable
            .filter('behavior === "pv")
            .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
            .groupBy('itemId, 'sw)
            .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)

        // 使用SQL实现按窗口分组选择 TopN 功能。TableAPI 没有开窗的功能
        tableEnv.createTemporaryView("agg", aggTable, 'itemId, 'cnt, 'windowEnd)
        val resultTable = tableEnv.sqlQuery(
            """
              |select *
              |from (
              |    select *,
              |        row_number() over (partition by windowEnd order by cnt desc) as row_num
              |    from agg
              |)
              |where row_num <= 5
              |""".stripMargin)

        import java.sql.Timestamp
        resultTable.toRetractStream[(Long, Long, Timestamp, Long)].print("result")

        env.execute("hot items with table api & sql")
    }


}
