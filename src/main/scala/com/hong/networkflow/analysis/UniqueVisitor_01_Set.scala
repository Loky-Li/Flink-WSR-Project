package com.hong.networkflow.analysis

import com.hong.hotItems.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UniqueVisitor_01_Set {
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

        val uvStream: DataStream[UvCount] = dataStream
                .filter(_.behavior == "pv")
                .timeWindowAll(Time.hours(1))   // 开一个小时的全窗
                .apply(new UvCountResult())         // 全窗函数使用 .apply()


        uvStream.print("UvCount")

        env.execute("uv job")
    }
}

class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{

    // 窗口关闭的时候才会调用apply。所有数据会放到 input中缓冲起来。
    override def apply(window: TimeWindow,
                       input: Iterable[UserBehavior],
                       out: Collector[UvCount]): Unit = {
        // 通过Set类型保存所有的id，并实现自动去重
        var idSet = Set[Long]()
        //每来一条数据存一次
        for(userBehavior <- input){
            idSet += userBehavior.userId
        }

        out.collect(UvCount(window.getEnd, idSet.size))
    }
}

case class UvCount(windowEnd: Long, count: Long)
