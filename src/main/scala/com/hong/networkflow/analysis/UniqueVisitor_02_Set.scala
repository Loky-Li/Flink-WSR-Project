package com.hong.networkflow.analysis

import com.hong.hotItems.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UniqueVisitor_02_Set {
    /**
     *  由于第一种方式，所有数据都会缓存到一个窗口，窗口触发后再计算。可能会OOM
     *  方式二：使用现增量，后窗口函数来实现
     * @param args
     */

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
                .aggregate(new UvCountAgg(), new UvCountResultWithWindow())

        uvStream.print("UvCount")

        env.execute("uv job")
    }
}

// 自定义增量聚合函数，需要定义一个Set作为累加器
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {
    override def createAccumulator(): Set[Long] = Set[Long]()

    override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] =
        accumulator + value.userId

    override def getResult(accumulator: Set[Long]): Long = accumulator.size

    override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

// 自定义窗口函数，添加 window 信息，包装成样例类
class UvCountResultWithWindow() extends AllWindowFunction[Long, UvCount, TimeWindow] {

    override def apply(window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[UvCount]): Unit = {
        out.collect(UvCount(window.getEnd, input.head))
    }
}

