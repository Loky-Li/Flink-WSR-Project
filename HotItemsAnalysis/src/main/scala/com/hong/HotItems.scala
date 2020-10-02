package com.hong

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HotItems {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val path: String = "E:\\code\\Flink-WSR-Project\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
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

        // 对数据进行转换，过滤出pv行为，开窗聚合统计个数。同时区分出不同的窗口
        // 既要对每来一条数据就计算一次，保存一个状态值，又要获取到窗口的信息。
        val resultStream: DataStream[ItemViewCount] = dataStream
            .filter(_.behavior == "pv")
            .keyBy(_.itemId)
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAgg(), new ItemCountWindowResult())

        resultStream.print()
        env.execute("hot items job")
    }
}

//全窗口的可能会造成需要处理的数据太多，且flink是在内存中运行，所以有可能OOM
//但是增量聚合没有窗口信息，所有不能实现窗口的结束时间输出
//增量聚合函数 和 全窗口聚合函数 结合使用
// 全窗口中只有一条窗口结束时间，而增量聚合中则实现对最大最小值的实时变化，减小全窗口中可能的OOM

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

    // 累计器的初始值
    override def createAccumulator(): Long = 0L

    // 单个累加器的累加逻辑
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

    // 从累加器中获取到最终需要输出的结果
    override def getResult(accumulator: Long): Long = accumulator

    // 多个不分区的累加器的合并逻辑
    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数。输入的是预聚合的输出
class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
        val itemId = key
        val windowEnd = window.getEnd
        val cnt = input.iterator.next()
        out.collect(ItemViewCount(itemId, windowEnd, cnt))
    }
}

// 定义输入的样例类
case class UserBehavior(
                       userId: Long,
                       itemId: Long,
                       categoryId: Int,
                       behavior: String,
                       timestamp: Long
                       )

// 定义输出的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)