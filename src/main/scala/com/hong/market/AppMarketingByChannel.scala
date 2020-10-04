package com.hong.market

import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp

import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val dataStream: DataStream[MarketUserBehavior] = env
            .addSource(new SimulateMarketEvenSource)
            .assignAscendingTimestamps(_.timestamp)

        val resultStream: DataStream[MarketCount] = dataStream
            .filter(_.behavior != "UNINSTALL")
            .keyBy(marketUserBehavior => (marketUserBehavior.channel, marketUserBehavior.behavior))
            .timeWindow(Time.hours(1), Time.seconds(5))
            .process(new MarketCountByChannel())
            // 上面可以使用 aggregate()传入聚合+全窗，也可以使用 apply()传入一个全窗函数。
            //  .apply()传入的可以得到 IN/OUT/Window信息, 而 process还可以拿到 KEY

        resultStream.print("marketCount")

        env.execute("market channel count")

    }
}

class MarketCountByChannel() extends
    ProcessWindowFunction[MarketUserBehavior, MarketCount, (String, String), TimeWindow] {
    override def process(key: (String, String),
                         context: Context,
                         elements: Iterable[MarketUserBehavior],
                         out: Collector[MarketCount]): Unit = {
//        import java.sql.Timestamp
        val windowStart: String = new Timestamp(context.window.getStart).toString
        val windowEnd: String = new Timestamp(context.window.getEnd).toString
        val channel: String = key._1
        val behavior: String = key._2
        val count: Long = elements.size

        out.collect(MarketCount(windowStart, windowEnd, channel, behavior, count))
    }
}

// 定义输出统计的样例类
case class MarketCount(windowStart: String,
                       windowEnd: String,
                       channel: String,
                       behavior: String,
                       count: Long)


