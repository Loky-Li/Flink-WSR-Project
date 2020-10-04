package com.hong.market

import java.util.UUID

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

// 自定义测试数据源
class SimulateMarketEvenSource() extends  RichParallelSourceFunction[MarketUserBehavior] {

    // 定义是否在运行的标识位
    var running: Boolean = true

    // 定义好可选的 behavior 和 channel 聚合
    val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
    val channelSet: Seq[String] = Seq("appstore", "huweistore", "weibo", "wechat")

    // 随机数生成器
    val rand: Random = Random

    override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
        // 定义一个发出数据数据的最大量，控制测试数据量
        val maxCounts = Long.MaxValue
        var count = 0L

        // while循环，不停地随机生成数据
        while(running && count < maxCounts){
            val id = UUID.randomUUID().toString
            val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
            val channel = channelSet(rand.nextInt(channelSet.size))
            val ts = System.currentTimeMillis()

            ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
            count += 1
            Thread.sleep(100)
        }
    }

    override def cancel(): Unit = {running = false}
}

// 输入数据样例类
case class MarketUserBehavior(userId: String,
                              behavior: String,
                              channel: String,
                              timestamp: Long)
