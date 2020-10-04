package com.hong.networkflow.analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.hong.hotItems.ItemViewCount
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NetworkFlowTopNPage {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\apache.log"
        val inputStream: DataStream[String] = env.readTextFile(path)

        val dataStream = inputStream
            .map(data => {
                val arr = data.split(" ")

                //将日志中的日期转成时间戳
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val timestamp = sdf.parse(arr(3)).getTime

                ApacheLogEvent(arr(0), arr(1), timestamp, arr(5), arr(6))
            })
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {  // ①大部分的数据在 1s 内延迟
                    override def extractTimestamp(element: ApacheLogEvent): Long = element.evenTime
                })

        // 开窗聚合
        val lateOutputTag = new OutputTag[ApacheLogEvent]("late date")

        val aggStream = dataStream
            .keyBy(_.url)
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .allowedLateness(Time.minutes(1))       // ② 窗口不关，在水位线1分钟内来的少部分延迟数据再计算处理。窗口正式关闭，窗口的状态也会清空
            .sideOutputLateData(lateOutputTag)     // ③ 对极少数迟到的数据，将一切已关闭窗口数据取出，再计算，并侧输出。
            .aggregate(new PageCountAgg(), new PageCountWindowResult())

        val lateDataStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateOutputTag)

        // 每个窗口的统计值排序输出
        val resultSteam = aggStream
            .keyBy(_.windowEnd)
            .process(new TopNHotPage(3))


        aggStream.print("agg")
        lateDataStream.print("late")

        resultSteam.print("result")

        env.execute("topN page")

    }

}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {

    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[PageViewCount]): Unit = {
        out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))

    }
}

class TopNHotPage(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

    lazy val pageCountListState: ListState[PageViewCount] =
        getRuntimeContext.getListState(
            new ListStateDescriptor[PageViewCount]("pageCount_list", classOf[PageViewCount])
        )

    override def processElement(value: PageViewCount,
                                ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                                out: Collector[String]): Unit = {

        pageCountListState.add(value)

        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
        val allPageCountList: ListBuffer[PageViewCount] = ListBuffer()
        val iter: util.Iterator[PageViewCount] = pageCountListState.get().iterator()
        while (iter.hasNext) {
            allPageCountList += iter.next()
        }

//        pageCountListState.clear()
        // 如果处理迟到数据（allowedLateness部分）的话，这里在第一次窗口输出的时候，就清空了，迟到数据将取不到状态值，一起再计算。
        // 所以再注册一个定时器，在1分钟后才清空


        val sortedPageCountList = allPageCountList
            .sortWith(_.count > _.count).take(n)

        val result: StringBuilder = new mutable.StringBuilder()
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

        for (i <- sortedPageCountList.indices) {
            val currentItemCount: PageViewCount = sortedPageCountList(i)
            result.append("Top").append(i + 1).append(":")
                .append(" 页面url=").append(currentItemCount.url)
                .append(" 访问量=").append(currentItemCount.count)
                .append("\n")
        }
        result.append("===================================\n\n")

        // 控制输出频率
        Thread.sleep(1000)

        out.collect(result.toString())
    }
}

case class ApacheLogEvent(ip: String,
                          userId: String,
                          evenTime: Long,
                          method: String,
                          url: String)

case class PageViewCount(url: String,
                         windowEnd: Long,
                         count: Long)
