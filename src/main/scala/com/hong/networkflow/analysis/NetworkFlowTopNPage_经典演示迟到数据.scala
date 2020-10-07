package com.hong.networkflow.analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import com.hong.hotItems.ItemViewCount
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
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

/*        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\apache.log"
        val inputStream: DataStream[String] = env.readTextFile(path)*/
        val inputStream: DataStream[String] = env.socketTextStream("hadoop203", 7777)

        val dataStream: DataStream[ApacheLogEvent] = inputStream
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
            .sideOutputLateData(lateOutputTag)     // ③ 对极少数迟到的数据，侧输出，原窗口已经关闭，所以这里只输出数据，不计算。
            .aggregate(new PageCountAgg(), new PageCountWindowResult())

        val lateDataStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateOutputTag)

        // 每个窗口的统计值排序输出
        val resultSteam = aggStream
            .keyBy(_.windowEnd)
            .process(new TopNHotPage(3))


        dataStream.print("data")
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

    // 开始使用的是ListState，在处理延迟数据的时候，有重叠结果。需要改用MapState
/*    lazy val pageCountListState =
        getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageCount_list", classOf[PageViewCount]))*/
    lazy val pageCountMapState: MapState[String, Long] =
        getRuntimeContext.getMapState(
            new MapStateDescriptor[String,Long]("pageCount_map", classOf[String], classOf[Long])
        )

    override def processElement(value: PageViewCount,
                                ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                                out: Collector[String]): Unit = {

//        pageCountListState.add(value)
        pageCountMapState.put(value.url, value.count)

        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)          // 触发计算的时间戳。
//        （我发现，当一条允许等待的数据属于多个窗口的时候，在这里每个窗口都会注册一个定时器，
//        然后不仅agg打印多个，窗口的计算result也会打印多个。可以看测试数据中 31秒输入后，再输入53秒的现象）

        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60*1000L)   // 等待了迟到数据，真正清空窗口的时间戳
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

        // 如果处理迟到数据（allowedLateness部分）的话，这里在第一次窗口输出的时候，就清空了，迟到数据将取不到状态值，一起再计算。
        // 所以再注册一个定时器，在1分钟后才清空。。
        // 而这样处理了之后，同一个窗口的同一个page，开始使用的ListState就会有重叠的记录。 如来一条xx,输出  Top1,url=xx,cnt=1，又来一条xx的url时，
        // 会同时输出 Top1,url=xx,cnt=2和 Top2,url=xx,cnt=1，所以需要将ListState，改为MapState
        while(timestamp == ctx.getCurrentKey + 60*1000L){
//            pageCountListState.clear()
            pageCountMapState.clear()
            return
        }

//        val allPageCountList: ListBuffer[PageViewCount] = ListBuffer()
        val allPageCountList: ListBuffer[(String, Long)] = ListBuffer()
//        val iter: util.Iterator[PageViewCount] = pageCountListState.get().iterator()
        val iter: util.Iterator[Map.Entry[String, Long]] = pageCountMapState.entries().iterator()
        while (iter.hasNext) {
            val entry: Map.Entry[String, Long] = iter.next()
            //def +=(elem1: A, elem2: A, elems: A*): this.type = this += elem1 += elem2 ++= elems。
            // += 是一个重载的方法，下面的方式，会误以为传入两个参数，都是String，和allPageCountList的元组泛型不符合。所以需要外加一个括号
//            allPageCountList += (entry.getKey, entry.getValue)
            allPageCountList += ((entry.getKey, entry.getValue))
        }

//        pageCountListState.clear()
        // 放在这里的话，同一个窗口计算了一次后，就清空了。造成该窗口再进来新的数据时，不能和历史的数据一起计算统计。(见该方法开头新加的while)



        val sortedPageCountList: mutable.Seq[(String, Long)] = allPageCountList
            .sortWith(_._2 > _._2).take(n)

        val result: StringBuilder = new mutable.StringBuilder()
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

        for (i <- sortedPageCountList.indices) {
            val currentItemCount: (String, Long) = sortedPageCountList(i)
            result.append("Top").append(i + 1).append(":")
                .append(" 页面url=").append(currentItemCount._1)
                .append(" 访问量=").append(currentItemCount._2)
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
