package com.hong.networkflow.analysis

import com.hong.hotItems.UserBehavior
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object PageView {
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

        // 分配key，包装成二元组，开窗聚合
        val pvStream: DataStream[PvCount] = dataStream
            .filter(_.behavior == "pv")
//            .map(data => ("pv", 1L))
// 上面会将所有数据都放到一个分区里，造成数据倾斜。可以自定义map，对key分组
            .map(new MyMapper())
            .keyBy(_._1)
            .timeWindow(Time.hours(1))
            .aggregate(new PvCountAgg(), new PvCountResult())

        // key分组后，需要将各分区的结果进行汇总
        val pvTotalStream: DataStream[PvCount] = pvStream
                .keyBy(_.windowEnd)
                .process(new TotalPvCountResult())

//        pvStream.print("PvCount")
        pvTotalStream.print("PvTotalCount")

        env.execute("pv job")

    }
}

// 自定义MapFunction，随机生成key
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {

    override def map(value: UserBehavior): (String, Long) = {
        (Random.nextInt(4).toString, 1L)
    }
}

// 自定义ProcessFunction，将多个分区的聚合的=结果按窗口合并
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {

    // 定义一个状态来保存之前所有结果之和
    lazy val totalCountState: ValueState[Long] =
        getRuntimeContext.getState(new ValueStateDescriptor[Long]("totalPvCount", classOf[Long]))

    override def processElement(value: PvCount,
                                ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context,
                                out: Collector[PvCount]): Unit = {
        val currentTotalCount = totalCountState.value()
        //加上新的值
        totalCountState.update(currentTotalCount + value.count)

        // 注册定时器， windowEnd+1之后触发.
        // (十个分区的windowEnd的WaterMark都到后，才会更新该处的WaterMark，所以不用担心某个分区是数据没有被统计输出)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    //上面的分析知道，之后所有分区的数据都来了后，WaterMark才会更新，也才会触发定时器时间
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext,
                         out: Collector[PvCount]): Unit = {
//        out.collect(PvCount(timestamp -1, totalCountState.value()))
        // 获取下面的方式也可以获取到windowEnd时间
        out.collect(PvCount(ctx.getCurrentKey, totalCountState.value()))

        totalCountState.clear()
    }
}

class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {

    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[PvCount]): Unit = {
        out.collect(PvCount(window.getEnd, input.head))
    }
}

case class PvCount(windowEnd: Long,
                   count: Long
                      )
