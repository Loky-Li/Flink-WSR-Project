package com.hong.market

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.AggregateDataSet
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object AdAnalysisByProvince_BlackList {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\AdClickLog.csv"

        val adLogStream: DataStream[AdClickEvent] = env.readTextFile(path)
            .map(data => {
                val arr: Array[String] = data.split(",")
                AdClickEvent(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        // todo 定义刷单行为过滤操作。
        //  （在计算之前过滤，正常数据进入主流，异常数据侧输出流）
        val filterBlackListStream: DataStream[AdClickEvent] = adLogStream
            .keyBy(data => (data.userId, data.adId)) //按照用户和广告id分组，即需要满足两个条件列入黑名单
//            .process(new FilterBlackList(100L))
            // 为什么我上面的实现和下面的FilterBlackListUser一样，却没有数据黑名单警告？？？迷了
            .process(new z_FilterBlackListUser(100L))


        //按照province分组开窗聚合统计 （接收到主流的数据）
        val adCountStream: DataStream[AdCountByProvince] = filterBlackListStream
            .keyBy(_.province)
            .timeWindow(Time.hours(1), Time.seconds(5))
            .aggregate(new AdCountAgg(), new AdCountResult())

        adCountStream.print("adCount")

        //获取到侧输出流

        filterBlackListStream
            .getSideOutput(new OutputTag[BlackListWarning]("black_list"))
            .print("blackList")

        env.execute("ad analysis province")
    }
}



// 自定义的ProcessFunction，过滤黑名单信息到侧输出流中。
// 每条key对应的数据都进来一次，不同的key保存不同的state
class FilterBlackList(maxClickCount: Long) extends
    KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    // 定义状态，需要保存点击量count
    lazy val countState: ValueState[Long] =
        getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-State", classOf[Long]))

    // 标识位：用来表示用户是否在黑名单中。默认值是false
    lazy val isBlackListState: ValueState[Boolean] =
        getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isBlackList-State", classOf[Boolean]))

    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent,
                                ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context,
                                out: Collector[AdClickEvent]): Unit = {
        // 取出状态数据
        val curCount = countState.value()


        // 判断：如果是第一个数据，那么注册第二天 0 点的定时器，用于清空状态
        if (curCount == 0) {
            // 当前日期到 1970年1月1日的天数，加1得到第二天的到1970年1月1日的天数，再乘 毫秒 得到第二天0点的时间戳
            val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)

            ctx.timerService().registerEventTimeTimer(ts)
            resetTime.update(ts)

            // 判断count值是否达到上限，如果达到，并且之前灭有输出过警报信息，那么警报
            if (curCount >= maxClickCount) {
                // 如果之前还没有在黑名单，则输出，并加入黑名单
                if (!isBlackListState.value()) {
                    isBlackListState.update(true)
                    ctx.output(
                        new OutputTag[BlackListWarning]("black_list"),
                        BlackListWarning(
                            value.userId,
                            value.adId,
                            "click over " + maxClickCount + "times today"))
                }
                // 该key的后续数据，直接就不在统计。
                return
            }

            countState.update(curCount + 1)
            out.collect(value)  //将没有过滤的，正常的数据，放在主流输出
        }

    }

    // 0 点后，清空状态。重新统计
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long),AdClickEvent, AdClickEvent]#OnTimerContext,
                         out: Collector[AdClickEvent]): Unit = {

//        countState.clear()
//        isBlackListState.clear()
        // 其实可以不给定时器状态变量，这里也不用判断，因为每天注册一次后。到第二天0点直接出发。上面两行实现即可
        if( timestamp == resetTime.value() ){
            isBlackListState.clear()
            countState.clear()
        }
    }
}

class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {

    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[AdCountByProvince]): Unit = {

        val province = key
        import java.sql.Timestamp
        val windowEnd = new Timestamp(window.getEnd).toString
        val count = input.head

        out.collect(AdCountByProvince(province, windowEnd, count))
    }
}

/*
543462,1715,beijing,beijing,1511658000
662867,2244074,guangdong,guangzhou,1511658060
 */
// 输入样例类
case class AdClickEvent(userId: Long,
                        adId: Long,
                        province: String,
                        city: String,
                        timestamp: Long)

// 输出样例类
case class AdCountByProvince(province: String, windowEnd: String, count: Long)

// 定义侧输出流报警信息样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)