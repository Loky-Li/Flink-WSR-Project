package com.hong.hotItems

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HotItems {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

/*
        // 从文件中读取数据
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\UserBehavior.csv"
        val inputStream: DataStream[String] = env.readTextFile(path)
*/
        // 从kafka中读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop203:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
            "hot_items",
            new SimpleStringSchema(),
            properties
        ))



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
        val aggStream: DataStream[ItemViewCount] = dataStream
            .filter(_.behavior == "pv")
            .keyBy(_.itemId)
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAgg(), new ItemCountWindowResult())
        // CountAgg在window之后，不能定义为RichAggregateFunction。所以只能使用聚合 + 全窗 的方式
        // 点击 .aggregate() 方法一直到java代码后可以看到。。
        /*
        if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}       而如果只是在keyBy() 之后的话，则可以使用富函数。
		这样做是目的是：不能让用户自己去破坏代码底层维护的窗口状态。如 ACC 累加器就是flink底层自己维护的状态。
         */

        // todo 对窗口聚合结果按照窗口进行分组，并做排序取出 topN 输出
        val resultStream: DataStream[String] = aggStream
            .keyBy(_.windowEnd)
            .process(new TopNHotItems(5))

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

    // 从spark的累加器，会以为是多个不分区的累加器的合并逻辑。
    // 但是实际上，只有session窗口的时候，才会调用到merge，将不同会话的进行合并。
    // 另外，由于是按照key进行分区，同样的key进入同一分区，而ACC的以key为粒度，所以这里并没有多个分区。
    override def merge(a: Long, b: Long): Long = a + b
}


// 扩展： 自定义求平均值的聚合函数。累积器的状态为 （sum，count）
// 这里就对时间戳求平均数吧，没有其他好用的属性
class MyAvgAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double] {

    override def createAccumulator(): (Long, Int) = (0L, 0)

    override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
        (accumulator._1 + value.timestamp, accumulator._2 + 1)

    override def getResult(accumulator: (Long, Int)): Double =
        accumulator._1 / accumulator._2.toDouble

    override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) =
        (a._1 + b._1, a._2 + b._2)
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

// 自定义Key额的ProcessFunction
class TopNHotItems(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    // 定义一个ListState，用来保存当前窗口所有的count结果
    lazy val itemCountListState: ListState[ItemViewCount] =
        getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemCountList", classOf[ItemViewCount]))


    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
        // 每来一条数据就保存到状态中
        itemCountListState.add(value)

        // 注册定时器，在 windowEnd+100 触发。
        // 正常情况，需要将定时器的时间戳保存在状态中。判断是否有定时器才注册。
        // 但由于下面两个原因，可以简化
        // ① 当前定时器只针对当前key；
        // ② 定时器不会重复注册，定时器的时间戳可以看做是定时器的id。所以不用担心重复注册。
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    // 定时器触发时，从状态中取数据，然后排序输出
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
        val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()

        // 直接获取到的是java迭代器，无法直接sort。同时也不能使用scala 的 <- 方式遍历，所以引入隐式转换
        //        val lists: lang.Iterable[ItemViewCount] = itemCountListState.get()

        import scala.collection.JavaConversions._
        for (itemCount <- itemCountListState.get()) {
            allItemCountList += itemCount
        }

        // 按照 count 的大小进行排序
        val sortedItemCountList: mutable.Seq[ItemViewCount] =
            allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

        // 清除状态
        itemCountListState.clear()

        // 将排名信息格式化为 String 格式
        val result: StringBuilder = new mutable.StringBuilder()
        result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
        // 遍历sorted列表，输出 TopN信息
        for (i <- sortedItemCountList.indices) {
            // 获取当前商品count的信息
            val currentItemCount: ItemViewCount = sortedItemCountList(i)
            result.append("Top").append(i + 1).append(":")
                .append(" 商品ID=").append(currentItemCount.itemId)
                .append(" 访问量=").append(currentItemCount.count)
                .append("\n")
        }
        result.append("===================================\n\n")

        // 控制输出频率
        Thread.sleep(1000)

        out.collect(result.toString())
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