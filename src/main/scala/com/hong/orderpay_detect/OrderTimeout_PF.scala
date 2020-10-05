package com.hong.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
在使用CEP的实现的方式中，对应pay记录来了，假设时间戳是10，但是水位线有莫过了pay的时间10后，create还没有来，
create的数据会被flink丢弃。而实际场景中，发生了pay，那create一定是有的，即使flink把这条记录丢失，
我们也应该做一个告警，说明该数据已经丢失。
所以在这里使用了ProcessFunction的方式，做更加精细化的管理控制。
 */
object OrderTimeout_PF {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\OrderLog.csv"
        val orderEventStream: DataStream[OrderEvent] = env.readTextFile(path)
            .map(data => {
                val arr: Array[String] = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
                override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
            })

        // 自定义ProcessFunction， 做精细化流程控制
        val orderResultStream: DataStream[OrderResult] = orderEventStream
            .keyBy(_.orderId)
            .process(new OrderPayMatchDetech())

        // 打印输出
        orderResultStream.print("payed")

        val outputTag = new OutputTag[OrderResult]("timeout")
        orderResultStream.getSideOutput(outputTag).print("timeout")

        env.execute("order timeout without cep job")
    }
}

// 自定义KeyedProcessFunction，  主流输出正常支付的记录，侧输出输出超时报警订单
class OrderPayMatchDetech() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    // 定义状态，用来保存是否来过 create 和 pay 事件的标志位，以及定时器时间戳
    lazy val isPayedState: ValueState[Boolean] =
        getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))

    lazy val isCreatedState: ValueState[Boolean] =
        getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))

    lazy val timerTsState: ValueState[Long] =
        getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))

    val orderTimeOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")


    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                out: Collector[OrderResult]): Unit = {
        // 先取出当前的状态
        val isPayed: Boolean = isPayedState.value()
        val isCreated: Boolean = isCreatedState.value()
        val timerTs: Long = timerTsState.value()

        // 判断当前时间的类型，分不同情况讨论
        // 情况1： 来的是create，要继续判断之前是否有pay来过
        if (value.eventType == "create") {
            // 情况1.1 ： 如果已经pay过的话，匹配成功
            if (isPayed) {
                out.collect(OrderResult(value.orderId, "payed successfully"))
                isPayedState.clear()
                timerTsState.clear()
                ctx.timerService().deleteEventTimeTimer(timerTs)
            }
            // 情况1.2： 如果没有pay过的话，那么就注册一个15分钟后的定时器，开始等待
            else {
                val ts: Long = value.eventTime * 1000L + 15 * 60 * 1000L
                ctx.timerService().registerEventTimeTimer(ts)
                timerTsState.update(ts)
                isCreatedState.update(true)
            }
        }
        // 情况2： 来的是pay，要继续判断是否来过create
        else if (value.eventType == "pay") {
            // 情况2.1： 如果create已经来过，匹配成功，并判断是否超过15分钟
            if (isCreated) {
                // 情况2.1.1：如果没有超时，正常输出结果到主流
                if (value.eventTime * 1000L < timerTs) {
                    out.collect(OrderResult(value.orderId, "payed successfully"))
                }
                // 情况2.1.2： 如果已经超时，输出到侧输出流
                else {
                    ctx.output(orderTimeOutputTag, OrderResult(value.orderId, "payed but already timeout"))
                }

                // 无论哪种情况，都已经有了输出，清空状态
                ctx.timerService().deleteEventTimeTimer(timerTs)
                timerTsState.clear()
                isCreatedState.clear()
            }
            //情况2.2： 如果create没来，需要等待乱序create，注册一个当前pay时间戳的定时器
            else{
                val ts: Long = value.eventTime * 1000L
                ctx.timerService().registerEventTimeTimer(ts)   //两个定时器注册的情况是互斥的
                timerTsState.update(ts)
                isPayedState.update(true)
            }
        }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                         out: Collector[OrderResult]): Unit = {
        // 定时器的触发要判断是那种情况的定时器。两种是互斥的
        if(isPayedState.value()){
            //如果pay过，那么说明create没来，可能出现数据丢失的异常情况
            ctx.output(orderTimeOutputTag,
                OrderResult(ctx.getCurrentKey, "already payed but not found crated log"))
        }else{
            // 如果没有payed过，那么说明真正15分钟超时
            ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
        }
    }
}
