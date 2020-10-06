package com.hong.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderPayTxMatch_2Flow_Co {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\OrderLog.csv"
        val path2: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\ReceiptLog.csv"

        // 订单流
        val orderEventStream: DataStream[OrderEvent] = env.readTextFile(path)
            .map(data => {
                val arr: Array[String] = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
                override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
            })
            .filter(_.txId != "") //过滤出pay的数据。txID为空的不是pay事件数据
            .keyBy(_.txId) // 可以提前将指定之后两个流的连接key，变成KeyedStream。
        //            也可以connect之后再得到ConnectedStreams，再指定连接key

        // 收据流
        val receiptEventStream: DataStream[ReceiptEvent] = env.readTextFile(path2)
            .map(data => {
                val arr: Array[String] = data.split(",")
                ReceiptEvent(arr(0), arr(1), arr(2).toLong)
            })
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
                    override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
                })
            .keyBy(_.txId)

        // 用connect连接两条流，匹配事件进行处理
        //        orderEventStream.connect(receiptEventStream).keyBy()  //如果两条流没有转成keyedStream，这样指定
        val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
            .connect(receiptEventStream)
            .process(new OrderPayTxDetect())

        val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
        val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

        resultStream.print("matched")
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")
        env.execute("order pay tx match job")
    }
}

// 自定义CoProcessFunction，实现两条流的join
class OrderPayTxDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 用两个valueState，保存当前交易对应的支付事件和对账事件
    lazy val payState: ValueState[OrderEvent] =
        getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))

    lazy val receiptState: ValueState[ReceiptEvent] =
        getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

    val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipts")

    override def processElement1(pay: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        // pay 来了，考察有没有对应的receipt来过
        val receipt = receiptState.value()
        if(receipt != null){
            // 如果已经有receipt，那么正常匹配，输出到主流
            out.collect((pay, receipt))
            receiptState.clear()
        }else{
            // 如果receipt还没来，那么把pay放到状态中，注册一个定时器等待receipt
            payState.update(pay)
            ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
        }
    }

    override def processElement2(receipt: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        // receipt 来了，考察有没有对应的pay来过
        val pay = payState.value()
        if(pay != null){
            // 如果已经有pay，正常匹配，输出主流
            out.collect((pay, receipt))
            payState.clear()
        }else{
            // 如果pay没来， 那么将receipt存入状态中，注册3秒 定时器等待
            receiptState.update(receipt)
            ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
        }

    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        // 定时器触发有两种情况。判断当前有没有 pay/receipt
        // 如果pay不为空，说明receipt没来。输出unmatchedPays
        if(payState.value() != null){
            ctx.output(unmatchedPays, payState.value())
        }
        if(receiptState.value() != null){
            ctx.output(unmatchedReceipts, receiptState.value())
        }

        // 清空状态
        payState.clear()
        receiptState.clear()
    }
}

/*
ewr342as4,wechat,1558430845
sd76f87d6,wechat,1558430847
 */
// 定义到账数据的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)


