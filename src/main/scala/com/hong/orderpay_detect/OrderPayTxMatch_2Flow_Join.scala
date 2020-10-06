package com.hong.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderPayTxMatch_2Flow_Join {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\OrderLog.csv"
        val path2: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\ReceiptLog.csv"

        // 订单流
        val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(path)
            .map(data => {
                val arr: Array[String] = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
                override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
            })
            .filter(_.txId != "")
            .keyBy(_.txId)

        // 收据流
        val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(path2)
            .map(data => {
                val arr: Array[String] = data.split(",")
                ReceiptEvent(arr(0), arr(1), arr(2).toLong)
            })
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
                    override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
                })
            .keyBy(_.txId)


        // 使用join连接两条流（要求是keyedStream）
        val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
            .intervalJoin(receiptEventStream)
            .between(Time.seconds(-3), Time.seconds(5))
            .process(new OrderPayTxDetectWithJoin())

        resultStream.print()
        env.execute("order pay tx match with join job")
    }

}

// 自定义ProcessJoinFunction。 只能输出能匹配上的数据，相当于innerJoin。所以使用CoProcessFunction可以输出更多信息
class OrderPayTxDetectWithJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    override def processElement(left: OrderEvent,
                                right: ReceiptEvent,
                                ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        out.collect(left, right)
    }
}
