package com.hong.orderpay_detect

import java.util
import java.util.UUID

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeout {
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

        // 1. 定义一个要匹配的事件模式。
        val orderPayPattern = Pattern
            .begin[OrderEvent]("create").where(_.eventType == "create")
            // 创建订单后，可以有其他的操作，所以是宽松近邻
            .followedBy("pay").where(_.eventType == "pay")
            .within(Time.seconds(15))

        // 2. 将pattern 应用在按照 orderId分组的数据流上。
        val patternStream: PatternStream[OrderEvent] = CEP.pattern(
            orderEventStream.keyBy(_.orderId),
            orderPayPattern)

        // 3. 定义一个侧输出流的标签，用来标明超时时间的侧输出流
        val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

        // 4. 调用select方法，提取匹配时间和超时事件，分别进行处理转换输出
        val resultStream: DataStream[OrderResult] = patternStream
            .select(orderTimeoutOutputTag,
                new OrderTimeoutSelect(),
                new OrderPaySelect())

        // 5. 打印输出
        resultStream.print("payed")
        resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

        env.execute("order timeout detect job")

/*      todo 源码中。select不仅可以传入PatternSelectFunction 处理正常匹配的数据
         还可以传入对超时事件处理的 patternTimeoutFunction
      def select[L: TypeInformation, R: TypeInformation](
        outputTag: OutputTag[L],
        patternTimeoutFunction: PatternTimeoutFunction[T, L],
        patternSelectFunction: PatternSelectFunction[T, R])
      : DataStream[R] = {
        val cleanedSelect = cleanClosure(patternSelectFunction)
        val cleanedTimeout = cleanClosure(patternTimeoutFunction)

        asScalaStream(
          jPatternStream
          .select(outputTag, cleanedTimeout, implicitly[TypeInformation[R]], cleanedSelect)
        )
      }
        */
    }
}

// 自定义超时处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTs: Long): OrderResult = {
        // 超时的在“pay”中无数据，只在“create”中有
        val timeOrderId: Long = map.get("create").iterator().next().orderId
        OrderResult(timeOrderId, "timeout at" + timeoutTs)
    }
}

// 自定义正常的数据处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
        val payOrderId = map.get("pay").get(0).orderId
        OrderResult(payOrderId, "payed successfully")
    }
}
/*class OrderPaySelect() extends PatternProcessFunction[OrderEvent, OrderResult] {
    override def processMatch(map: util.Map[String, util.List[OrderEvent]],
                              context: PatternProcessFunction.Context,
                              collector: Collector[OrderResult]): Unit = ???
}*/

case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
case class OrderResult(orderId: Long, resultMsg: String)
