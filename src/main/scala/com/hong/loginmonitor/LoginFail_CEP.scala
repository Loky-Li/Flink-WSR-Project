package com.hong.loginmonitor


import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFail_CEP {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\LoginLog.csv"
        val loginEventStream: DataStream[LoginEvent] = env.readTextFile(path)
            .map(data => {
                val arr: Array[String] = data.split(",")
                LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
            })

        //1. 定义一个匹配的模式
        val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
                .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
                .next("secondFail").where(_.eventType == "fail")
                .within(Time.seconds(2))    // 在2秒内检测匹配

        // 2. 在分组之后的数据流上应用模式，得到一个PatternStream
        val patternStream: PatternStream[LoginEvent] =
            CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

        // 3. 将检测到的事件序列，转换输出报警信息
        val loginFailStream: DataStream[Warning] = patternStream
                .select(new LoginFailDetect())

        // 4. 打印输出
        loginFailStream.print()

        env.execute("login fail with cep")
    }
}

// 自定义PatternSelectFunction，用来检测到连续登录失败事件，包装成输出样例类
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning] {

    // patternStream：每检测到一个符合pattern的数据，就斯奥用一次该select方法
    // map里存放的就是匹配到的一个事件，key是定义的事件模式名称
    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
        val firstLoginFail = map.get("firstFail").get(0)
        val secondLoginFail = map.get("secondFail").get(0)  //因为没有times(n)循环模式，所以只有一条记录

        // todo 扩展：视频后面有 使用 time()的演示。但是并不是严格近邻。

        Warning(firstLoginFail.userId,
            firstLoginFail.eventTime,
            secondLoginFail.eventTime,
        "login fail!")
    }

}
