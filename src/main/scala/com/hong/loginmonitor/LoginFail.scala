package com.hong.loginmonitor

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFail {
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

        // 用ProcessFunction进行转换，如果遇到连续2秒内登录失败，就发出报警
        val loginWarningStream: DataStream[Warning] = loginEventStream
            .keyBy(_.userId)
            .process(new LoginFailWarning(2))
    }
}

class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    // 定义 List 状态变量，用来保存 2秒内 所有登陆失败的记录。（因为需求要取出第一条和最后一条的时间）
    lazy val loginFailListState: ListState[LoginEvent] =
        getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFail", classOf[LoginEvent]))

    // 定义value状态，用来保存定时器的时间戳
    lazy val timerTsState: ValueState[Long] =
        getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))


    override def processElement(value: LoginEvent,
                                ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                                out: Collector[Warning]): Unit = {
        // 判断当前数据是否登录失败。。
        if(value.eventType == "fail"){  // 这里并处理不了乱序数据。

            // 如果失败，那么添加到 ListState里，如果没有注册定时器，就注册
            loginFailListState.add(value)
            if(timerTsState.value() == 0){
                val ts: Long = value.eventTime * 1000L + 2000L
                ctx.timerService().registerEventTimeTimer(ts)
                timerTsState.update(ts)
            }
        }else{
            //情景1：可能存在第一条登录失败的记录，然后注册了2秒后的定时器，但是这两秒内都没有新的数据进入，
            //那么这时候，不是只有一条登录失败就会触发定时器的任务了吗？
            // 是的，会触发，但是怎么处理在 onTime中控制，对于小于最大监控条数的不处理。

            // 情景2：在第一条进入并注册了定时器，2s 内又连续进入3条fail的信息，大于监控的条数。但是这时候
            // 还是 2s 内，有一条success的记录进来，这时候清空了状态。但是在2s内，它确实有2条以上登录失败的情况，
            // 那么如果按照现在的处理方式，这部分异常记录放过去了吗？
            // todo 改进：不使用定时器，来一条数据进

            // 如果登录成功，清空状态
            ctx.timerService().deleteEventTimeTimer(timerTsState.value())
            loginFailListState.clear()
            timerTsState.clear()

        }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext,
                         out: Collector[Warning]): Unit = {
        // 如果两秒后的定时器触发了，那么判断ListState中失败的记录个数
        val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
        val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
        while(iter.hasNext){
            allLoginFailList += iter.next()
        }

        if(allLoginFailList.size >= maxFailTimes){
            out.collect(Warning(ctx.getCurrentKey,
                allLoginFailList.head.eventTime,
                allLoginFailList.last.eventTime,
            "login fail in 2s for " + allLoginFailList.size + "times."))
        }

    }
}

/*
5402,83.149.11.115,success,1558430815
23064,66.249.3.15,fail,1558430826
 */
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(useId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

