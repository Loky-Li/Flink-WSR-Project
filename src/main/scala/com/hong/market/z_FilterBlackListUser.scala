package com.hong.market

import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class z_FilterBlackListUser(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    // 保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 标记当前（用户，广告）作为key是否第一次发送到黑名单
    lazy val firstSent: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstsent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳，届时清空重置状态
    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
        val curCount = countState.value()
        // 如果是第一次处理，注册一个定时器，每天 00：00 触发清除
        if( curCount == 0 ){
            val ts = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000)
            resetTime.update(ts)
            ctx.timerService().registerProcessingTimeTimer(ts)
        }
        // 如果计数已经超过上限，则加入黑名单，用侧输出流输出报警信息
        if( curCount > maxCount ){
            if( !firstSent.value() ){
                firstSent.update(true)
                ctx.output( new OutputTag[BlackListWarning]("black_list"), BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today.") )
            }
            return
        }
        // 点击计数加1
        countState.update(curCount + 1)
        out.collect( value )
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
        if( timestamp == resetTime.value() ){
            firstSent.clear()
            countState.clear()
        }
    }
}
