package com.hong

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object HotItems {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val path: String = "E:\\code\\Flink-WSR-Project\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
        val inputStream: DataStream[String] = env.readTextFile(path)
        val dataStream: DataStream[UserBehavior] = inputStream.

    }
}

case class UserBehavior(
                       userId: Long,
                       itemId: Long,
                       categoryId: Int,
                       behavior: String,
                       timestamp: Long
                       )
