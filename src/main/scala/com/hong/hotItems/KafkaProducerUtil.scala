package com.hong.hotItems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {
    def main(args: Array[String]): Unit = {
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\UserBehavior.csv"
        writeToKafkaWithTopic("hot_items", path)
    }

    def writeToKafkaWithTopic(topic: String, path: String): Unit ={
        val properties = new Properties()

        properties.setProperty("bootstrap.servers", "hadoop203:9092")
        properties.setProperty("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")

        // 创建一个kafka producer，读取文件的数据并发送
        val producer = new KafkaProducer[String, String](properties)

        // 从文件中读取数据，逐条发送
        val bufferedSource = io.Source.fromFile(path)
        for(line <- bufferedSource.getLines()){
            val record = new ProducerRecord[String, String](topic, line)
            producer.send(record)
        }

        producer.close()

    }

}
