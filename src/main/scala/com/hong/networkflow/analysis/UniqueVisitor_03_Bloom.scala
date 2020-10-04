package com.hong.networkflow.analysis

import com.hong.hotItems.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UniqueVisitor_03_Bloom{
    /**
     *  第二种方式，虽然没有将整个窗口的数据缓存，但是Set作为累积器，底层是个状态变量，同样也保存了很多的数据。
     *  方式三：将Set累加器，变成布隆过滤器，只有bit的大小
     * @param args
     */

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据
        val path: String = "E:\\code\\Flink-WSR-Project\\src\\main\\resources\\UserBehavior.csv"
        val inputStream: DataStream[String] = env.readTextFile(path)

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

        val uvStream: DataStream[UvCount] = dataStream
                .filter(_.behavior == "pv")
                .map(data => ("uv", data.userId))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .trigger(new MyTrigger())
            // 下面是 trigger触发计算的计算逻辑。
                .process(new UvCountResultWithBloomFilter())



        uvStream.print("UvCount")

        env.execute("uv job")
    }
}

// 自定义触发器，每来一条数据，就触发一次窗口计算，避免窗口的数据积压太多
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

//    TriggerResult: 有两种状态组合成4种操作， fire表示触发窗口计算，purge表示清空窗口
    // 每来一条数据，就触发计算并清空状态
    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.FIRE_AND_PURGE

//        ctx.registerEventTimeTimer()      // 直接注册，无需调出 timeServer()
        // ctx在trigger里面还可以注册定时器，并在到达时间调用下面的 onXXXTime()方法。
        // 水位线触发窗口计算的原理，即使这里的trigger来实现的。
        // 也就是说，本例中，trigger FIRE 对应触发的是后面processFunction的动作，
        // 而trigger自己定义的定时器，对应触发 trigger内的 onXXXTime()方法的动作。
        // todo 具体trigger的使用，见旧版代码（左元版）
    }

    //表示：xxxx，本次不做操作
    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // 清空状态
    override def clear(window: TimeWindow,
                       ctx: Trigger.TriggerContext): Unit = {}
}

// 自定义 ProcessFunction， 把当前数据进行处理，位图保存在redis中
// 由于trigger定义了每来一条数据进行一次计算。计算也就是调用.trigger()后的方法。
class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

    var jedis: Jedis = _
    var bloom: Bloom = _


    override def open(parameters: Configuration): Unit = {
        jedis = new Jedis("hadoop203", 6379)

        // 位图大小确定，假设1亿用户。 未压缩 10^8 * 100byte ≈ 10G。
        // 压缩后：10^8 * 1bit * 10倍(扩容) ≈ 10^3 M*bit ≈ (1000/8) M ≈ 128M
        // 压缩比：其实就是 原来的 100byte / (1bit * 扩容)。
        // 位图大小： 128M = 2^7 * 2^10 * 2^10 * 2^3 = 2 ^ 30 bit
        bloom = new Bloom(1<<30)
    }


    /**
     * 每来一个 数据，使用布隆过滤器判断redis中位图中的对应位置是否为1（即是否存在数据）
     * @param key
     * @param context
     * @param elements  // todo 这里不是同样需要保存整个窗口数据吗？不是的，由于触发操作调用该方法
     *                  且指定了 TriggerResult.FIRE_AND_PURGE 清空窗口状态，所以数据和状态数据会清空！
     * @param out
     */
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UvCount]): Unit = {

        // todo 注意：实际存在redis的有两个数据。分别是存布隆过滤器得到bit String，和统计结果 Hash
        // bitmap用当前窗口的end作为key，保存在redis中的结构为 String（windowEnd, bitmap）
        val storedKey = context.window.getEnd.toString


        // 将每个窗口的 uv count值，作为状态也存入到redis中，存为 countMap表。
        // Hash结构 （countMap, windowEnd, count）
        val countMap = "countMap"
        // 先获取到当前的count值
        var count = 0L
        if(jedis.hget(countMap, storedKey) != null){
            count = jedis.hget(countMap, storedKey).toLong

            // 取 userId，计算hash值，判断是否在位图中
            val userId = elements.last._2.toString
            val offset = bloom.hash(userId, 61)

            // 获取redis中hash结果的key对应位的value。
            val isExist = jedis.getbit(storedKey, offset)

            // 如果不存在，则将对应位置 置1, count+1， 否则不操作
            if( !isExist ){
                jedis.setbit(storedKey, offset, true)   // todo 更新 String 数据结构
                jedis.hset(countMap, storedKey, (count+1).toString)     // 更新统计的hash 结构
            }
        }

    }
}

// 自定义一个布隆过滤器
class Bloom(size: Long) extends Serializable{
    // 定义位图的大小, 2的整次幂
    private val cap = size

    // 实现一个 hash函数
    def hash(str: String, seed: Int): Long = {
        var result = 0
        for(i <- 0 until str.length){
            result = result * seed + str.charAt(i)  // 这样i越到，result越大
        }

        // 返回一个在 cap 范围内的一个值。 使用 位与 运行。
        // 如 cap： 000100 （4） & 011010 （26），得到 000000，在大于4的部分都被“与”运算后，变为0
        // 而cap是2的整次幂（假设为m），1+m个0，为了保证都小于 2^m，则应该为 2^m-1,即 11111（m个1），
        // 这样可以直接将 result中 大于 2^m-1 的部分截去，保留后半部。
        (size - 1) & result


    }
}


