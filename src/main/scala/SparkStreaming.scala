import java.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ykl on 2017/7/3.
  */
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(
      conf, Durations.seconds(5));
    val topicSet = Set[String]("test123")

    val kafkaParams = Map[String, String](
      ("bootstrap.servers" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)),
      ("group.id" -> "heheda"),
      ("enable.auto.commit"->"false"))
    //val lines = KafkaUtils.createDirectStream(ssc, kafkaParams, )
/*    val liness=lines.flatMap(x=>{
      x._2
    })
    liness.print()*/
    ssc.start()
    ssc.awaitTermination()
  }
}
