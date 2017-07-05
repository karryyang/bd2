import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by lhp on 2017/7/3.
 */
public class StreamingTest {
    public static void main(String[] args) throws InterruptedException{
        SparkConf conf=new SparkConf().setAppName("StreamingTest").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(5));
        //jssc.checkpoint("hdfs://192.168.1.105:9000/streaming_checkpoint");

        // 正式开始进行代码的编写
        // 实现咱们需要的实时计算的业务逻辑和功能

        // 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
        // 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）

        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<>();
/*        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));*/
        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        kafkaParams.put("group.id", "heheda");
        kafkaParams.put("enable.auto.commit", "false");

        // kafkaParams.put("metadata.broker.list","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<>();
        for(String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream =
                KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        adRealTimeLogDStream.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
