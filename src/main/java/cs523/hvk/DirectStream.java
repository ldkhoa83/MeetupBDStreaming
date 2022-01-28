package cs523.hvk;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
// import org.apache.spark.streaming.kafka.KafkaUtils;
// import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

//https://github.com/apache/spark/blob/v3.1.1/examples/src/main/java/org/apache/spark/examples/streaming/JavaNetworkWordCount.java

//https://github.com/eBay/Spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java

public class DirectStream {
    
    private static final String KAFKA_BROKERS = "10.211.55.2:29092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "latest";	
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";
    Map<String, Object> kafkaParams = new HashMap<>();
    // Create a local StreamingContext with two working thread and batch interval of 1 second
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("BDTSparkKafka");
        //    .set("es.index.auto.create", "true")
        //     .set("es.nodes", "elasticsearch")
        //     .set("es.port", "9200")
        //     .set("es.nodes.wan.only", "true");
    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));

    public DirectStream() {
        kafkaParams.put("bootstrap.servers", "10.211.55.2:29092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "meetupGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
    }
    public static void main(String[] args) throws Exception {
        DirectStream directStream = new DirectStream();
        directStream.start();
    }

    public void start() throws Exception {
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("meetupTopic", 1);
        //todo change the port
        // JavaPairReceiverInputDStream<String, String> messagesStream =
        //         KafkaUtils.createStream(streamingContext, "10.211.55.2:22181", "kafka", topicMap);

        // AtomicInteger id = new AtomicInteger(1);
        // HbaseTable table = new HbaseTable();
        // messagesStream.foreachRDD(stringStringJavaPairRDD -> {
        //     List<Tuple2<String, String>> result = stringStringJavaPairRDD.collect();
        //     if (result.size() > 0) {
        //         List<Long> stockPrices = new ArrayList<>();
        //         result.forEach(tuple2 -> {
        //             String line = tuple2._2();
        //             String[] values = line.split(",");

        //                 // JavaRDD  rdd = streamingContext.sparkContext().parallelize(stockPrices);
        //                 // JavaEsSpark.saveToEs(rdd,"stocks");
        //                 //TODO skip for local testing
        //                 try {
        //                     table.insertData(id.get(), "khoa", 19);
        //                 } catch (IOException e) {
        //                     e.printStackTrace();
        //                 }
        //                 id.getAndIncrement();

                    
        //         });
        //     }
        // });

        streamingContext.start();
        streamingContext.awaitTermination();
    }


}
