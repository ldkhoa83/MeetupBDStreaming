package cs523.hvk;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
// import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class StreamingRsvpsDStreamCountWindow {

    private static final String APPLICATION_NAME = "Streaming Rsvps DStream Count Window";
    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local";
    private static final String CHECKPOINT_FOLDER = "/home/hdoop/spack_checkpointing";
    
    private static final int BATCH_DURATION_INTERVAL_MS = 1000;
    private static final int WINDOW_LENGTH_MS = 30000;
    private static final int SLIDING_INTERVAL_MS = 5000;
    
    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES; 

    private static final String KAFKA_BROKERS = "10.211.55.2:29092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "latest";	
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";
    private static final Collection<String> TOPICS = 
            Collections.unmodifiableList(Arrays.asList(KAFKA_TOPIC));	

    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_RESET_TYPE);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KAFKA_CONSUMER_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);
    }
	
    private static final String HBASE_TABLE_NAME = "meetup_test";
    private static final String CF_INFO = "infor";
    private static final String COL_COUNT = "Count";
        
    public static void main(String[] args) throws InterruptedException, IOException {

        // System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

        // set up Hadoop HBase configuration using TableOutputFormat
         Configuration hBaseConf = HBaseConfiguration.create();
        // hBaseConf.set("TableOutputFormat.OUTPUT_TABLE", HBASE_TABLE_NAME);
        // final JobConf jobConfig = new JobConf(hBaseConf, StreamingRsvpsDStreamCountWindow.class);
        // jobConfig.setOutputFormat(TableOutputFormat.class);

        // try (Connection connection = ConnectionFactory.createConnection(hBaseConf);
	// 			Admin admin = connection.getAdmin())
	// 	{
	// 		TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(HBASE_TABLE_NAME))
	// 									.setColumnFamilies(Lists.newArrayList(ColumnFamilyDescriptorBuilder.of(CF_INFO))).build();

	// 		System.out.print("Creating table.... ");

	// 		if (!admin.tableExists(table.getTableName()))
	// 		{
        //         admin.createTable(table);
	// 		}
        // }




//          SparkConf conf = new SparkConf()
//                 .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
//                 .setAppName(APPLICATION_NAME)
//                 // .set("spark.mongodb.output.uri", MONGODB_OUTPUT_URI)
//                 // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                 .set("spark.driver.allowMultipleContexts","true")
//                 .set("spark.streaming.kafka.consumer.cache.enabled", "false");


//         JavaSparkContext jsc = new JavaSparkContext(conf);

//         JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hBaseConf);

//         final JavaStreamingContext streamingContext
//                 = new JavaStreamingContext(conf, new Duration(BATCH_DURATION_INTERVAL_MS));

//         streamingContext.checkpoint(CHECKPOINT_FOLDER);

//         final JavaInputDStream<ConsumerRecord<String, String>> meetupStream =
//                 KafkaUtils.createDirectStream(
//                         streamingContext,
//                         LocationStrategies.PreferConsistent(),
//                         ConsumerStrategies.<String, String>Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
//                 );
                
//         // transformations, streaming algorithms, etc
//         JavaDStream<Long> countStream  
//             = meetupStream.countByWindow(
//                  new Duration(WINDOW_LENGTH_MS), 
//                  new Duration(SLIDING_INTERVAL_MS));        

//         AtomicInteger id = new AtomicInteger(1);
//         HbaseTable table = new HbaseTable();
//         countStream.foreachRDD((JavaRDD<Long> countRDD) -> {                
//             // MongoSpark.save(        
//             //         countRDD.map(
//             //             r -> Document.parse("{\"rsvps_count\":\"" + String.valueOf(r) + "\"}")
//             //         )
//             // );            
//         //     countRDD.mapToPair(r -> {
//         //         double rowKey = Math.random();
//         //         Put p = new Put(Bytes.toBytes(String.valueOf(rowKey)));
//         //         p.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes(COL_COUNT), Bytes.toBytes(String.valueOf(r)));
//         //         return new Tuple2<ImmutableBytesWritable,Put>(new ImmutableBytesWritable(Bytes.toBytes(String.valueOf(rowKey))), p);
//         //     }).saveAsHadoopDataset(jobConfig);

// // try (Connection connection = ConnectionFactory.createConnection(hBaseConf);)
// // 		{
// //             Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
// // 		Put p = addRecordData(id, name, price);
// // 		table.put(p);
// // 		table.close();
// //         System.out.println("Record "+id+" inserted");
// //         }

// //                         id.getAndIncrement();

//         });

        
//         // streamBulkPut(hbaseContext, HBASE_TABLE_NAME, countStream);
        
//         // some time later, after outputs have completed
//         meetupStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> meetupRDD) -> {        
//             OffsetRange[] offsetRanges = ((HasOffsetRanges) meetupRDD.rdd()).offsetRanges();            

//             ((CanCommitOffsets) meetupStream.inputDStream())
//                 .commitAsync(offsetRanges, new MeetupOffsetCommitCallback());
//         });
        
//         streamingContext.start();
//         streamingContext.awaitTermination();    
//     }

//     public static void streamBulkPut(JavaHBaseContext jhc, String tableName, JavaDStream<Long> putDStream) {
// 		jhc.streamBulkPut(putDStream, TableName.valueOf(tableName), i -> {
//                 double rowKey = Math.random();
//                 Put p = new Put(Bytes.toBytes(String.valueOf(rowKey)));
//                 p.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes(COL_COUNT), Bytes.toBytes(String.valueOf(i)));
//                 return p;
//         });
	}
}

// final class MeetupOffsetCommitCallback implements OffsetCommitCallback, Serializable {

//     private static final long serialVersionUID = 42L;

//     private static final Logger log = Logger.getLogger(MeetupOffsetCommitCallback.class.getName());

//     @Override
//     public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//         log.info("---------------------------------------------------");
//         log.log(Level.INFO, "{0} | {1}", new Object[]{offsets, exception});
//         log.info("---------------------------------------------------");
//     }
// }
