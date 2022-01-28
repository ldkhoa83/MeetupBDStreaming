package cs523.hvk;

import java.io.IOException;
import java.time.LocalTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;
import org.json.*;


public class StreamingRsvpsDStream {

    private static final String APPLICATION_NAME = "Streaming Rsvps DStream";
    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    private static final int BATCH_DURATION_INTERVAL_MS = 5000;

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;

    private static final String KAFKA_BROKERS = "10.211.55.2:29092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "latest";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";

    private static String TABLE_NAME = "rsvps";
    private static final byte[] CF_BASIC_INFO = Bytes.toBytes("basic_info");
    private static final byte[] C_ID = Bytes.toBytes("id");
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


    public static void main(String[] args) throws InterruptedException, IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master","localhost:60000");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "localhost");
        Connection connection = ConnectionFactory.createConnection(config);

        final SparkConf conf = new SparkConf()
                .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                .setAppName(APPLICATION_NAME);

        final JavaStreamingContext streamingContext
                = new JavaStreamingContext(conf, new Duration(BATCH_DURATION_INTERVAL_MS));

            Admin admin = connection.getAdmin();
            final ColumnFamilyDescriptor infoColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(CF_BASIC_INFO).build();
            final TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                    .setColumnFamilies(Arrays.asList(infoColumnFamily)).build();

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);

//        {
//            "venue": {
//            "venue_name": "Online event",
//                    "lon": 179.1962,
//                    "lat": -8.521147,
//                    "venue_id": 26906060
//        },
//            "visibility": "public",
//                "response": "yes",
//                "guests": 0,
//                "member": {
//            "member_id": 337451013,
//                    "photo": "https://secure.meetupstatic.com/photos/member/e/7/e/1/thumb_308939361.jpeg",
//                    "member_name": "Ms. S"
//        },
//            "rsvp_id": 1901706362,
//                "mtime": 1643260471792,
//                "event": {
//            "event_name": "Learn how to stop inadvertently amplifying your painful emotions and sensations",
//                    "event_id": "283073187",
//                    "time": 1643741100000,
//                    "event_url": "https://www.meetup.com/evidence-based-self-help/events/283073187/"
//        },
//            "group": {
//            "group_topics": [
//            {
//                "urlkey": "understanding-yourself",
//                    "topic_name": "Understanding Yourself"
//            },
//            {
//                "urlkey": "you-can-heal-your-life",
//                    "topic_name": "You Can Heal Your Life"
//            },
//            {
//                "urlkey": "help-each-other-experience-growth",
//                    "topic_name": "Help Each Other Experience Growth"
//            },
//            {
//                "urlkey": "mental-illness-young-adults",
//                    "topic_name": "Mental Illness & Young Adults"
//            },
//            {
//                "urlkey": "self-awareness-workshops",
//                    "topic_name": "Self awareness workshops"
//            },
//            {
//                "urlkey": "self-empowerment-and-action",
//                    "topic_name": "Self Empowerment and Action"
//            },
//            {
//                "urlkey": "social-psychology",
//                    "topic_name": "Social Psychology"
//            },
//            {
//                "urlkey": "finding-happiness",
//                    "topic_name": "Finding Happiness"
//            },
//            {
//                "urlkey": "social-phobia",
//                    "topic_name": "Social Phobia"
//            },
//            {
//                "urlkey": "generalized-anxiety-disorder",
//                    "topic_name": "Generalized Anxiety Disorder"
//            },
//            {
//                "urlkey": "depression",
//                    "topic_name": "Depression"
//            },
//            {
//                "urlkey": "self-help-techniques-for-dealing-with-anxiety",
//                    "topic_name": "Self Help Techniques for Dealing with Anxiety"
//            },
//            {
//                "urlkey": "space-clearing-sacred-space",
//                    "topic_name": "Space Clearing & Sacred Space"
//            },
//            {
//                "urlkey": "higher-self",
//                    "topic_name": "Higher Self"
//            },
//            {
//                "urlkey": "resources-for-adhd",
//                    "topic_name": "Resources for ADHD"
//            }
//    ],
//            "group_city": "London",
//                    "group_country": "gb",
//                    "group_id": 34976561,
//                    "group_name": "Evidence-Based Self-Help for Anxiety, Depression, ADHD, OCD",
//                    "group_lon": -0.18,
//                    "group_urlname": "evidence-based-self-help",
//                    "group_lat": 51.49
//        }
//        }

        final JavaInputDStream<ConsumerRecord<String, String>> meetupStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
                );

        JavaPairDStream<ImmutableBytesWritable, Put> data = meetupStream
//                .filter(f -> !f.value().contains("\"guests\":0"))
                .map(ConsumerRecord::value)
                .map(Parser::parse)
                .filter(Objects::nonNull)
                .mapToPair(d -> {
                    return new Tuple2<>(new ImmutableBytesWritable(), rSVPDataToPut(d));});

       JobConf jobConf = new JobConf(HBaseConfiguration.create(), StreamingRsvpsDStream.class);
       jobConf.setOutputFormat(TableOutputFormat.class);
       jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        data.foreachRDD( r-> {
             r.saveAsHadoopDataset(jobConf);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Put rSVPDataToPut(RSVP rsvp) {
        final Put put = new Put(Bytes.toBytes(rsvp.getDateTime()));
        put.addColumn(CF_BASIC_INFO, C_ID, Bytes.toBytes(rsvp.getId()));
        return put;
    }

    public static class Parser {
        public static RSVP parse(String s) {
            JSONObject obj = new JSONObject(s);
            return new RSVP(obj.getInt("rsvp_id"), obj.getLong("mtime"));
        }
    }

    public static class RSVP {

        private int id;
        private long dateTime;

        public RSVP(int id, long dateTime) {
            this.id = id;
            this.dateTime = dateTime;
        }

        public long getDateTime() {
            return dateTime;
        }

        public int getId() {
            return id;
        }
    }
}
