package cs523.hvk;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;

public class AnalyzingRsvps {

    private static final byte[] HOT_TOPIC_CF = Bytes.toBytes("data");
    private static final byte[] EVENT_DATE_COL = Bytes.toBytes("event_date");
    private static final byte[] HOT_TOPIC_COL = Bytes.toBytes("hot_topic");
    private static final byte[] OCC_TOPIC_COL = Bytes.toBytes("occ_num");
    private static final String HOT_TOPIC_TABLE = "daily_hot_topic";

    public static void main(String[] args) throws SQLException, IOException {


        String hiveCreSta = "create external table if not exists " +
        "rsvps_hive (id string, mtime string, response string, guests string, " +
        "visibility string, venue_id string, venue_name string, venue_lon string, venue_lat string, " +
        "event_id string, event_name string, event_time string, " +
        "event_url string, mem_id string, mem_name string, group_id string, group_name string, group_topics string, group_lon string, " +
        "group_lat string, group_city string, group_state string, group_country string) \n" +
        "row format delimited \n" +
        "fields terminated by '\t' \n" +
        "STORED AS textfile \n" +
        "LOCATION 'hdfs:///input';";

        try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            Admin admin = connection.getAdmin()) {
            final ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(HOT_TOPIC_CF)
                    .setCompressionType(Compression.Algorithm.NONE).build();
            final TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(HOT_TOPIC_TABLE))
                    .setColumnFamilies(Arrays.asList(columnFamily)).build();

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        }

        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("AnalyzingRsvps");

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/output"), true);

        final SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .config("hive.metastore.uris", AppProperties.get("hive.metastore.uris"))
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql(hiveCreSta);

        final JobConf jobConf = new JobConf(HBaseConfiguration.create(), AnalyzingRsvps.class);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, HOT_TOPIC_TABLE);

        final String sql = "SELECT SUBSTR(event_time,1,10) as event_date, group_topics ,COUNT(*) as num_of_events FROM rsvps_hive GROUP BY event_time, group_topics;";

        sparkSession.sql(sql).javaRDD()
        .mapToPair(r -> new Tuple2<>(r.getString(0), Arrays.asList(new Tuple2<>(r.getString(1),r.getLong(2)))))
        .reduceByKey((x,y) -> {List<Tuple2<String,Long>> r = new ArrayList<>();  r.addAll(x); r.addAll(y); return r;})
        .filter(t -> t._2() != null)
        .mapToPair(t -> {
            Map<String,Long> topicMap = new HashMap<>();
            t._2().forEach(i -> {
                if(i != null){
                    for(String topicToken : i._1().split(",|\\s+")){
                        topicToken = topicToken.trim();
                        if(Strings.isNotEmpty(topicToken) && topicToken.matches("\\w+")){
                            if(topicMap.containsKey(topicToken)){
                                topicMap.put(topicToken, topicMap.get(topicToken) + i._2());
                            }else {
                                topicMap.put(topicToken, i._2());
                            }
                        }
                    }
                }
            }); 
            if(topicMap.isEmpty()) return new Tuple2<>(new ImmutableBytesWritable(), toPut("", "", 0L));
            List<Entry<String, Long>> list = new ArrayList<>(topicMap.entrySet());
		    list.sort(Entry.comparingByValue());
            Entry<String,Long> hotTopic = list.get(list.size()-1);
            return new Tuple2<>(new ImmutableBytesWritable(), toPut(t._1(),hotTopic.getKey(),hotTopic.getValue()));
        }).saveAsHadoopDataset(jobConf);


        sparkContext.close();
    }

    private static Put toPut(String eventDate, String hotTopic, Long occuranceNum) {
        Put p = new Put(Bytes.toBytes(String.valueOf(Instant.now().toEpochMilli())));
        p.addColumn(HOT_TOPIC_CF, EVENT_DATE_COL, Bytes.toBytes(eventDate));
        p.addColumn(HOT_TOPIC_CF, HOT_TOPIC_COL, Bytes.toBytes(hotTopic));
        p.addColumn(HOT_TOPIC_CF, OCC_TOPIC_COL, Bytes.toBytes(String.valueOf(occuranceNum)));
        return p;
    }

}