package cs523.hvk;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
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
    private static final byte[] COLUMN_FAMILY = null;
    private static final byte[] GROUP_TBL_CF = Bytes.toBytes("group");

    public static final String RSVPS_HIVE_TABLE = "rsvps_hive";
    public static final String ANALIZED_RSVPS_GROUP_TABLE = "analized_rsvps_group";
    public static final String EVENTS_SPREADING_TABLE = "events_spreading";
    private static final byte[] EVENTS_SP_CF = Bytes.toBytes("data");
    private static final byte[] G_STATE_COL = Bytes.toBytes("state");
    private static final byte[] EVENTS_NUM_COL = Bytes.toBytes("events_num");

    public static void main(String[] args) throws SQLException, IOException {


        String hiveCreSta = "create external table if not exists " +
        "rsvps_hive (id string, visibility string, response string, guests string, " +
        "mtime string, venue_id string, venue_name string, venue_lon string, venue_lat string, " +
        "mem_id string, mem_name string, event_id string, event_name string, event_time string, " +
        "event_url string, group_id string, group_name string, group_topics string, group_lon string, " +
        "group_lat string, group_city string, group_state string, group_country string) \n" +
        "row format delimited \n" +
        "fields terminated by '\t' \n" +
        "STORED AS textfile \n" +
        "LOCATION 'hdfs:///input';";

        try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            Admin admin = connection.getAdmin()) {
            final ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(EVENTS_SP_CF)
                    .setCompressionType(Compression.Algorithm.NONE).build();
            final TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(EVENTS_SPREADING_TABLE))
                    .setColumnFamilies(Arrays.asList(columnFamily)).build();

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        }

        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("AnalyzingRsvps");

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/output"), true);

        final SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .enableHiveSupport()
                .getOrCreate();



        sparkSession.sql(hiveCreSta);

        final JobConf jobConf = new JobConf(HBaseConfiguration.create(), AnalyzingRsvps.class);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, EVENTS_SPREADING_TABLE);

        final String sql = "SELECT group_state, COUNT(*) as num_events FROM rsvps_hive WHERE group_country = 'us' GROUP BY group_state;";

        sparkSession.sql(sql).javaRDD()
        .mapToPair(r -> new Tuple2<>(new ImmutableBytesWritable(), eventsSpreadingToPut(r)))
        .saveAsHadoopDataset(jobConf);


        sparkContext.close();
    }

    private static Put eventsSpreadingToPut(Row r) {
        Put p = new Put(Bytes.toBytes(String.valueOf(Instant.now().toEpochMilli())));
        p.addColumn(EVENTS_SP_CF, G_STATE_COL, Bytes.toBytes(r.getString(0)));
        p.addColumn(EVENTS_SP_CF, EVENTS_NUM_COL, Bytes.toBytes(String.valueOf(r.getLong(1))));
        return p;
    }
}