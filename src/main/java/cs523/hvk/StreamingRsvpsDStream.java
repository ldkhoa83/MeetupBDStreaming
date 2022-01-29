package cs523.hvk;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
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
    private static final int BATCH_DURATION_INTERVAL_MS = 60000;

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;

    private static final String KAFKA_BROKERS = "10.211.55.2:29092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "earliest";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";

    private static String TABLE_NAME = "rsvps";
    private static final byte[] CF_BASIC_INFO = Bytes.toBytes("basic_info");
    private static final byte[] CF_VENUE = Bytes.toBytes("venue");
    private static final byte[] CF_MEMBER = Bytes.toBytes("member");
    private static final byte[] CF_EVENT = Bytes.toBytes("event");
    private static final byte[] CF_GROUP = Bytes.toBytes("group");

    private static final byte[] C_ID = Bytes.toBytes("id");
    private static final byte[] C_MTIME = Bytes.toBytes("mtime");
    private static final byte[] C_VISIBILITY = Bytes.toBytes("visibility");
    private static final byte[] C_RESPONSE = Bytes.toBytes("response");
    private static final byte[] C_GUESTS = Bytes.toBytes("guests");
    private static final byte[] C_VENUE_NAME = Bytes.toBytes("name");
    private static final byte[] C_LON = Bytes.toBytes("lon");
    private static final byte[] C_LAT = Bytes.toBytes("lat");
    private static final byte[] C_VENUE_ID = Bytes.toBytes("id");
    private static final byte[] C_EVENT_NAME = Bytes.toBytes("name");
    private static final byte[] C_EVENT_ID = Bytes.toBytes("id");
    private static final byte[] C_EVENT_TIME = Bytes.toBytes("time");
    private static final byte[] C_EVENT_URL = Bytes.toBytes("url");
    private static final byte[] C_MEM_ID = Bytes.toBytes("id");
    private static final byte[] C_MEM_NAME = Bytes.toBytes("name");
    private static final byte[] C_TOPICS = Bytes.toBytes("topics");
    private static final byte[] C_CITY = Bytes.toBytes("city");
    private static final byte[] C_COUNTRY = Bytes.toBytes("country");
    private static final byte[] C_STATE = Bytes.toBytes("state");
    private static final byte[] C_GROUP_ID = Bytes.toBytes("id");
    private static final byte[] C_G_NAME = Bytes.toBytes("name");
    private static final byte[] C_G_LON = Bytes.toBytes("lon");
    private static final byte[] C_G_LAT = Bytes.toBytes("lat");

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
        final ColumnFamilyDescriptor venueColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(CF_VENUE).build();
        final ColumnFamilyDescriptor memberColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(CF_MEMBER).build();
        final ColumnFamilyDescriptor eventColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(CF_EVENT).build();
        final ColumnFamilyDescriptor groupColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(CF_GROUP).build();
        final TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                .setColumnFamilies(Arrays.asList(infoColumnFamily,venueColumnFamily,memberColumnFamily,eventColumnFamily,groupColumnFamily)).build();

        // if (admin.tableExists(table.getTableName())) {
        //     admin.disableTable(table.getTableName());
        //     admin.deleteTable(table.getTableName());
        // }

        if (!admin.tableExists(table.getTableName())) {
            admin.createTable(table);
        }

        // Configuration hadoopConfig = new Configuration();
        // FileSystem hdfs = FileSystem.get(hadoopConfig);

        final JavaInputDStream<ConsumerRecord<String, String>> meetupStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
                );

        JavaPairDStream<ImmutableBytesWritable, Put> data = meetupStream
                .map(c -> c.value())
                .map(Parser::parse)
                .filter(Objects::nonNull)
                .mapToPair(d -> {
                    return new Tuple2<>(new ImmutableBytesWritable(), rSVPDataToPut(d));});
        
        JavaPairDStream<Text, NullWritable> textData = meetupStream
                    .map(ConsumerRecord::value)
                    .map(Parser::parse)
                    .mapToPair(d -> {
                        return new Tuple2<>(new Text(d.toString()),NullWritable.get());});

       JobConf jobConf = new JobConf(HBaseConfiguration.create(), StreamingRsvpsDStream.class);
       jobConf.setOutputFormat(TableOutputFormat.class);
       jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        data.foreachRDD( r-> {
             r.saveAsHadoopDataset(jobConf);
        });
        textData.foreachRDD(r -> {
            r.saveAsHadoopFile("/input", Text.class, NullWritable.class, TextOutputFormat.class);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    // private static void merge(FileSystem hdfs, Configuration hadoopConfig, String srcPath, String dstPath, String fileName) throws IllegalArgumentException, IOException {
    //     Path destinationPath = new Path(dstPath);
    //     System.out.println("####"+destinationPath);
    //     if (!hdfs.exists(destinationPath)) {
    //       hdfs.mkdirs(destinationPath);
    //     }
    //     FileUtil.copy(hdfs, new Path(srcPath), hdfs, new Path(dstPath + "/" + fileName), false, true, hadoopConfig);
    //   }

    public static Put rSVPDataToPut(RSVP rsvp) {
        final Put put = new Put(Bytes.toBytes(rsvp.getId()));
        addStringColumn(put,CF_BASIC_INFO, C_ID, String.valueOf(rsvp.getId()));
        addStringColumn(put,CF_BASIC_INFO, C_VISIBILITY, rsvp.getVisibility());
        addStringColumn(put,CF_BASIC_INFO, C_RESPONSE, rsvp.getResponse());
        addStringColumn(put,CF_BASIC_INFO, C_GUESTS, String.valueOf(rsvp.getGuests()));
        addStringColumn(put,CF_BASIC_INFO, C_MTIME, rsvp.getMtime());
        addStringColumn(put,CF_VENUE, C_VENUE_ID, rsvp.getVenueId());
        addStringColumn(put,CF_VENUE, C_VENUE_NAME, rsvp.getVenueName());
        addStringColumn(put,CF_VENUE, C_LON, String.valueOf(rsvp.getVenueLon()));
        addStringColumn(put,CF_VENUE, C_LAT, String.valueOf(rsvp.getVenueLat()));
        addStringColumn(put,CF_MEMBER, C_MEM_ID, rsvp.getMemId());
        addStringColumn(put,CF_MEMBER, C_MEM_NAME, rsvp.getMemName());
        addStringColumn(put,CF_EVENT, C_EVENT_ID, rsvp.getEventId());
        addStringColumn(put,CF_EVENT, C_EVENT_NAME, rsvp.getEventName());
        addStringColumn(put,CF_EVENT, C_EVENT_TIME, rsvp.getEventTime());
        addStringColumn(put,CF_EVENT, C_EVENT_URL, rsvp.getEventURL());
        addStringColumn(put,CF_GROUP, C_GROUP_ID, rsvp.getGroupId());
        addStringColumn(put,CF_GROUP, C_G_NAME, rsvp.getGroupName());
        addStringColumn(put,CF_GROUP, C_TOPICS, rsvp.getGroupTopics());
        addStringColumn(put,CF_GROUP, C_G_LAT, String.valueOf(rsvp.getGroupLat()));
        addStringColumn(put,CF_GROUP, C_G_LON, String.valueOf(rsvp.getGroupLon()));
        addStringColumn(put,CF_GROUP, C_CITY, rsvp.getGroupCity());
        addStringColumn(put,CF_GROUP, C_COUNTRY, rsvp.getGroupCountry());
        addStringColumn(put,CF_GROUP, C_STATE, rsvp.getGroupState());
        return put;
    }

    public static void addStringColumn(Put put, byte[] cfColumn, byte[] column, String value){
        if(value != null) put.addColumn(cfColumn, column, Bytes.toBytes(value));
    }

    public static class Parser {
        public static RSVP parse(String s) {
            JSONObject obj = new JSONObject(s);
            RSVP rsvp = new RSVP(obj.getString("rsvp_id"));

            if(obj.has("event")){
                JSONObject event = obj.getJSONObject("event");
                String eventName = event.optString("event_name");
                String eventId = event.optString("event_id");
                String eventTime = event.optString("time");
                String eventURL = event.optString("event_url");

                rsvp.setEventId(eventId);
                rsvp.setEventName(eventName);
                rsvp.setEventTime(eventTime);
                rsvp.setEventURL(eventURL);
            }

            if(obj.has("member")){
                JSONObject member = obj.getJSONObject("member");
                String memId = member.optString("member_id");
                String memName = member.optString("member_name");

                rsvp.setMemId(memId);
                rsvp.setMemName(memName);
            }
           
            if(obj.has("group")){
                JSONObject group = obj.getJSONObject("group");
                String gCity = group.optString("group_city");
                String gState = group.optString("group_state");
                String gCountry = group.optString("group_country");
                String gId = group.optString("group_id");
                String gName = group.optString("group_name");
                Double gLon = group.optDouble("group_lon");
                Double gLat = group.optDouble("group_lat");
                JSONArray groupTopics = group.optJSONArray("group_topics");
                List<String> topics = new ArrayList<>();
                for (int i = 0; i < groupTopics.length(); i++ ) {
                    topics.add(groupTopics.getJSONObject(i).getString("topic_name")); 
                }

                rsvp.setGroupTopics(String.join(",",topics));
                rsvp.setGroupCity(gCity);
                rsvp.setGroupState(gState);
                rsvp.setGroupCountry(gCountry);
                rsvp.setGroupLat(gLat);
                rsvp.setGroupLon(gLon);
                rsvp.setGroupName(gName);
                rsvp.setGroupId(gId);
                }
           
            if(obj.has("venue")){
                JSONObject venue = obj.getJSONObject("venue");
                String venueName = venue.optString("venue_name");
                Double venueLon = venue.optDouble("lon");
                Double venueLat = venue.optDouble("lat");
                String venueId = venue.optString("venue_id");

                rsvp.setVenueName(venueName);
                rsvp.setVenueId(venueId);
                rsvp.setVenueLat(venueLat);
                rsvp.setVenueLon(venueLon);
                rsvp.setVenueId(venueId);
            }

            rsvp.setMtime(obj.optString("mtime"));
            rsvp.setResponse(obj.optString("response"));
            rsvp.setGuests(Integer.parseInt(obj.optString("guests","0")));
            rsvp.setVisibility(obj.optString("visibility"));
           return rsvp;
        }
    }

    public static class RSVP {

        private String id;
        private String mtime;
        private String response;
        private int guests;
        private String visibility;
        private String venueName;
        private double venueLon;
        private double venueLat;
        private String venueId;
        private String eventName;
        private String eventId;
        private String eventTime;
        private String eventURL;
        private String memId;
        private String memName;
        private String groupTopics;
        private String groupCity;
        private String groupState;
        private String groupCountry;
        private String groupId;
        private String groupName;
        private double groupLon;
        private double groupLat;

        public RSVP(String id) {
            this.id = id;
        }

        public String getId(){
            return this.id;
        }
        public String getMtime(){
            return this.mtime;
        }
        public String getResponse(){
            return this.response;
        }
        public int getGuests(){
            return this.guests;
        }
        public String getVenueName(){
            return this.venueName;
        }
        public double getVenueLon(){
            return this.venueLon;
        }
        public double  getVenueLat() {
            return this.venueLat;
        }
        public String  getVenueId(){
            return this.venueId;
        }
        public String getEventName(){
            return this.eventName;
        }
        public String getEventId(){
            return this.eventId;
        }
        public String getEventTime(){
            return this.eventTime;
        }
        public String  getEventURL(){
            return this.eventURL;
        }
        public String getMemId(){
            return this.memId;
        }
        public String getMemName(){
            return this.memName;
        }
        public String getGroupTopics(){
            return this.groupTopics;
        }
        public String getGroupCity(){
            return this.groupCity;
        }
        public String  getGroupState(){
            return this.groupState;
        }
        public String  getGroupId(){
            return this.groupId;
        }
        public String  getGroupName(){
            return this.groupName;
        }
        public double getGroupLon(){
            return this.groupLon;
        }
        public double getGroupLat(){
            return this.groupLat;
        }
        public void setId(String id) {
            this.id = id;
        }

        public void setMtime(String mtime) {
            this.mtime = Instant.ofEpochMilli(Long.parseLong(mtime)).toString();
        }

        public void setResponse(String response) {
            this.response = response;
        }

        public void setGuests(int guests) {
            this.guests = guests;
        }

        public void setVenueName(String venueName) {
            this.venueName = venueName;
        }

        public void setVenueLon(double venueLon) {
            this.venueLon = venueLon;
        }

        public void setVenueLat(double venueLat) {
            this.venueLat = venueLat;
        }

        public void setVenueId(String venueId) {
            this.venueId = venueId;
        }

        public void setEventName(String eventName) {
            this.eventName = eventName;
        }

        public void setEventId(String eventId) {
            this.eventId = eventId;
        }

        public void setEventTime(String eventTime) {
            this.eventTime = Instant.ofEpochMilli(Long.parseLong(eventTime)).toString();
        }

        public void setEventURL(String eventURL) {
            this.eventURL = eventURL;
        }

        public void setMemId(String memId) {
            this.memId = memId;
        }

        public void setMemName(String memName) {
            this.memName = memName;
        }

        public void setGroupTopics(String groupTopics) {
            this.groupTopics = groupTopics;
        }

        public void setGroupCity(String groupCity) {
            this.groupCity = groupCity;
        }

        public void setGroupState(String groupState) {
            this.groupState = groupState;
        }

        public void setGroupCountry(String groupCountry) {
            this.groupCountry = groupCountry;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        public void setGroupLon(double groupLon) {
            this.groupLon = groupLon;
        }

        public void setGroupLat(double groupLat) {
            this.groupLat = groupLat;
        } 
    
        public String getVisibility() {
            return visibility;
        }
    
        public void setVisibility(String visibility) {
            this.visibility = visibility;
        }
    
        public String getGroupCountry() {
            return groupCountry;
        }

        @Override
        public String toString() {
            List<String> texts = Arrays.asList(id,mtime,response,String.valueOf(guests),visibility,venueId,venueName,
                            String.valueOf(venueLon),String.valueOf(venueLat),eventId,eventName,eventTime,eventURL,memId,
                            memName,groupId,groupName,groupTopics,String.valueOf(groupLon),String.valueOf(groupLat),groupCity,groupState,groupCountry);
            return String.join("\t", texts);
        }

    }
}
