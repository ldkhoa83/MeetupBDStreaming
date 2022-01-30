package cs523.hvk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class VisualizingRsvps {

    public static void main(String[] args) {
        String eventSpreading = "create external table if not exists " +
        "events_spreading_rsvps (rowkey string, event_lon double, event_lat double, events_num int) \n" +
        "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' \n" +
        "WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,data:event_lon,data:event_lat,data:events_num') \n"+
        "TBLPROPERTIES('hbase.table.name' = 'realtime_event_spreading')";

        String hotTopic = "create external table if not exists " +
        "daily_hot_topic_keyword (rowkey string, event_date string, hot_topic_keyword string, occurance int) \n" +
        "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' \n" +
        "WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,data:event_date,data:hot_topic,data:occ_num') \n"+
        "TBLPROPERTIES('hbase.table.name' = 'daily_hot_topic')";

        Connection con = null;
        try {
            String conStr = "jdbc:hive2://"+AppProperties.get("hive2.thrift.uri")+"/default";
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            con = DriverManager.getConnection(conStr, "hdoop", "");
            Statement stmt = con.createStatement();

            stmt.execute(eventSpreading);
            stmt.execute(hotTopic);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (con != null)
                    con.close();
            } catch (Exception ex) {
            }
        }   
    }

}
