package cs523.hvk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class VisualizingRsvps {

    public static void main(String[] args) {
        String hiveCreSta = "create external table if not exists " +
        "events_spreading_rsvps_hive (state string, CAST(event_num as int)) \n" +
        "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' \n" +
        "WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,data:state,data:events_num \n"+
        "TBLPROPERTIES('hbase.table.name' = 'events_spreading')";

        Connection con = null;
        try {
            String conStr = "jdbc:hive2://localhost:10000/default";
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            con = DriverManager.getConnection(conStr, "hdoop", "");
            Statement stmt = con.createStatement();

        
            stmt.execute(hiveCreSta);
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
