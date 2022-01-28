package cs523.hvk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.Date;

public class HbaseTable {

    private static final String TABLE_NAME = "meetup_test";

    private static final String CF_DEFAULT = "info";

	private static final String NAME = "Count";
	private static final String PRICE = "price";
	private static final String TIME = "time";

    private Configuration config = null;

    public HbaseTable() {
        this.config = HBaseConfiguration.create();
    }

    private Put addRecordData(int id, String name, double price) throws IOException {
        byte[] row = Bytes.toBytes(id);
        Put p = new Put(row);
        p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(NAME), Bytes.toBytes(name));
        // p.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(PRICE), Bytes.toBytes(String.valueOf(price)));
        // p.addImmutable(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(TIME), Bytes.toBytes(String.valueOf(time.getTime())));
        return p;
    }

    public void insertData(int id, String name, double price) throws IOException {
		// instantiate HTable class
        try (Connection connection = ConnectionFactory.createConnection(config);)
		{
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		Put p = addRecordData(id, name, price);
		table.put(p);
		table.close();
        System.out.println("Record "+id+" inserted");
        }

    }

}
