package main.java.org.apache.hadoop.hbase.master.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Type;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hbase.thirdparty.com.google.gson.Gson;


@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
public class SnapshotTableManager {
  private final Gson gson;
  private final Configuration conf;
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotTableManager.class);

  public SnapshotTableManager(Configuration conf) throws IOException {
    this.conf = conf;
    createTableIfNotExists();
    this.gson = new Gson();
  }


  public void writeRegionManifest(SnapshotDescription desc, SnapshotRegionManifest regionData) {
    LOG.info("In write region manifest...");
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      LOG.info("Connected...");
      TableName tableName = TableName.valueOf("hbase:snapshot");
      LOG.info("My sent obj:" + regionData.toString());
      try (Table table = connection.getTable(tableName)) {
        Put put = new Put(Bytes.toBytes(regionData.getRegionInfo().hashCode()));
        put.addColumn(Bytes.toBytes("description"), Bytes.toBytes("desc"),
          Bytes.toBytes(gson.toJson(desc)));
        put.addColumn(Bytes.toBytes("description"), Bytes.toBytes("name"),
          Bytes.toBytes(desc.getTable()));

//        String regionDataJSON=gson.toJson(regionData);
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        regionData.writeTo(byteArrayOutput);
        byte[] regionDataBytes = byteArrayOutput.toByteArray();

        put.addColumn(Bytes.toBytes("regionManifests"), Bytes.toBytes("region"),
          Bytes.toBytes(ByteBuffer.wrap(regionDataBytes)));


        table.put(put);
        LOG.info("Write done...");
      } catch (IOException e) {
        e.fillInStackTrace();
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      e.fillInStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void createTableIfNotExists() throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Admin admin = connection.getAdmin()) {
        TableName tableName = TableName.valueOf("hbase:snapshot");

        // Check if the table exists
        if (!admin.tableExists(tableName)) {
          ColumnFamilyDescriptor generalDesc =
            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("description")).build();
          ColumnFamilyDescriptor regionManDesc =
            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("regionManifests")).build();
          ColumnFamilyDescriptor dataManifestDesc =
            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("dataManifest")).build();

          // Define table descriptor
          TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamilies(Arrays.asList(generalDesc, regionManDesc, dataManifestDesc)).build();

          // Create the table
          admin.createTable(tableDescriptor);

          LOG.info("hbase:snapshot table created successfully.");
        } else {
          LOG.info("hbase:snapshot table already exists.");
        }
      }
    }
  }

  public List<SnapshotRegionManifest> loadRegionManifests(SnapshotDescription desc) {
    TableName tableName = TableName.valueOf("hbase:snapshot");

    try (Connection connection = ConnectionFactory.createConnection(conf);
      Table table = connection.getTable(tableName)) {
      Scan scan = new Scan();
      LOG.info("Desc JSON to be checked:" + gson.toJson(desc));
      SingleColumnValueFilter valueFilter =
        new SingleColumnValueFilter(Bytes.toBytes("description"), Bytes.toBytes("name"),
          CompareOperator.EQUAL, Bytes.toBytes(desc.getTable()));
      valueFilter.setFilterIfMissing(true);
      scan.setFilter(valueFilter);

      ResultScanner scanner = table.getScanner(scan);
      List<SnapshotRegionManifest> regionManifests = new ArrayList<>();

      for (Result result : scanner) {
        byte[] jsonBytes = result.getValue(Bytes.toBytes("regionManifests"), Bytes.toBytes("region"));
        String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
        LOG.info("Iterating over region manifest.." + jsonString);

        SnapshotRegionManifest s = SnapshotRegionManifest.parseFrom(jsonBytes);
        LOG.info("My received obj:" + s.toString());
        regionManifests.add(s);
      }
      return regionManifests;
    } catch (Exception e) {
      e.fillInStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void deleteRegionManifests(SnapshotDescription desc) {

    // Specify the table name
    TableName tableName = TableName.valueOf("hbase:snapshot");

    try (Connection connection = ConnectionFactory.createConnection(conf);
      Admin admin = connection.getAdmin();
      Table table = connection.getTable(tableName)) {

      Filter filter = new SingleColumnValueFilter(Bytes.toBytes("description"), Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes(desc.getTable()));

      Scan scan = new Scan();
      scan.setFilter(filter);

      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        byte[] rowKey = result.getRow();

        Delete delete = new Delete(rowKey);
        table.delete(delete);
        LOG.info("Deleted row: " + Bytes.toString(rowKey));
      }
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
  }
}

