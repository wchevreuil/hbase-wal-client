package com.wal.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;

public class WALClient {

  private static final List<String> SYSTEM_TABLES = new ArrayList<String>();

  static {
    SYSTEM_TABLES.add("hbase:meta");
    SYSTEM_TABLES.add("hbase:names");

  }

  public static Map<String, List<String>> cfMap = new HashMap<String, List<String>>();

  private class Record {
    String table;
    String rowKey;
    String cellDescription;
    Long timestamp;
    String value;
    Boolean isDelete;
    Long writeTime;

    @Override
    public String toString() {

      return this.table + " | " + this.rowKey + " | " + this.cellDescription + " | "
          + this.timestamp + " | " + this.value + " | " + this.isDelete + " | " + this.writeTime;

    }

  }

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      throw new IllegalArgumentException("Invalid number of params");
    }

    WALClient recover = new WALClient();

    switch (args[0]) {
    case "check_missing":
      recover.printWalEntriesDetails(new Path(args[1]),
        (args.length == 3 && args[1].equals("verbose")) ? true : false);
      break;
    case "recover_missing":
      recover.recoverMissingEdits(new Path(args[1]));
      break;

    }

  }

  private void readDistinctEntries(WAL.Reader log, Map<String, Record> readEntries)
      throws IOException {

    WAL.Entry entry;

    while ((entry = log.next()) != null) {

      WALKey key = entry.getKey();

      WALEdit edit = entry.getEdit();

      long writeTime = key.getWriteTime();

      String tblName = Bytes.toString(key.getTablename().getName());

      for (Cell c : edit.getCells()) {

        Record r = new Record();

        r.writeTime = writeTime;

        r.table = tblName;

        r.rowKey = Bytes.toString(c.getRow());

        r.cellDescription = Bytes.toString(c.getFamily()) + ":" + Bytes.toString(c.getQualifier());

        r.timestamp = c.getTimestamp();

        r.value = Bytes.toString(c.getValue());

        r.isDelete = (r.value == null || r.value.equals(""));

        String mapKey = r.table + "|" + r.rowKey + "|" + r.cellDescription;

        Record existing = readEntries.get(mapKey);

        if (existing != null && existing.timestamp > r.timestamp) {
          System.out.println("---->found existing record more recent then current read: ");
          System.out.println("existing: " + existing.toString());
          System.out.println("current: " + r.toString());
          System.out.println("-------------------");
        } else {
          readEntries.put(mapKey, r);
        }

      }

    }

  }

  private Collection<Record> getDistinctEntries(Path walsPath, Map<String, Record> readEntries)
      throws IOException {

    Configuration config = HBaseConfiguration.create();

    FileSystem fs = FileSystem.get(config);

    for (FileStatus f : fs.listStatus(walsPath)) {

      System.out.println(f.getPath().getName() + " - " + new Date(f.getAccessTime()));

      if (f.isFile()) {

        try {

          WAL.Reader log = WALFactory.createReader(fs, f.getPath(), config);

          readDistinctEntries(log, readEntries);

        } catch (IOException e) {
          FSDataInputStream is = fs.open(f.getPath());

          long init = System.currentTimeMillis();
          while (is.read() >= 0) {

          }
          System.out.println("Finished in: " + (System.currentTimeMillis() - init));

          System.out.println("ignoring file: " + f.getPath());

        }

      } else {
        getDistinctEntries(f.getPath(), readEntries);
      }

    }

    fs.close();

    return readEntries.values();

  }

  private void insertMissing(HTable table, Record r) throws IOException {

    String[] dellDesc = r.cellDescription.split(":");

    Put put = new Put(Bytes.toBytes(r.rowKey));

    put.addColumn(Bytes.toBytes(dellDesc[0]), Bytes.toBytes(dellDesc[1]), Bytes.toBytes(r.value));

    table.put(put);

  }

  public void recoverMissingEdits(Path walsPath) throws IOException {

    Configuration config = HBaseConfiguration.create();

    Map<String, Record> readEntries = new HashMap<String, Record>();

    Collection<Record> records = this.getDistinctEntries(walsPath, readEntries);

    for (Record r : records) {

      if (!WALClient.SYSTEM_TABLES.contains(r.table)
          && !r.cellDescription.startsWith("METAFAMILY")) {

        HTable table = new HTable(config, r.table);

        String[] cellDesc = r.cellDescription.split(":");

        Get get = new Get(Bytes.toBytes(r.rowKey));

        get.addColumn(Bytes.toBytes(cellDesc[0]), Bytes.toBytes(cellDesc[1]));

        System.out.println(">>>> " + r);

        Result result = table.get(get);

        if (result.isEmpty()) {

          if (!r.isDelete) {
            // do the put, as the missing edit is an update and the table has no equivalent cell
            // currently
            this.insertMissing(table, r);

            System.out.println(">>> inserting: " + r);
          }

        } else {

          Cell cell = result.rawCells()[0];

          long currentTs = cell.getTimestamp();

          if (currentTs < r.timestamp) {

            if (r.isDelete) {

              // delete this cell current on the table, as the missing edit is a delete with a
              // higher timestamp
              Delete delete = new Delete(Bytes.toBytes(r.rowKey));

              delete.addColumn(Bytes.toBytes(cellDesc[0]), Bytes.toBytes(cellDesc[1]));

              System.out.println(">>> deleting: " + r);

            } else {
              // do the put, as the missing edit is an update with a higher timestamp
              this.insertMissing(table, r);

              System.out.println(">>> inserting: " + r);
            }

          }

        }

        table.close();

      }
    }

  }

  public void printWalEntriesDetails(Path walsPath, boolean verbose) throws Exception {

    Map<String, Long> tableEntries = new HashMap<String, Long>();

    Map<String, Record> readEntries = new HashMap<String, Record>();

    for (Record r : this.getDistinctEntries(walsPath, readEntries)) {

      tableEntries.put(r.table, tableEntries.get(r.table) != null ? tableEntries.get(r.table) + 1
          : 1L);

      if (verbose) {

        System.out.println("--------");
        System.out.println(r.toString());
        System.out.println("--------");

      }

    }

    System.out.println("-----SUMMARY-----");

    for (String table : tableEntries.keySet()) {

      System.out.println("Table: " + table + " had " + tableEntries.get(table) + " entries;");

    }

  }

}
