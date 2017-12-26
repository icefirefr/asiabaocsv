package com.asiabao.hadoop.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDAO {

    private Admin admin = null;
    private Configuration cfg = null;
    private Connection connection;
    private String tableName;
    private String columnfamily;

    private static final Log LOG = LogFactory.getLog(HbaseDAO.class);

    public HbaseDAO() throws Exception {
        cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum",
                "node2.master.zdt.com,node1.master.zdt.com,node1.slave.zdt.com,node2.slave.zdt.com,node8.slave.zdt.com");
//        cfg.set("hbase.zookeeper.quorum", "node3.abdata.com");

        cfg.set("hbase.zookeeper.property.clientPort", "2181");
//        cfg.set("zookeeper.znode.parent", "/hbase-unsecure");

        try {
            connection = ConnectionFactory.createConnection(cfg);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean existTable(String table) throws IOException {
        TableName tableName = TableName.valueOf(table);
        return admin.tableExists(tableName);
    }

    public byte[][] calcSplitKeys(Set<String> ids, String siteId) throws ParseException {
        byte[][] splitKeys = new byte[ids.size()][];
        long time = System.currentTimeMillis();
        String[] idss = ids.toArray(new String[ids.size()]);
        for (int i = 0; i < idss.length; i++) {
            splitKeys[i] = RowKeyGenerator.getIns().generateRowKey(siteId, idss[i], time);
        }
        return splitKeys;
    }

    // 创建一张表，指定表名，列族
    public void createTable(Set<String> ids, String siteId) throws Exception {
        TableName tableName = TableName.valueOf(this.tableName);
        if (admin.tableExists(tableName)) {
            LOG.debug(tableName + "存在！");
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(this.tableName));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(this.columnfamily);
        columnDescriptor.setCompressionType(Algorithm.SNAPPY);
        tableDesc.addFamily(columnDescriptor);
        admin.createTable(tableDesc, calcSplitKeys(ids, siteId));
        LOG.debug(tableName + "创建表成功！");
    }

    public List<String> getAllTables() {
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                    LOG.debug("表:　" + hTableDescriptor.getNameAsString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }

    // Hbase中往某个表中添加一条记录
    public boolean addOneRecord(String table, String key, String family, String col, byte[] dataIn) throws IOException {
        Table tb = connection.getTable(TableName.valueOf(table));
        HTable t;
        Put put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), col.getBytes(), dataIn);
        try {
            tb.put(put);
            LOG.trace("插入数据条 " + key + "成功！！！");
            return true;
        } catch (IOException e) {
            LOG.error("插入数据条" + key + "失败！！！");
            return false;
        }
    }

    public boolean addRecords(String table, String family, List<Row> rows) throws IOException {
        Table tb = connection.getTable(TableName.valueOf(table));
        HTable t;

        List<Put> puts = new ArrayList<Put>();
        for (Row r : rows) {
            Put p = new Put(Bytes.toBytes(r.getRowKey()));
            for (Column c : r.getCols()) {
                p.addColumn(family.getBytes(), Bytes.toBytes(c.getKey()), Bytes.toBytes(c.getValues()));
            }
            puts.add(p);
        }
        // Put put = new Put(key);
        // for(Column col :cols){
        // put.addColumn(family.getBytes(), col.getKey().getBytes(), col.getValues().getBytes());
        // }
        try {
            tb.put(puts);
            // LOG.trace("插入数据条 " + Bytes.toString(key) + "成功！！！");
            return true;
        } catch (IOException e) {
            LOG.error("插入数据条" + "" + "失败！！！");
            return false;
        }
    }

    // Hbase表中记录信息的查询
    public void getValueFromKey(String table, String key) throws IOException {
        Table tb = connection.getTable(TableName.valueOf(table));
        Get get = new Get(key.getBytes());
        try {
            Result rs = tb.get(get);
            if (rs.rawCells().length == 0) {
                LOG.debug("不存在关键字为" + key + "的行！!");

            } else {
                for (Cell kv : rs.rawCells()) {
                    System.out.println(
                            new String(kv.getTagsArray(), "UTF-8") + " " + new String(kv.getValueArray(), "UTF-8"));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 显示所有数据，通过HTable Scan类获取已有表的信息
    public void getAllData(String tableName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (Cell kv : r.rawCells()) {
                LOG.debug(new String(kv.getTagsArray(), "UTF-8") + new String(kv.getValueArray(), "UTF-8"));
            }
        }
    }

    // Hbase表中记录信息的删除
    public boolean deleteRecord(String tablename, String key) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Delete de = new Delete(key.getBytes());
        try {
            table.delete(de);
            return true;
        } catch (IOException e) {
            System.out.println("删除记录" + key + "异常！！！");
            return false;
        }
    }

    // Hbase中表的删除
    public boolean deleteTable(String TABLE) {
        try {
            TableName table = TableName.valueOf(TABLE);
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
                LOG.debug("删除表" + table + "!!!");
            }
            return true;
        } catch (IOException e) {
            LOG.error("删除表" + TABLE + "异常!!!");
            return false;
        }
    }

    public List<Map<String, String>> findByTime(final String tableName, String siteId, String devid, Date start,
            Date end, long max) throws IOException {
        long before = System.currentTimeMillis();
        LOG.info("开始查询...");
        Scan scan = new Scan();
        final String family = columnfamily;
        scan.addFamily(Bytes.toBytes(family));
        String startRows = RowKeyGenerator.getIns().getRowKey(siteId, devid, String.valueOf(start.getTime()));
        LOG.debug("startRow " + startRows);
        String stopRows = RowKeyGenerator.getIns().getRowKey(siteId, devid, String.valueOf(end.getTime()));
        LOG.debug("stopRow " + stopRows);
        byte[] startRow = Bytes.toBytes(startRows);
        byte[] stopRow = Bytes.toBytes(stopRows);
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        scan.setMaxResultSize(max);
        List<Map<String, String>> ls = new ArrayList<Map<String, String>>();

        Table table = connection.getTable(TableName.valueOf(tableName));

        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> it = resultScanner.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            List<Cell> cells = r.listCells();
            Map<String, String> row = new HashMap<String, String>();
            for (Cell cell : cells) {
                byte[] key = cell.getQualifier();
                String keys = Bytes.toString(key);
                byte[] value = cell.getValue();
                String values = Bytes.toString(value);
                row.put(keys, values);
            }
            ls.add(row);
        }

        float elapsedTime = System.currentTimeMillis() - before;
        LOG.info("查询HBASE耗时  " + elapsedTime / 1000 + " 秒");
        return ls;
    }

    public PageResult getNext(int pageSize, final String tableName, String siteId, String devid,
           String startRows,Date start,Date end) throws Exception {

        long before = System.currentTimeMillis();
        LOG.info("开始查询...");
        Scan scan = new Scan();
        final String family = columnfamily;
        scan.addFamily(Bytes.toBytes(family));
        if(StringUtils.isBlank(startRows)){
            startRows = RowKeyGenerator.getIns().getRowKey(siteId, devid, String.valueOf(start.getTime()));
        }
        String endRows = null;
        endRows = RowKeyGenerator.getIns().getRowKey(siteId, devid, String.valueOf(end.getTime()));
        LOG.debug("startRow " + startRows);
        LOG.debug("endRows " + endRows);
        byte[] startRow = Bytes.toBytes(startRows);
        byte[] endRow = Bytes.toBytes(endRows);
        Filter filter = new PageFilter(pageSize + 1);
        scan.setFilter(filter);
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner result = table.getScanner(scan);
        Iterator<Result> it = result.iterator();

        List<Map<String, String>> ls = new ArrayList<Map<String, String>>();
        int count = 0;
        String nextStartRow = "";
        while (it.hasNext()) {
            count++;
            Result r = it.next();
            if (count == pageSize + 1) {
                nextStartRow = Bytes.toString(r.getRow());
                break;
            }
            List<Cell> cells = r.listCells();
            Map<String, String> row = new HashMap<String, String>();
            for (Cell cell : cells) {
                byte[] key = cell.getQualifier();
                String keys = Bytes.toString(key);
                byte[] value = cell.getValue();
                String values = Bytes.toString(value);
                row.put(keys, values);
            }
            ls.add(row);
        }
        float elapsedTime = System.currentTimeMillis() - before;
        LOG.info("查询HBASE耗时  " + elapsedTime / 1000 + " 秒");
        return new PageResult(ls, nextStartRow);
    }

//    public void save(User u) throws IOException {
        // Table tb = connection.getTable(TableName.valueOf(tableName));
        // Put put = new Put(u.getRowkey());
        // for (int i = 0; i < 3; i++) {
        // put.addColumn(CF_INFO, Bytes.toBytes("ne" + i), Bytes.toBytes(u.getName()));
        // put.addColumn(CF_INFO, Bytes.toBytes("el" + i), Bytes.toBytes(u.getEmail()));
        // put.addColumn(CF_INFO, Bytes.toBytes("pd" + i), Bytes.toBytes(u.getPassword()));
        // put.addColumn(CF_INFO, Bytes.toBytes("sx" + i), Bytes.toBytes(u.getSex()));
        // put.addColumn(CF_INFO, Bytes.toBytes("ht" + i), Bytes.toBytes(u.getHeight()));
        // put.addColumn(CF_INFO, Bytes.toBytes("ae" + i), Bytes.toBytes(u.getAge()));
        // put.addColumn(CF_INFO, Bytes.toBytes("pe" + i), Bytes.toBytes(u.getPhone()));
        // put.addColumn(CF_INFO, Bytes.toBytes("ct" + i), Bytes.toBytes(u.isContent()));
        // }
        //
        // put.addColumn(CF_INFO, Bytes.toBytes("ne" + 3), Bytes.toBytes(u.getName()));
        // put.addColumn(CF_INFO, Bytes.toBytes("el" + 3), Bytes.toBytes(u.getEmail()));
        // put.addColumn(CF_INFO, Bytes.toBytes("pd" + 3), Bytes.toBytes(u.getPassword()));
        // put.addColumn(CF_INFO, Bytes.toBytes("sx" + 3), Bytes.toBytes(u.getSex()));
        // put.addColumn(CF_INFO, Bytes.toBytes("ht" + 3), Bytes.toBytes(u.getHeight()));
        // put.addColumn(CF_INFO, Bytes.toBytes("ae" + 3), Bytes.toBytes(u.getAge()));
        //
        // tb.put(put);
//    }

//    public void save1(List<User> users) throws IOException {
        // Table tb = connection.getTable(TableName.valueOf(tableName));
        // List<Put> puts = new ArrayList<Put>();
        // for (User u : users) {
        // Put put = new Put(u.getRowkey());
        // put.addColumn(CF_INFO, qUser, Bytes.toBytes(u.getName()));
        // put.addColumn(CF_INFO, qEmail, Bytes.toBytes(u.getEmail()));
        // put.addColumn(CF_INFO, qPassword, Bytes.toBytes(u.getPassword()));
        // put.addColumn(CF_INFO, qSex, Bytes.toBytes(u.getSex()));
        // put.addColumn(CF_INFO, qHeight, Bytes.toBytes(u.getHeight()));
        // puts.add(put);
        // }
        // tb.put(puts);
//    }

    public void setTableCommit(boolean flag) throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        table.setAutoFlush(flag);
        table.setWriteBufferSize(1024 * 1024 * 200);
    }

    public void flush() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        table.flushCommits();
    }

    public void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    public void init(String tableName, String columnfamily) {
        this.tableName = tableName;
        this.columnfamily = columnfamily;
    }
}
