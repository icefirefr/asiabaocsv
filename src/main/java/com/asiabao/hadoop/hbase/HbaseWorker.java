package com.asiabao.hadoop.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.csvreader.CsvReader;

public class HbaseWorker implements Runnable {

    private SimpleDateFormat sf = new SimpleDateFormat(" yy-MM-dd  HH:mm:ss");

    private static final Log LOG = LogFactory.getLog(HbaseWorker.class);
    private String workName;
    private long count = 0;
    private HbaseDAO dao;
    private boolean stop = false;
    private boolean test = false;
    private String path;
    private String siteId;
    private String columnfarily;
    public static final String WTNO = "WTNO";
    public static final String DateAcqTime = "DateAcqTime";

    public HbaseWorker(String workName, String path, String siteId, String columnfarily) {
        super();
        this.workName = workName;
        this.path = path;
        this.siteId = siteId;
        this.columnfarily = columnfarily;
    }

    private void collectionInfo(String path) throws ParseException {
        LOG.info("-read " + path);
        long b = System.currentTimeMillis();
        doProcess(path);
        long a = System.currentTimeMillis();
        LOG.info(path + " 用时" + (a - b)/1000);
    }
    
    private int insertCount = 500;
    public void doProcess(String filePath) {
        int count = 0;
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(filePath);
            // 读表头
            csvReader.readHeaders();
            String[] headers = csvReader.getHeaders();
            List<Row> rs = new ArrayList<Row>();
            while (csvReader.readRecord()) {
                // 读一整行
                // System.out.println(csvReader.getRawRecord());
                // 读这行的某一列
                // System.out.println(csvReader.get("Link"));
                List<Column> cols = new ArrayList<Column>();
                for (String key : headers) {
                    String value = csvReader.get(key);
                    Column col = new Column(key, value);
                    cols.add(col);
                }
                String devId = csvReader.get(WTNO);
                String time = csvReader.get(DateAcqTime);
                String rowkey = null;
                try {
                    if(StringUtils.isBlank(time)){
                        LOG.info("=filePath= " + filePath + " devId  " + devId + " time " + time );
                        continue;
                    }
                    rowkey = RowKeyGenerator.getIns().generateRowKeytoString(siteId, devId, time);
                    Row r = new Row(cols,rowkey);
                    rs.add(r);
                    count++;
                    if (count % insertCount == 0){
                        LOG.info(path + " :开始存储数据 大小 " + rs.size());
                        dao.addRecords(workName, columnfarily, rs);
                        rs.clear();
                    }
                } catch (Exception e) {
                    LOG.error(siteId + " " + devId + " " + time);
                    System.out.println("==============" + time + "==============");
                    e.printStackTrace();
                    continue;
                }
            }
            if(rs.size() > 0){//剩余
                dao.addRecords(workName, columnfarily, rs);
            }
            LOG.info(path + " :处理了 " + count);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            csvReader.close();
        }
    }

    @Override
    public void run() {
        try {
            Random rd = new Random();
            dao = HbaseDaofactory.getFactory().getHDao();
            LOG.info(path + "　开始工作");
            collectionInfo(path);
            LOG.info(path + "　开始完毕");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e);
        }
    }

    public void stop() throws IOException {
        stop = true;
        dao.closeConnection();
        LOG.info(workName + " 线程停止");
    }

}
