package com.asiabao.hadoop.hbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.csvreader.CsvReader;

public class HaseTestBoot {

    private static String idsFileName = "ids.txt";

    private static final Log LOG = LogFactory.getLog(HaseTestBoot.class);

    public static void main(String[] args) {
        //E:\WorkSpace\asiabao\eclipse_workspace\file-test-info\test\xs xs_point1 xs_info1 xs insert
        //E:\WorkSpace\asiabao\eclipse_workspace\file-test-info\test\xs sw_point sw_info sw query 10 "2013/12/30 13:46:30" "2013/12/30 13:50:30"
        if (args.length == 0) {
            System.out.println("参数错误！！！");
            System.exit(1);
        }

        HaseTestBoot bt = new HaseTestBoot();
        bt.doMain(args);

    }

    private void doMain(String[] args) {
        String path = args[0];
        String tableName = args[1];
        String columnfarily = args[2];
        String siteId = args[3];
        String operation = args[4];
        String devid = args[5];
        String start = args[6];
        String end = args[7];
        if (operation.equals("insert")) {
            try {
                System.out.println("========================！");
                HbaseDAO dao = HbaseDaofactory.getFactory().getHDao();
                if (!dao.existTable(tableName)) {
                    Set<String> devids = collectionInfo(path);
                    dao.init(tableName, columnfarily);
                    dao.createTable(devids, siteId);
                    LOG.info("创建表成功！！！");
                    dao.closeConnection();
                } else {
                    dao.init(tableName, columnfarily);
                    LOG.info("表已经存在");
                }
                List<MyThread> workers = collectionWorkers(path, tableName, siteId, columnfarily);
                for (MyThread wk : workers) {
                    wk.start();
                }

                for (MyThread wk : workers) {
                    wk.join();
                }
                LOG.info("处理完毕");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            try {
                HbaseDAO dao = HbaseDaofactory.getFactory().getHDao();
                dao.init(tableName, columnfarily);
                SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                List<Map<String,String>> r = dao.findByTime(tableName, siteId, devid, sf.parse(start), sf.parse(end),1000L);
                LOG.info("======================================== rows " + r.size());
                for(Map<String,String> m : r){
                    LOG.info("======================================== ");
                    Set<String> sets = m.keySet();
                    for(String key : sets){
                        LOG.info("KEY=> " + key + " value=> " + m.get(key));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void writeFileIds(Set<String> ids) {
        String path = getClass().getResource("/").getFile() + File.separatorChar + idsFileName;
        File f = new File(path);
        if (!f.exists()) {
            f.getParentFile().mkdirs();
        }
        if (ids != null && ids.size() > 0) {
            String[] idss = ids.toArray(new String[ids.size()]);
            String content = "";
            for (String id : idss) {
                content += id + ",";
            }
            int index = content.lastIndexOf(",");
            if (index > -1) {
                content = content.substring(0, index);
            }
            FileWriter fw = null;
            BufferedWriter bw = null;
            try {
                fw = new FileWriter(path);
                bw = new BufferedWriter(fw);
                bw.write(content);
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    fw.close();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }
    }

    private String[] readFileIds() {
        String path = getClass().getResource("/").getFile();
        File file = new File(path + File.separatorChar + idsFileName);
        if (!file.exists()) {
            return null;
        }
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                if (tempString != null && !tempString.equals("")) {
                    return tempString.split(",");
                }
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return null;
    }

    private List<MyThread> collectionWorkers(String path, String tableName, String siteId, String columnfarily)
            throws ParseException {
        List<MyThread> workers = new ArrayList<MyThread>();
        File dir = new File(path);
        System.out.println(dir);
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File f : files) {
                System.out.println("read " + f);
                if (f.isDirectory()) {
                    File[] csv_txts = f.listFiles();
                    for (File fc : csv_txts) {
                        LOG.info("-read " + fc);
                        long b = System.currentTimeMillis();
                        HbaseWorker w = new HbaseWorker(tableName, fc.getAbsolutePath(), siteId, columnfarily);
                        MyThread worker = new MyThread(w);
                        worker.setName(tableName);
                        workers.add(worker);

                        long a = System.currentTimeMillis();
                        LOG.info(fc + " 用时" + (a - b) / 1000);
                    }
                }
            }
        }
        return workers;
    }

    private Set<String> collectionInfo(String path) {
        String[] fs = readFileIds();
        Set<String> sets = new HashSet<String>();
        if (fs != null && fs.length > 0) {
            for (String s : fs) {
                sets.add(s);
            }
            return sets;
        }
        File dir = new File(path);
        LOG.info(dir);
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File f : files) {
                System.out.println("read " + f);
                if (f.isDirectory()) {
                    File[] csv_txts = f.listFiles();
                    for (File fc : csv_txts) {
                        System.out.println("-read " + fc);
                        long b = System.currentTimeMillis();
                        readColumn(fc.getAbsolutePath(), "WTNO", sets);
                        long a = System.currentTimeMillis();
                        LOG.info(fc + " 用时" + (a - b) / 1000);
                        LOG.info("ids " + sets);
                    }
                }
            }
        }
        LOG.info(path + "收集风机编号完毕！！！");
        writeFileIds(sets);
        return sets;
    }

    public void readColumn(String filePath, String column, Set<String> sets) {
        CsvReader csvReader = null;
        try {
            // 创建CSV读对象
            csvReader = new CsvReader(filePath);
            // 读表头
            csvReader.readHeaders();
            while (csvReader.readRecord()) {
                // 读一整行
                // System.out.println(csvReader.getRawRecord());
                // 读这行的某一列
                String col = csvReader.get(column);
                if (col != null) {
                    sets.add(col);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            csvReader.close();
        }
    }

    public class MyThread extends Thread {

        HbaseWorker worker;

        public MyThread() {
            super();
        }

        public MyThread(Runnable target, String name) {
            super(target, name);
        }

        public MyThread(String name) {
            super(name);
        }

        public MyThread(ThreadGroup group, Runnable target, String name, long stackSize) {
            super(group, target, name, stackSize);
        }

        public MyThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        public MyThread(ThreadGroup group, Runnable target) {
            super(group, target);
        }

        public MyThread(ThreadGroup group, String name) {
            super(group, name);
        }

        public MyThread(HbaseWorker target) {
            super(target);
            this.worker = target;
        }

        public void stopThread() throws IOException {
            worker.stop();
        }
    }

}
