package com.asiabao.hadoop.hbase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.csvreader.CsvReader;

public class HaseTestQuery {
    private static final Log LOG = LogFactory.getLog(HaseTestQuery.class);
    private String saveFile = "/home/idx.txt";
    public static void main(String[] args) throws Exception {
        String path= args[0];
        HaseTestQuery hq = new HaseTestQuery();
        hq.saveFile = args[1];
        hq.collectionInfo(path);
    }
    
    private Set<String> collectionInfo(String path) {
        Set<String> sets = new HashSet<String>();
        File dir = new File(path);
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
    
    private  void writeFileIds(Set<String> ids) {
        String path = saveFile;
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

}
