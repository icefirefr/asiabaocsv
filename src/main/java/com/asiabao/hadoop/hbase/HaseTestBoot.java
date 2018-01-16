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
        				//文件路径   表名            列簇名       风场id  操作 风场id start end
    	/*String str = "E:/xyd,xyd_point,xyd_info,xyd,insert, , , ";
    	args = str.split(",");*/
    	//System.out.println(args.length);
    	if (args.length == 0) {
            System.out.println("参数错误！！！");
            System.exit(1);
        }

        HaseTestBoot bt = new HaseTestBoot();
        bt.doMain(args);

    }

    private void doMain(String[] args) {
        String path = args[0];			//文件路径 E:/csvFile
        String tableName = args[1];		//表名 xyd_point1
        String columnfarily = args[2];	//列簇名 xyd_info1
        String siteId = args[3];		//风场id xyd
        String operation = args[4];		//操作 (insert/other)insert
        String devid = args[5];			//风机id  - insert 用不上
        String start = args[6];			//开始时间 - insert 用不上
        String end = args[7];			//结束时间 - insert 用不上
        if (operation.equals("insert")) {
            try {
                System.out.println("========================！");
                HbaseDAO dao = HbaseDaofactory.getFactory().getHDao();//获取表操作对象
                if (!dao.existTable(tableName)) {
                	System.out.println("set 获取 设备id集合");
                    Set<String> devids = collectionInfo(path);//获取设备id - 已改
                    System.out.println("获取设备id的个数"+ devids.size());
                    if(devids == null){
                    	LOG.error("获取设备id集合失败! 无法进行初始化操作，程序将结束");
                    	return;
                    }
                    System.out.println("set init dao");
                    dao.init(tableName, columnfarily);//初始化表操作对象
                    System.out.println("create table");
                    dao.createTable(devids, siteId);//创建表
                    LOG.info("创建表成功！！！");
                    dao.closeConnection();//关闭连接
                } else {
                	System.out.println("表已经存在");
                    dao.init(tableName, columnfarily);//初始化表列簇
                    System.out.println("表初始化结束");
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
/*
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
*/
/*    
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
*/
    private List<MyThread> collectionWorkers(String path, String tableName, String siteId, String columnfarily)
            throws ParseException {
        List<MyThread> workers = new ArrayList<MyThread>();
        File dir = new File(path);
        System.out.println(dir);
        String deviceArea;
    	String[] strs;
        if (dir.isDirectory()) {
        	File[] files = dir.listFiles();
    		for(File areaFile:files){//csvFile
    			if(areaFile.isDirectory()){
	    			deviceArea = areaFile.getName().substring(0, 1);
	    			for(File csvFile:areaFile.listFiles()){//csv
	    				if("1".equals(csvFile.getName())){//文件1是需要导入的风机数据
	    					File[] deviceDataFiles = csvFile.listFiles();
	    					for(File dataFile:deviceDataFiles){
	    						strs = dataFile.getName().split("_");
								LOG.info("-read " + dataFile);
		                        long b = System.currentTimeMillis();
		                        //一个文件一个线程
		                        								//表名		文件路径				            风场id		列簇           风机id=期数+期数编号
		                        HbaseWorker w = new HbaseWorker(tableName, dataFile.getAbsolutePath(), siteId, columnfarily,deviceArea + "_" + strs[0]);
		                        MyThread worker = new MyThread(w);
		                        worker.setName(tableName);
		                        workers.add(worker);
	
		                        long a = System.currentTimeMillis();
		                        LOG.info(dataFile + " 用时" + (a - b) / 1000);
	    					}
	    				}
	    			}
    			}
    		}
        }
        return workers;
    }

    /**
     * 生成风机id
     * 风机id = 第几期+文件名上的编码
     *  例 
     *  1期CSV
     *  	- 1
     *  		- 1_1.csv 风机id = 1_1
	 *	2期CSV
	 *		- 1   
	 *			- 1_1.csv 风机id = 2_1
     * @param path 文件路径
     * @return 返回风机id集合
     */
    private Set<String> collectionInfo(String path) {
    	File rootFile = new File(path);
    	Set<String> set = new HashSet<String>();
    	if(!rootFile.exists()){
    		return null;
    	}
    	String deviceArea;
    	String[] strs;
    	//如果不是规定的文件格式直接返回null
    	try{
	    	//if(rootFile.isDirectory()){
	    		File[] files = rootFile.listFiles();
	    		for(File areaFile:files){//csvFile
	    			if(areaFile.isDirectory()){
		    			deviceArea = areaFile.getName().substring(0, 1);
		    			for(File csvFile:areaFile.listFiles()){//csv
		    				if("1".equals(csvFile.getName())){//文件1是需要导入的风机数据
		    					File[] deviceDataFiles = csvFile.listFiles();
		    					for(File dataFile:deviceDataFiles){
		    						strs = dataFile.getName().split("_");
		    						if("1.csv".equals(strs[1])){
		    							set.add(deviceArea + "_" + strs[0]);
		    						}
		    					}
		    				}
		    			}
	    			}
	    		}
	    		return set;
	    	/*}else{
	    		return null;
	    	}*/
    	}catch(Exception e){
    		
    		return null;
    	}
    }

    /*public void readColumn(String filePath, String column, Set<String> sets) {
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
    }*/

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
