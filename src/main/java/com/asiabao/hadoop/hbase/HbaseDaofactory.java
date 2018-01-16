package com.asiabao.hadoop.hbase;

public class HbaseDaofactory {
	private static HbaseDaofactory factory = null;
	private static HbaseDAO hbaseDao;
	
	public synchronized static HbaseDaofactory getFactory(){
		if(factory == null){
			factory = new HbaseDaofactory();
		}
		return factory;
	}
	
	public synchronized HbaseDAO getHDao() throws Exception{
		if(hbaseDao == null){
			System.out.println("==========================================");
			System.out.println("==========================================");
			System.out.println("get hbase dao");
			hbaseDao = new HbaseDAO();
		}
		return hbaseDao;
	}
}
