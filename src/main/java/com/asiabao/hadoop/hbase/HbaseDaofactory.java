package com.asiabao.hadoop.hbase;

public class HbaseDaofactory {
	private static HbaseDaofactory factory = null;
	
	public synchronized static HbaseDaofactory getFactory(){
		if(factory == null){
			factory = new HbaseDaofactory();
		}
		return factory;
	}
	
	public HbaseDAO getHDao() throws Exception{
		return new HbaseDAO();
	}
}
