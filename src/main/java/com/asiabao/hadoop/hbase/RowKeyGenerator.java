package com.asiabao.hadoop.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyGenerator {
	private  final Log LOG = LogFactory.getLog(RowKeyGenerator.class);
	private  SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	public  byte[] generateRowKey(String farm,String fjid,String time) throws ParseException{
		Date times = sf.parse(time);
		String b = farm + "_" + fjid + "_" + times.getTime();
		return Bytes.toBytes(b);
	}
	
	public  String generateRowKeytoString(String farm,String fjid,String time){
        Date times = null;
        try {
            times = sf.parse(time);
        } catch (ParseException e) {
            System.out.println("==============" + time + "==============");
            e.printStackTrace();
        }
        String b = farm + "_" + fjid + "_" + times.getTime();
        return b;
    }
	
	public  byte[] generateRowKey(String farm,String fjid,long time) throws ParseException{
        String b = farm + "_" + fjid + "_" + String.valueOf(time);
        return Bytes.toBytes(b);
    }
	
	public String getRowKey(String farm,String fjid,String time){
        String b = farm + "_" + fjid + "_" + String.valueOf(time);
        return b;
    }
	
	public static RowKeyGenerator getIns(){
	    return new RowKeyGenerator();
	}
	
}
