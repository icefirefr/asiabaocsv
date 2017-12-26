package com.asiabao.hadoop.hbase;


public class Column {
    private String key;
    private String values;
    
    public Column(String key, String values) {
        super();
        this.key = key;
        this.values = values;
    }

    public String getKey() {
        return key;
    }
    
    public void setKey(String key) {
        this.key = key;
    }
    
    public String getValues() {
        return values;
    }
    
    public void setValues(String values) {
        this.values = values;
    }
    
    
}
