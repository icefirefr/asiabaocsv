package com.asiabao.hadoop.hbase;

import java.util.List;
import java.util.Map;

public class PageResult {
    private List<Map<String, String>> ls;
    private String startRow;
    public PageResult(List<Map<String, String>> ls, String startRow) {
        super();
        this.ls = ls;
        this.startRow = startRow;
    }
    
    public List<Map<String, String>> getLs() {
        return ls;
    }
    
    public void setLs(List<Map<String, String>> ls) {
        this.ls = ls;
    }
    
    public String getStartRow() {
        return startRow;
    }
    
    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }
    
    
    
}
