package com.asiabao.hadoop.hbase;

import java.util.ArrayList;
import java.util.List;

public class Row {

    List<Column> cols = new ArrayList<Column>();
    private String rowKey;

    public Row(List<Column> cols, String rowKey) {
        super();
        this.cols = cols;
        this.rowKey = rowKey;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Row(List<Column> cols) {
        super();
        this.cols = cols;
    }

    public List<Column> getCols() {
        return cols;
    }

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

}
