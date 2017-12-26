package com.asiabao.csv.asiabaocsv;

import java.io.IOException;

import com.csvreader.CsvReader;

public class MyCsvReader {

    public static void read() {

        String filePath = "XXX.csv";

        try {
            // 创建CSV读对象
            CsvReader csvReader = new CsvReader(filePath);
            // 读表头
            csvReader.readHeaders();
            while (csvReader.readRecord()) {
                // 读一整行
                System.out.println(csvReader.getRawRecord());
                // 读这行的某一列
                System.out.println(csvReader.get("Link"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
