package com.dfjx.diy;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import java.util.HashMap;
import java.util.Map;

/**
 * @author wayne
 * @date 2020.09.16
 */
public class DataSync {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://10.4.0.47:5432/tiwisdom?currentSchema=file&binaryTransfer=false",
                    "tbaseadmin", "sawXG841rx");
            System.out.println(Thread.currentThread().getName() + " : " + "mpp获取连接成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        PreparedStatement pstmt;
        ResultSet rs;

        try {
            pstmt = conn.prepareStatement("select dq from file.sourcefileSJ1_Sheet120201203110826 group by dq");
            rs = pstmt.executeQuery();

            List<Map> list = new ArrayList<>();
            ResultSetMetaData metaData = rs.getMetaData();
            int colLen = metaData.getColumnCount();
            while (rs.next()) {
                Map map = new HashMap();
                for (int i = 0; i < colLen; i++) {
                    String colName = metaData.getColumnName(i + 1);
                    Object colValue = rs.getObject(colName);
                    if (null == colValue) {
                        colValue = "";
                    }
                    map.put(colName, colValue);
                }
                list.add(map);
            }
            for (Map map : list) {
                System.out.println(map.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
