package com.example.databasereader;

import java.sql.*;

public class DatabaseScanner {

    public static void scanDatabase(String url, String user, String password) {
        String dbType;

        try {
            if (url.startsWith("jdbc:postgresql")) {
                Class.forName("org.postgresql.Driver");
                dbType = "PostgreSQL";
            } else if (url.startsWith("jdbc:mysql")) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                dbType = "MySQL";
            } else {
                System.err.println(" Unsupported JDBC URL.");
                return;
            }
        } catch (ClassNotFoundException e) {
            System.err.println(" Driver not found: " + e.getMessage());
            return;
        }

        System.out.println("\n==============================");
        System.out.println(dbType + " Database");
        System.out.println("==============================");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet tables;
            if (dbType.equals("PostgreSQL")) {
                tables = meta.getTables(null, "public", "%", new String[]{"TABLE"});
            } else {
                String catalog = getCatalogFromUrl(url);
                tables = meta.getTables(catalog, null, "%", new String[]{"TABLE"});
            }

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                System.out.println("\n Table: " + tableName);

                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 10")) {

                    ResultSetMetaData rsMeta = rs.getMetaData();
                    int columnCount = rsMeta.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rsMeta.getColumnName(i) + "\t");
                    }
                    System.out.println();

                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(rs.getString(i) + "\t");
                        }
                        System.out.println();
                    }
                } catch (SQLException e) {
                    System.err.println("SQL error while reading table '" + tableName + "': " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            System.err.println(" SQL error: " + e.getMessage());
        }
    }

    private static String getCatalogFromUrl(String url) {
        String prefix = "jdbc:mysql://";
        if (!url.startsWith(prefix)) return null;

        String afterPrefix = url.substring(prefix.length());
        int slashIndex = afterPrefix.indexOf('/');
        if (slashIndex == -1) return null;

        String afterHostPort = afterPrefix.substring(slashIndex + 1);
        int questionMarkIndex = afterHostPort.indexOf('?');
        if (questionMarkIndex != -1) {
            return afterHostPort.substring(0, questionMarkIndex);
        } else {
            return afterHostPort;
        }
    }
}