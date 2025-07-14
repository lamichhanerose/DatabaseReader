package com.example.databasereader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class MigrationService {

    @Autowired
    private DbProperties dbProperties;

    private Connection getPostgresConn() throws SQLException {
        return DriverManager.getConnection(
                dbProperties.getPostgres().getUrl(),
                dbProperties.getPostgres().getUsername(),
                dbProperties.getPostgres().getPassword()
        );
    }

    private Connection getSourceConn() throws SQLException {
        return DriverManager.getConnection(
                dbProperties.getSource().getUrl(),
                dbProperties.getSource().getUsername(),
                dbProperties.getSource().getPassword()
        );
    }

    public void migrateAreaTowns() throws SQLException {
        try (Connection src = getPostgresConn(); Connection dest = getSourceConn()) {
            Statement cleaner = dest.createStatement();
            cleaner.executeUpdate("DELETE FROM towns");

            ResultSet rs = src.createStatement().executeQuery("SELECT * FROM area_towns");

            PreparedStatement ps = dest.prepareStatement(
                    "INSERT INTO towns (id, name, active) VALUES (?, ?, ?)");

            while (rs.next()) {
                ps.setInt(1, rs.getInt("id"));
                ps.setString(2, rs.getString("name"));
                ps.setBoolean(3, rs.getBoolean("active"));
                ps.executeUpdate();
            }
        }
    }

    public void migrateDistributors() throws SQLException {
        try (Connection src = getPostgresConn(); Connection dest = getSourceConn()) {
            Statement cleaner = dest.createStatement();
            cleaner.executeUpdate("DELETE FROM distributors");

            ResultSet rs = src.createStatement().executeQuery("SELECT * FROM subd");

            PreparedStatement ps = dest.prepareStatement("""
                INSERT INTO distributors (id, name, town_id, active, invoice, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """);

            while (rs.next()) {
                ps.setInt(1, rs.getInt("id"));
                ps.setString(2, rs.getString("name"));
                ps.setInt(3, rs.getInt("town_id"));
                ps.setBoolean(4, rs.getBoolean("active"));
                ps.setString(5, rs.getString("invoice"));
                ps.setTimestamp(6, rs.getTimestamp("created_at"));
                ps.setTimestamp(7, rs.getTimestamp("updated_at"));
                ps.executeUpdate();
            }
        }
    }

    public void migrateCustomers() throws SQLException {
        try (Connection src = getPostgresConn(); Connection dest = getSourceConn()) {
            Statement cleaner = dest.createStatement();
            cleaner.executeUpdate("DELETE FROM customers");

            ResultSet rs = src.createStatement().executeQuery("SELECT * FROM customers");

            PreparedStatement ps = dest.prepareStatement("""
                INSERT INTO customers (
                    id, name, phone_number, active, pan_number,
                    created_at, updated_at, created_by, updated_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """);

            while (rs.next()) {
                ps.setInt(1, rs.getInt("id"));
                ps.setString(2, rs.getString("name"));
                ps.setString(3, rs.getString("phone_number"));
                ps.setBoolean(4, rs.getBoolean("active"));
                ps.setString(5, rs.getString("pan_number"));
                ps.setTimestamp(6, rs.getTimestamp("created_at"));
                ps.setTimestamp(7, rs.getTimestamp("updated_at"));
                ps.setString(8, rs.getString("created_by"));
                ps.setString(9, rs.getString("updated_by"));
                ps.executeUpdate();
            }
        }
    }

    public void migrateInvoices() throws SQLException {
        try (Connection src = getPostgresConn(); Connection dest = getSourceConn()) {
            Statement cleaner = dest.createStatement();
            cleaner.executeUpdate("DELETE FROM invoices");

            ResultSet rs = src.createStatement().executeQuery("SELECT * FROM invoice_records");

            PreparedStatement ps = dest.prepareStatement("""
                INSERT INTO invoices (
                    id, invoice_number, amount_details, remarks, payment_mode, date,
                    customer_id, created_at, updated_at, created_by, updated_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """);

            while (rs.next()) {
                ps.setInt(1, rs.getInt("id"));
                ps.setString(2, rs.getString("invoice_number"));
                ps.setInt(3, rs.getInt("amount_details"));
                ps.setString(4, rs.getString("remarks"));
                ps.setString(5, rs.getString("payment_mode"));
                ps.setDate(6, rs.getDate("date"));
                ps.setInt(7, rs.getInt("customer_id"));
                ps.setTimestamp(8, rs.getTimestamp("created_at"));
                ps.setTimestamp(9, rs.getTimestamp("updated_at"));
                ps.setString(10, rs.getString("created_by"));
                ps.setString(11, rs.getString("updated_by"));
                ps.executeUpdate();
            }
        }
    }

    public void migrateOrdersWithHistoryFromPostgres() throws SQLException {
        try (
                Connection sourceConn = getPostgresConn();
                Connection targetConn = getSourceConn()
        ) {
            try (Statement cleaner = targetConn.createStatement()) {
                cleaner.executeUpdate("DELETE FROM order_histories");
                cleaner.executeUpdate("DELETE FROM orders");
            }

            Statement readStmt = sourceConn.createStatement();
            ResultSet rs = readStmt.executeQuery("SELECT * FROM orders");

            String insertOrderSQL = """
                INSERT INTO orders (
                     order_date, state, quantity, invoice_id, distributor_id,
                    amount_details, cancellation_reason, created_by, updated_by,
                    pg_order_ref_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

            String insertHistorySQL = """
                INSERT INTO order_histories (
                    order_id, status, created_by
                ) VALUES (?, ?, ?)
            """;

            PreparedStatement orderStmt = targetConn.prepareStatement(insertOrderSQL);
            PreparedStatement historyStmt = targetConn.prepareStatement(insertHistorySQL);

            Set<Integer> insertedOrders = new HashSet<>();

            while (rs.next()) {
                int sourceOrderId = rs.getInt("source_order_id");

                if (!insertedOrders.contains(sourceOrderId)) {
                    orderStmt.setDate(1, rs.getDate("order_date"));
                    orderStmt.setString(2, rs.getString("state"));
                    orderStmt.setInt(3, rs.getInt("quantity"));
                    orderStmt.setInt(4, rs.getInt("invoice_id"));
                    orderStmt.setInt(5, rs.getInt("distributor_id"));
                    orderStmt.setObject(6, rs.getObject("amount_details"));
                    orderStmt.setString(7, rs.getString("cancel_note"));
                    orderStmt.setString(8, rs.getString("created_by"));
                    orderStmt.setString(9, rs.getString("updated_by"));
                    orderStmt.setInt(10, sourceOrderId);

                    orderStmt.executeUpdate();
                    insertedOrders.add(sourceOrderId);
                }

                historyStmt.setInt(1, sourceOrderId);
                historyStmt.setString(2, rs.getString("final_status"));
                historyStmt.setString(3, rs.getString("history_created_by"));
                historyStmt.executeUpdate();
            }

            orderStmt.close();
            historyStmt.close();
            rs.close();
            readStmt.close();
        }
    }





}
