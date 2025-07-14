package com.example.databasereader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;

@RestController
@RequestMapping("/api/migrate")
public class MigrationController {

    @Autowired
    private MigrationService migrationService;

    @PostMapping("/{tableName}")
    public String migrateTable(@PathVariable String tableName) {
        try {
            switch (tableName.toLowerCase()) {
                case "area_towns" -> migrationService.migrateAreaTowns();
                case "subd" -> migrationService.migrateDistributors();
                case "customers" -> migrationService.migrateCustomers();
                case "invoice_records" -> migrationService.migrateInvoices();
                case "orders" -> migrationService.migrateOrdersWithHistoryFromPostgres();
                default -> {
                    return " Unsupported table name: " + tableName;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return " Migration failed for table: " + tableName + ". Error: " + e.getMessage();
        }
        return " Migration complete for table: " + tableName;
    }

    @PostMapping("/all")
    public String migrateAll() {
        StringBuilder log = new StringBuilder();
        try {
            migrateTable("area_towns");
            log.append("area_towns done\n");
            migrateTable("subd");
            log.append("distributors (subd) done\n");
            migrateTable("customers");
            log.append("customers done\n");
            migrateTable("invoice_records");
            log.append("invoices done\n");
            migrateTable("orders");
            log.append("orders + histories done\n");
        } catch (Exception e) {
            log.append(" Error during full migration: ").append(e.getMessage());
        }
        return log.toString();
    }
}
