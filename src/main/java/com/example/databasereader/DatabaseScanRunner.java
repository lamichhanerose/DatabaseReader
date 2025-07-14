package com.example.databasereader;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatabaseScanRunner {

    @Autowired
    private DbProperties dbProperties;

    @PostConstruct
    public void runScanner() {
        // Scan Source DB
        DatabaseScanner.scanDatabase(
                dbProperties.getSource().getUrl(),
                dbProperties.getSource().getUsername(),
                dbProperties.getSource().getPassword()
        );

        // Scan PostgreSQL DB
        DatabaseScanner.scanDatabase(
                dbProperties.getPostgres().getUrl(),
                dbProperties.getPostgres().getUsername(),
                dbProperties.getPostgres().getPassword()
        );

        // Scan MySQL DB
        DatabaseScanner.scanDatabase(
                dbProperties.getMysql().getUrl(),
                dbProperties.getMysql().getUsername(),
                dbProperties.getMysql().getPassword()
        );
    }
}
