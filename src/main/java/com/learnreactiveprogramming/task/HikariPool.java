package com.learnreactiveprogramming.task;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariPool {
    private static final HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/your_db");
        config.setUsername("postgres");
        config.setPassword("password");

        // Рекомендуемые настройки
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);      // 30 сек
        config.setIdleTimeout(600000);           // 10 мин
        config.setMaxLifetime(1800000);        // 30 мин

        dataSource = new HikariDataSource(config);
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = getConnection()) {
            // работаем с соединением
            System.out.println("Соединение получено: " + !conn.isClosed());
        }

        // при завершении приложения
        shutdown();
    }

}
