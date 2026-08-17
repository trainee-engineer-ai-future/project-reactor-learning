package com.learnreactiveprogramming.task;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionPool {

    private static final String URL = "jdbc:postgresql://localhost:5432/your_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "password";

    private static final BlockingQueue<Connection> pool = new LinkedBlockingQueue<>();
    private static final List<Connection> realConnections = new ArrayList<>();
    private static volatile boolean isShutdown = false;

    static {
        for (int i = 0; i < 10; i++) {
            try {
                Connection real = DriverManager.getConnection(URL, USER, PASSWORD);
                realConnections.add(real);

                Connection proxy = createProxy(real);
                pool.put(proxy);

            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Закрываем всё при завершении JVM
        Runtime.getRuntime().addShutdownHook(new Thread(ConnectionPool::shutdown));
    }

    private static Connection createProxy(Connection real) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if ("close".equals(method.getName())) {
                        if (isShutdown) {
                            real.close(); // реальное закрытие
                        } else {
                            pool.put(real); // возвращаем в пул
                        }
                        return null;
                    }
                    return method.invoke(real, args);
                }
            }
        );
    }

    public static Connection getConnection() throws InterruptedException {
        if (isShutdown) {
            throw new IllegalStateException("Pool is shutdown");
        }
        return pool.take();
    }

    public static void shutdown() {
        isShutdown = true;

        for (Connection conn : realConnections) {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        pool.clear();
        realConnections.clear();
    }
}