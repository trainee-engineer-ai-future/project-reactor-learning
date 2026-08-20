package com.learnreactiveprogramming.task;

import org.jspecify.annotations.Nullable;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ProductRepositoryImpl implements ProductRepository {

    @Override
    @Nullable
    public Product findById(Long id) throws InterruptedException {
        Connection connection = ConnectionPool.getConnection();
        Product product = new Product();
        try (connection) {
            connection.setAutoCommit(false);
            String sql = "select * from products where id = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setLong(1, id);
                ResultSet resultSet = preparedStatement.executeQuery();
                connection.commit();
                try (resultSet) {
                    if (!resultSet.next()) {
                        return  null;
                    }
                    product.setId(resultSet.getLong("id"));
                    product.setName(resultSet.getString("name"));
                    product.setPrice(resultSet.getBigDecimal("price"));
                    product.setStock(resultSet.getInt("stock"));
                }
            }
            return product;
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);   // не теряем исходное исключение
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Product> findAll() throws InterruptedException {
        Connection connection = ConnectionPool.getConnection();
        List<Product> products = new ArrayList<>();
        try (connection) {
            connection.setAutoCommit(false);
            String sql = "select * from products";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                ResultSet resultSet = preparedStatement.executeQuery();
                connection.commit();
                try (resultSet) {
                    while (resultSet.next())
                        products.add(
                            Product.builder()
                                .id(resultSet.getLong("id"))
                                .name(resultSet.getString("name"))
                                .price(resultSet.getBigDecimal("price"))
                                .stock(resultSet.getInt("stock"))
                                .build());
                }
            }
            return products;
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);   // не теряем исходное исключение
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Product> findByIds(Collection<Long> ids) throws InterruptedException {
        Connection connection = ConnectionPool.getConnection();
        List<Product> products = new ArrayList<>();
        try (connection) {
            connection.setAutoCommit(false);
            String sql = "select * from products where  id = ANY(?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                Array sqlArray = connection.createArrayOf("BIGINT", ids.toArray());
                preparedStatement.setArray(1, sqlArray);
                ResultSet resultSet = preparedStatement.executeQuery();
                connection.commit();
                try (resultSet) {
                    while (resultSet.next())
                        products.add(
                            Product.builder()
                                .id(resultSet.getLong("id"))
                                .name(resultSet.getString("name"))
                                .price(resultSet.getBigDecimal("price"))
                                .stock(resultSet.getInt("stock"))
                                .build());
                }
            }
            return products;
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);   // не теряем исходное исключение
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateStock(Long productId, int delta) throws InterruptedException {
        Connection connection = ConnectionPool.getConnection();
        try (connection) {
            connection.setAutoCommit(false);
            String sql = "update products set stock = stock + ? where id = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setInt(1, delta);
                preparedStatement.setLong(2, productId);
                preparedStatement.executeUpdate();
                connection.commit();
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }
            throw new RuntimeException(e);
        }

    }
}
