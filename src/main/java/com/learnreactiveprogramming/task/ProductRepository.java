package com.learnreactiveprogramming.task;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.List;

public interface ProductRepository {

    @Nullable
    Product findById(Long id) throws InterruptedException;

    List<Product> findAll() throws InterruptedException;

    List<Product> findByIds(Collection<Long> ids) throws InterruptedException;

    void updateStock(Long productId, int delta) throws InterruptedException;

}
