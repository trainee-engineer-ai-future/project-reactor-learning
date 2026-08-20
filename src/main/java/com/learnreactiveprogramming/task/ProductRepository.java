package com.learnreactiveprogramming.task;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.List;

public interface ProductRepository {

    @Nullable
    Product findById(Long id) throws InterruptedException;
    // throws ProductNotFoundException, если нет

    List<Product> findAll() throws InterruptedException;

    List<Product> findByIds(Collection<Long> ids) throws InterruptedException;
    // удобно для createOrder (загрузка нескольких product)

    void updateStock(Long productId, int delta) throws InterruptedException;

}
