package com.learnreactiveprogramming.task;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OrderReactiveServiceTest {

    private final ProductRepository productRepository = new ProductRepositoryImpl();
    private final OrderReactiveService orderReactiveService = new OrderReactiveService(productRepository);

    @Test
    void findById() throws InterruptedException {

        Mono<Product> byId = orderReactiveService.findById(1L);
        StepVerifier.create(byId)
            .expectNextCount(1)
            .verifyComplete();

    }

    @Test
    void findAll() throws InterruptedException {
        Flux<Product> byId = orderReactiveService.findAll();
        StepVerifier.create(byId)
            .expectNextCount(5)
            .verifyComplete();
    }

    @Test
    void findAllMono() throws InterruptedException {
        Mono<List<Product>> byId = orderReactiveService.findAllMono();
        StepVerifier.create(byId)
            .assertNext(products -> {
                assertEquals(5, products.size());
            })
            .verifyComplete();
    }

    @Test
    void findByIds() throws InterruptedException {
        Flux<Product> byId = orderReactiveService.findByIds(List.of(1L, 2L));
        StepVerifier.create(byId)
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    void updateStock() throws InterruptedException {
        Mono<Void> voidMono = orderReactiveService.updateStock(1L, 1);
        StepVerifier.create(voidMono)
            .verifyComplete();
    }
}