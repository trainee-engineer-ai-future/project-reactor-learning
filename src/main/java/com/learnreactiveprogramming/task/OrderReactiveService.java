package com.learnreactiveprogramming.task;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.List;

@RequiredArgsConstructor
public class OrderReactiveService {

    private final ProductRepository productRepository;

    @Nullable
    Mono<Product> findById(Long id) throws InterruptedException {
        return Mono.fromCallable(() -> productRepository.findById(id))
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }


    Flux<Product> findAll() throws InterruptedException {
        return Flux.<Product>create(objectFluxSink -> {
                try {
                    List<Product> all = productRepository.findAll();
                    int[] consumed = new int[]{0};
                    objectFluxSink.onRequest(number -> {
                        var counter = 0;
                        while (consumed[0] + counter < all.size() && counter < number) {
                            objectFluxSink.next(all.get(consumed[0] + counter));
                            ++counter;
                        }
                        consumed[0] += counter;
                        if (consumed[0] == all.size()) {
                            objectFluxSink.complete();
                        }
                    });
                } catch (InterruptedException e) {
                    objectFluxSink.error(e);
                }
            })
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

    Mono<List<Product>> findAllMono() throws InterruptedException {
        return Mono.fromCallable(productRepository::findAll)
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

    Flux<Product> findByIds(Collection<Long> ids) throws InterruptedException {
        return Flux.<Product>create(objectFluxSink -> {
                try {
                    List<Product> all = productRepository.findByIds(ids);
                    int[] consumed = new int[]{0};
                    objectFluxSink.onRequest(number -> {
                        var counter = 0;
                        while (consumed[0] + counter < all.size() && counter < number) {
                            objectFluxSink.next(all.get(counter));
                            ++counter;
                        }
                        consumed[0] += counter;
                        if (consumed[0] == all.size()) {
                            objectFluxSink.complete();
                        }
                    });
                } catch (InterruptedException e) {
                    objectFluxSink.error(e);
                }
            })
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

    Mono<Void> updateStock(Long productId, int delta) throws InterruptedException {
        return Mono.<Void>fromRunnable(() -> {
                try {
                    productRepository.updateStock(productId, delta);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            })
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

}
