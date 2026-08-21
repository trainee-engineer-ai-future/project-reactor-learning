package com.learnreactiveprogramming.task;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.List;

@RequiredArgsConstructor
public class OrderReactiveService {

    private final ProductRepository productRepository;
    private Sinks.Many<Product> sink = Sinks.many().multicast().onBackpressureBuffer();

    @Nullable
    public Mono<Product> findById(Long id) throws InterruptedException {
        return Mono.fromCallable(() -> productRepository.findById(id))
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

    public Flux<Product> stockChanges() {
        return sink.asFlux();
    }

    public Flux<Product> findAll() throws InterruptedException {
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

    public Mono<List<Product>> findAllMono() throws InterruptedException {
        return Mono.fromCallable(productRepository::findAll)
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

    public Flux<Product> findByIds(Collection<Long> ids) throws InterruptedException {
        return Flux.<Product>create(objectFluxSink -> {
                try {
                    List<Product> all = productRepository.findByIds(ids);
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

    public Mono<Void> updateStock(Long productId, int delta) throws InterruptedException {
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

    public Mono<Product> reserveStock(Long id, int delta) throws InterruptedException {
        return Mono.<Product>fromCallable(() -> {
                try {
                    return productRepository.findById(id);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            })
            .flatMap(product -> {
                if (product.getStock() < delta) {
                    return Mono.error(new RuntimeException("stock exceeded"));
                }
                return Mono.fromCallable(() -> {
                        productRepository.updateStock(id, -delta);
                        product.setStock(product.getStock() - delta);
                        sink.tryEmitNext(product);
                        return product;
                    })
                    .subscribeOn(Schedulers.boundedElastic());
            })
            .log()
            .subscribeOn(Schedulers.boundedElastic());
    }

    public Flux<Product> reserveStockBatch(List<ReserveRequest> items) {
        return Flux.fromIterable(items)
            .concatMap(item -> {
                try {
                    return reserveStock(item.getProductId(), item.getQuantity());
                } catch (InterruptedException e) {
                    return Flux.error(new RuntimeException(e));
                }
            })
            .subscribeOn(Schedulers.boundedElastic());
    }

}
