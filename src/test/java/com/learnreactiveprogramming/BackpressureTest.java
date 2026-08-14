package com.learnreactiveprogramming;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BackpressureTest {

    @Test
    public void testBackpressure() {
        var rangeFlux = Flux.range(1, 100);
        rangeFlux
            .log()
            .subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(3);
                }

                @Override
                protected void hookOnNext(Integer value) {
                    log.info("number is {}", value);
                    if (value == 2) {
                        cancel();
                    }
                }

                @Override
                protected void hookOnComplete() {
                    super.hookOnComplete();
                }

                @Override
                protected void hookOnError(Throwable throwable) {
                    super.hookOnError(throwable);
                }

                @Override
                protected void hookOnCancel() {
                    super.hookOnCancel();
                }
            });
    }

    @Test
    public void testBackpressure2() throws InterruptedException {
        var rangeFlux = Flux.range(1, 100);
        var countDownLatch = new CountDownLatch(1);
        rangeFlux
            .log()
            .subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(3);
                }

                @Override
                protected void hookOnNext(Integer value) {
                    log.info("number is {}", value);
                    if (value % 2 == 0 || value < 50) {
                        request(2);
                    } else {
                        cancel();
                    }
                }

                @Override
                protected void hookOnComplete() {
                    super.hookOnComplete();
                }

                @Override
                protected void hookOnError(Throwable throwable) {
                    super.hookOnError(throwable);
                }

                @Override
                protected void hookOnCancel() {
                    countDownLatch.countDown();
                    super.hookOnCancel();
                }
            });
        Assertions.assertTrue(countDownLatch.await(1L, TimeUnit.SECONDS));
    }

    @Test
    public void testBackpressure3() throws InterruptedException {
        var rangeFlux = Flux.range(1, 100);
        var countDownLatch = new CountDownLatch(1);
        rangeFlux
            .log()
            .onBackpressureDrop(item -> log.info("dropped number is {}", item))
            .subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(3);
                }

                @Override
                protected void hookOnNext(Integer value) {
                    log.info("number is {}", value);
                    if (value % 2 == 0 || value < 50) {
                        request(2);
                    } else {
                        cancel();
                    }

//                    if (value == 3) {
//                        hookOnCancel();
//                    }
                }

                @Override
                protected void hookOnComplete() {
                    super.hookOnComplete();
                }

                @Override
                protected void hookOnError(Throwable throwable) {
                    super.hookOnError(throwable);
                }

                @Override
                protected void hookOnCancel() {
                    countDownLatch.countDown();
                    super.hookOnCancel();
                }
            });
        Assertions.assertTrue(countDownLatch.await(5L, TimeUnit.SECONDS));
    }

    @Test
    public void testBackpressure4() throws InterruptedException {
        var rangeFlux = Flux.range(1, 100);
        var countDownLatch = new CountDownLatch(1);
        rangeFlux
            .log()
            .onBackpressureError()
            .onBackpressureBuffer(10, value -> log.info("buffered number is {}", value))
            .publishOn(Schedulers.parallel())
            .subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(3);
                }

                @Override
                protected void hookOnNext(Integer value) {
                    try {
                        log.info("sleep");
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info("number is {}", value);
                    if (value < 50) {
                        request(1);
                    } else {
                        hookOnCancel();
                    }
                }

                @Override
                protected void hookOnComplete() {
                    super.hookOnComplete();
                }

                @Override
                protected void hookOnError(Throwable throwable) {
                    super.hookOnError(throwable);
                }

                @Override
                protected void hookOnCancel() {
                    countDownLatch.countDown();
                    super.hookOnCancel();
                }
            });
        Assertions.assertTrue(countDownLatch.await(5L, TimeUnit.SECONDS));
    }

}
