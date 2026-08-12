package com.learnreactiveprogramming.service;

import com.learnreactiveprogramming.domain.Movie;
import com.learnreactiveprogramming.exception.MovieException;
import com.learnreactiveprogramming.exception.NetworkException;
import com.learnreactiveprogramming.exception.ServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@RequiredArgsConstructor
@Slf4j
public class MovieReactiveService {

    private final MovieInfoService movieInfoService;
    private final ReviewService reviewService;

    public Flux<Movie> getAllMovies() {
        var movieInfoFlux = movieInfoService.retrievMovieInfoFlux();
        return movieInfoFlux
            .flatMap(movieInfo -> {
                var reviewsMono = reviewService.retrieveReviewsFlux(movieInfo.getMovieInfoId())
                    .collectList();
                return reviewsMono
                    .map(reviews -> new Movie(movieInfo, reviews));
            })
            .onErrorMap(ex -> {
                log.error("Error retrieving movies", ex);
                throw new MovieException(ex.getMessage());
            });
    }

    public Flux<Movie> getAllMoviesRetry() {
        var movieInfoFlux = movieInfoService.retrievMovieInfoFlux();
        return movieInfoFlux
            .flatMap(movieInfo -> {
                var reviewsMono = reviewService.retrieveReviewsFlux(movieInfo.getMovieInfoId())
                    .collectList();
                return reviewsMono
                    .map(reviews -> new Movie(movieInfo, reviews));
            })
            .onErrorMap(ex -> {
                log.error("Error retrieving movies", ex);
                throw new MovieException(ex.getMessage());
            })
            .retry(3)
            .log();
    }

    public Flux<Movie> getAllMoviesRetryWhen() {
        var movieInfoFlux = movieInfoService.retrievMovieInfoFlux();
        return movieInfoFlux
            .flatMap(movieInfo -> {
                var reviewsMono = reviewService.retrieveReviewsFlux(movieInfo.getMovieInfoId())
                    .collectList();
                return reviewsMono
                    .map(reviews -> new Movie(movieInfo, reviews));
            })
            .onErrorMap(ex -> {
                log.error("Error retrieving movies", ex);
                if (ex instanceof NetworkException) {
                    throw new MovieException(ex.getMessage());
                }
                throw new ServiceException(ex.getMessage());
            })
            .retryWhen(Retry.backoff(3, Duration.of(500, ChronoUnit.MILLIS))
                .filter(ex -> ex instanceof MovieException)
                .onRetryExhaustedThrow((spec, signal) -> Exceptions.propagate(signal.failure())))
            .log();
    }

    public Flux<Movie> getAllMoviesRepeat() {
        var movieInfoFlux = movieInfoService.retrievMovieInfoFlux();
        return movieInfoFlux
            .flatMap(movieInfo -> {
                var reviewsMono = reviewService.retrieveReviewsFlux(movieInfo.getMovieInfoId())
                    .collectList();
                return reviewsMono
                    .map(reviews -> new Movie(movieInfo, reviews));
            })
            .onErrorMap(ex -> {
                log.error("Error retrieving movies", ex);
                if (ex instanceof NetworkException) {
                    throw new MovieException(ex.getMessage());
                }
                throw new ServiceException(ex.getMessage());
            })
            .retryWhen(Retry.backoff(3, Duration.of(500, ChronoUnit.MILLIS))
                .filter(ex -> ex instanceof MovieException)
                .onRetryExhaustedThrow((spec, signal) -> Exceptions.propagate(signal.failure())))
            .repeat(2)
            .log();
    }

    public Mono<Movie> getMovieById(Long movieId) {
        return movieInfoService.retrieveMovieInfoMonoUsingId(movieId)
            .flatMap(movieInfo ->
                reviewService.retrieveReviewsFlux(movieId).collectList()
                    .map(reviews -> new Movie(movieInfo, reviews)));
    }

}
