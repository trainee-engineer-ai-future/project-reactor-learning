package com.learnreactiveprogramming.service;

import com.learnreactiveprogramming.domain.Movie;
import com.learnreactiveprogramming.domain.MovieInfo;
import com.learnreactiveprogramming.domain.Review;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RequiredArgsConstructor
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
            });
    }

    public Mono<Movie> getMovieById(Long movieId) {
        return movieInfoService.retrieveMovieInfoMonoUsingId(movieId)
            .map(movieInfo -> {
                var reviews = reviewService.retrieveReviewsFlux(movieId).collectList().block();
                return new Movie(movieInfo, reviews);
            });
    }

}
