package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MovieReactiveServiceTest {

    private final MovieInfoService movieInfoService = new MovieInfoService();
    private final ReviewService reviewService = new ReviewService();
    private final MovieReactiveService movieReactiveService = new MovieReactiveService(movieInfoService, reviewService);

    @Test
    void getAllMovies() {
        //given
        //when
        var movieFlux = movieReactiveService.getAllMovies();
        //then
        StepVerifier.create(movieFlux)
            .assertNext(movie -> {
                assertEquals( "Batman Begins", movie.getMovie().getName());
                assertEquals(2, movie.getReviewList().size());
            })
            .assertNext(movie -> {
                assertEquals( "The Dark Knight", movie.getMovie().getName());
                assertEquals(2, movie.getReviewList().size());
            })
            .assertNext(movie -> {
                assertEquals( "Dark Knight Rises", movie.getMovie().getName());
                assertEquals(2, movie.getReviewList().size());
            })
            .verifyComplete();
    }

    @Test
    void getMovieById() {
        //given
        long id = 1;
        //when
        var movieMono = movieReactiveService.getMovieById(id);
        //then
        StepVerifier.create(movieMono)
            .assertNext(movie -> {
                assertEquals( "Batman Begins", movie.getMovie().getName());
                assertEquals(2, movie.getReviewList().size());
                assertEquals(1, movie.getMovieId());
            })
            .verifyComplete();
    }
}
