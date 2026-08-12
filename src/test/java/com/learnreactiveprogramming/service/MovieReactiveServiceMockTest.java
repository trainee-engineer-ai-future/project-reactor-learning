package com.learnreactiveprogramming.service;

import com.learnreactiveprogramming.exception.MovieException;
import com.learnreactiveprogramming.exception.NetworkException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class MovieReactiveServiceMockTest {

    @Mock
    private MovieInfoService movieInfoService;
    @Mock
    private ReviewService reviewService;
    @InjectMocks
    private MovieReactiveService movieReactiveService;

    @Test
    void getAllMovies() {
        Mockito.when(movieInfoService.retrievMovieInfoFlux())
            .thenCallRealMethod();
        Mockito.when(reviewService.retrieveReviewsFlux(anyLong()))
            .thenCallRealMethod();
        var movieFlux = movieReactiveService.getAllMovies();
        StepVerifier.create(movieFlux)
            .expectNextCount(3)
            .verifyComplete();
    }

    @Test
    void getAllMovies1() {
        Mockito.when(movieInfoService.retrievMovieInfoFlux())
            .thenCallRealMethod();
        Mockito.when(reviewService.retrieveReviewsFlux(anyLong()))
            .thenThrow(new RuntimeException("Exception occurred in ReviewService"));
        var movieFlux = movieReactiveService.getAllMovies();
        StepVerifier.create(movieFlux)
            .expectError(MovieException.class)
            .verify();
    }

    @Test
    void getAllMoviesRetry() {
        Mockito.when(movieInfoService.retrievMovieInfoFlux())
            .thenCallRealMethod();
        Mockito.when(reviewService.retrieveReviewsFlux(anyLong()))
            .thenThrow(new RuntimeException("Exception occurred in ReviewService"));
        var movieFlux = movieReactiveService.getAllMoviesRetry();
        StepVerifier.create(movieFlux)
            .expectError(MovieException.class)
            .verify();

        verify(reviewService, times(4))
            .retrieveReviewsFlux(isA(Long.class));
    }


    @Test
    void getAllMoviesRetryWhen() {
        Mockito.when(movieInfoService.retrievMovieInfoFlux())
            .thenCallRealMethod();
        Mockito.when(reviewService.retrieveReviewsFlux(anyLong()))
            .thenThrow(new NetworkException("Exception occurred in ReviewService"));
        var movieFlux = movieReactiveService.getAllMoviesRetryWhen();
        StepVerifier.create(movieFlux)
            .expectError(MovieException.class)
            .verify();

        verify(reviewService, times(4))
            .retrieveReviewsFlux(isA(Long.class));
    }

    @Test
    void getAllMoviesRepeat() {
        Mockito.when(movieInfoService.retrievMovieInfoFlux())
            .thenCallRealMethod();
        Mockito.when(reviewService.retrieveReviewsFlux(anyLong()))
            .thenCallRealMethod();
        var movieFlux = movieReactiveService.getAllMoviesRepeat();
        StepVerifier.create(movieFlux)
            .expectNextCount(9)
            .verifyComplete();
    }
}