package com.delivery.match_service.service;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DetachedFindSearchRunnerTest {

    private static final UUID COMMAND = UUID.fromString("99999999-9999-9999-9999-999999999999");

    private final MatchCommandStore store = mock(MatchCommandStore.class);
    private final DetachedFindSearchRunner runner = new DetachedFindSearchRunner(store, 60, 10);

    @Test
    void completesAsSoonAsTheLeaseIsTakenWhileTheSearchKeepsRunning() {
        when(store.claimSearch(COMMAND, Duration.ofSeconds(60))).thenReturn(true);
        Sinks.Empty<Void> search = Sinks.empty();
        AtomicBoolean started = new AtomicBoolean();

        runner.run(COMMAND, () -> search.asMono().doOnSubscribe(s -> started.set(true)))
                .block(Duration.ofSeconds(2));

        // The Kafka record is acked here; the search is still in flight.
        assertThat(started).isTrue();
        search.tryEmitEmpty();
    }

    @Test
    void duplicateDeliveryWithALiveLeaseDoesNotStartASecondSearch() {
        when(store.claimSearch(COMMAND, Duration.ofSeconds(60))).thenReturn(false);
        AtomicBoolean started = new AtomicBoolean();

        runner.run(COMMAND, () -> Mono.<Void>empty().doOnSubscribe(s -> started.set(true)))
                .block(Duration.ofSeconds(2));

        assertThat(started).isFalse();
    }

    @Test
    void failedSearchShortensTheLeaseSoRecoveryRetriesSoon() {
        when(store.claimSearch(COMMAND, Duration.ofSeconds(60))).thenReturn(true);

        runner.run(COMMAND, () -> Mono.error(new IllegalStateException("settlement down")))
                .block(Duration.ofSeconds(2));

        verify(store, timeout(2000)).moveSearchLease(COMMAND, Duration.ofSeconds(10));
    }

    @Test
    void eachRetryWaitExtendsTheLeasePastTheWait() {
        runner.beforeRetry(COMMAND, Duration.ofSeconds(75));

        verify(store).moveSearchLease(COMMAND, Duration.ofSeconds(135));
    }

    @Test
    void rejectsLeaseSettingsThatWouldLetRecoveryStealLiveSearches() {
        assertThatThrownBy(() -> new DetachedFindSearchRunner(store, 5, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
