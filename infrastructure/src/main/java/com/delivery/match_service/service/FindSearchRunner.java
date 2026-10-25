package com.delivery.match_service.service;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Decides where a single-shipper search runs relative to the Kafka record that
 * started it. A search can retry for minutes; holding the record that long
 * pauses the whole partition (async acks), so production runs it detached.
 */
public interface FindSearchRunner {

    Mono<Void> run(UUID commandId, Supplier<Mono<Void>> search);

    /** Called before each retry wait so a live search keeps its lease. */
    default void beforeRetry(UUID commandId, Duration nextDelay) {
    }

    /** Runs the search on the caller's subscription (focused tests). */
    static FindSearchRunner inline() {
        return (commandId, search) -> search.get();
    }
}
