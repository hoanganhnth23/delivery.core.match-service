package com.delivery.match_service.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Acks the find command as soon as it is durable and leased, then searches off
 * the consumer. The lease (match_commands.next_attempt_at) is what makes that
 * safe: if this process dies mid-search the lease runs out and
 * {@link FindSearchRecovery} replays the stored command.
 */
@Slf4j
@Component
public class DetachedFindSearchRunner implements FindSearchRunner {

    private final MatchCommandStore commandStore;
    private final Duration leaseGrace;
    private final Duration failureBackoff;

    public DetachedFindSearchRunner(
            MatchCommandStore commandStore,
            @Value("${matching.search-lease.grace-seconds:60}") long leaseGraceSeconds,
            @Value("${matching.search-lease.failure-backoff-seconds:10}") long failureBackoffSeconds) {
        if (leaseGraceSeconds < 10 || failureBackoffSeconds < 1) {
            throw new IllegalArgumentException("search lease grace must be >= 10 s and backoff >= 1 s");
        }
        this.commandStore = commandStore;
        this.leaseGrace = Duration.ofSeconds(leaseGraceSeconds);
        this.failureBackoff = Duration.ofSeconds(failureBackoffSeconds);
    }

    @Override
    public Mono<Void> run(UUID commandId, Supplier<Mono<Void>> search) {
        return Mono.fromCallable(() -> commandStore.claimSearch(commandId, leaseGrace))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(claimed -> {
                    if (!claimed) {
                        log.info("Find command {} is already being searched; acking the duplicate", commandId);
                        return;
                    }
                    Mono.defer(search)
                            .onErrorResume(error -> {
                                // Infrastructure failure: give the command back to recovery soon,
                                // bounded by Dispatch's matching deadline.
                                log.warn("Search for find command {} failed; recovery retries in {}: {}",
                                        commandId, failureBackoff, error.toString());
                                return Mono.fromRunnable(() -> commandStore.moveSearchLease(commandId, failureBackoff))
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .then();
                            })
                            .subscribe(null, error -> log.error("Could not release lease for {}", commandId, error));
                })
                .then();
    }

    @Override
    public void beforeRetry(UUID commandId, Duration nextDelay) {
        commandStore.moveSearchLease(commandId, nextDelay.plus(leaseGrace));
    }
}
