package com.delivery.match_service.service;

import com.delivery.match_service.entity.MatchCommand;
import com.delivery.match_service.listener.FindShipperEventListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Replays single searches whose lease ran out, e.g. after a pod was killed mid-search. */
@Slf4j
@Component
public class FindSearchRecovery {

    private final MatchCommandStore commandStore;
    private final FindShipperEventListener listener;
    private final int batchSize;
    private final Duration maxAge;

    public FindSearchRecovery(MatchCommandStore commandStore, FindShipperEventListener listener,
                              @Value("${matching.search-lease.recovery-batch-size:50}") int batchSize,
                              @Value("${matching.search-lease.recovery-max-age-minutes:60}") long maxAgeMinutes) {
        this.commandStore = commandStore;
        this.listener = listener;
        this.batchSize = batchSize;
        this.maxAge = Duration.ofMinutes(maxAgeMinutes);
    }

    @Scheduled(fixedDelayString = "${matching.search-lease.recovery-delay-ms:15000}")
    public void recoverAbandonedSearches() {
        for (MatchCommand command : commandStore.abandonedSearches(batchSize, maxAge)) {
            try {
                log.info("Recovering abandoned search for delivery {} (command {})",
                        command.getDeliveryId(), command.getEventId());
                // Exact replay of the stored payload: the inbox resumes, skips or
                // processes it, and the lease claim stops a double search.
                listener.handleFindShipperEvent(command.getPayload(), command.getTopic(), -1, null).block();
            } catch (RuntimeException error) {
                log.warn("Recovery of find command {} failed: {}", command.getEventId(), error.toString());
            }
        }
    }
}
