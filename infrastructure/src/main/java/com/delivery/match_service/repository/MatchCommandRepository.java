package com.delivery.match_service.repository;

import com.delivery.match_service.entity.MatchCommand;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.LockModeType;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface MatchCommandRepository extends JpaRepository<MatchCommand, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select command from MatchCommand command where command.eventId = :eventId")
    Optional<MatchCommand> findByEventIdForUpdate(@Param("eventId") UUID eventId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select command
            from MatchCommand command
            where command.deliveryId = :deliveryId
              and command.matchingSessionId = :matchingSessionId
            """)
    Optional<MatchCommand> findByDeliveryAndSessionForUpdate(
            @Param("deliveryId") Long deliveryId,
            @Param("matchingSessionId") UUID matchingSessionId);

    /**
     * Takes the single-search lease. next_attempt_at doubles as the lease end:
     * a command with a live lease is already being searched by some replica.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update MatchCommand command
            set command.nextAttemptAt = :leaseUntil, command.updatedAt = :now
            where command.eventId = :eventId
              and command.status in (com.delivery.match_service.entity.MatchCommand.Status.PENDING,
                                     com.delivery.match_service.entity.MatchCommand.Status.CANDIDATE_STAGED)
              and (command.nextAttemptAt is null or command.nextAttemptAt < :now)
            """)
    int claimSearch(@Param("eventId") UUID eventId, @Param("leaseUntil") LocalDateTime leaseUntil,
                    @Param("now") LocalDateTime now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update MatchCommand command
            set command.nextAttemptAt = :leaseUntil, command.updatedAt = :now
            where command.eventId = :eventId
              and command.status in (com.delivery.match_service.entity.MatchCommand.Status.PENDING,
                                     com.delivery.match_service.entity.MatchCommand.Status.CANDIDATE_STAGED)
            """)
    int moveSearchLease(@Param("eventId") UUID eventId, @Param("leaseUntil") LocalDateTime leaseUntil,
                        @Param("now") LocalDateTime now);

    /** Single searches whose lease ran out without a result: the searching process died. */
    @Query("""
            select command
            from MatchCommand command
            where command.status in (com.delivery.match_service.entity.MatchCommand.Status.PENDING,
                                     com.delivery.match_service.entity.MatchCommand.Status.CANDIDATE_STAGED)
              and command.nextAttemptAt is not null
              and command.nextAttemptAt < :now
              and command.createdAt > :oldest
            order by command.nextAttemptAt
            """)
    List<MatchCommand> findAbandonedSearches(@Param("now") LocalDateTime now,
                                             @Param("oldest") LocalDateTime oldest,
                                             org.springframework.data.domain.Pageable page);
}
