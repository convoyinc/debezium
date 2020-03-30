/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.replication.LogSequenceNumber;

public class PostgresPendingLsnStore {
    private static final String LSN_PROCESSED = "LSN_PROCESSED";

    private final ConcurrentHashMap<Long, Integer> lsnsInProgress = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> lsnsProcessed = new ConcurrentHashMap<>();

    public void recordPolledLsn(SourceRecord record) {
        Long lsn = getLsnFromRecord(record);
        if (lsn == null) {
            return;
        }

        lsnsInProgress.compute(lsn, (_lsn, count) -> {
            if (count == null) {
                return 1;
            } else {
                return count + 1;
            }
        });
    }

    public void recordPolledLsns(Collection<SourceRecord> records) {
        records.forEach(this::recordPolledLsn);
    }

    public void recordProcessedLsn(SourceRecord record) throws IllegalStateException {
        Long lsn = getLsnFromRecord(record);
        if (lsn == null) {
            return;
        }

        AtomicBoolean isFullyProcessed = new AtomicBoolean(false);
        lsnsInProgress.compute(lsn, (_lsn, count) -> {
            if (count == null || count < 0) {
                throw new IllegalStateException("Attempted to record processed LSN when it hasn't been polled");
            } else if (count == 1) {
                isFullyProcessed.set(true);
                return null;
            } else {
                return count - 1;
            }
        });

        if (isFullyProcessed.get()) {
            lsnsProcessed.put(lsn, LSN_PROCESSED);
        }
    }

    /**
     * Get the largest LSN from an event comitted to Kafka so far.
     * @return the Long lsn value, or null if there are no events that have been processed yet.
     */
    public Long getLargestProcessedLsn() {
        Long earliestUnprocessedLsn = getEarliestUnprocessedLsn();
        Long largestProcessedLsn = getLargestProcessedLsnLessThan(earliestUnprocessedLsn);
        removeProcessedLsnsLessThan(largestProcessedLsn);
        return largestProcessedLsn;
    }

    /**
     * Get the earliest polled LSN from an event that has yet to be committed to Kafka.
     * @return the Long lsn value, or null if there are no unprocessed LSNs
     */
    private Long getEarliestUnprocessedLsn() {
        // If there are no elements, return null
        Enumeration<Long> lsnIterator = lsnsInProgress.keys();
        if (!lsnIterator.hasMoreElements()) {
            return null;
        }

        // Otherwise, iterate through each lsn to find the smallest
        Long earliestLsn = lsnIterator.nextElement();
        while (lsnIterator.hasMoreElements()) {
            Long lsn = lsnIterator.nextElement();
            if (lsn < earliestLsn) {
                earliestLsn = lsn;
            }
        }

        return earliestLsn;
    }

    /**
     * Get the earliest polled LSN from an event that has yet to be committed to Kafka.
     * @param upperBound
     * @return the Long lsn value, or null if there are no unprocessed LSNs
     */
    private Long getLargestProcessedLsnLessThan(Long upperBound) {
        // If there was no upper bound passed in, then set it to Long.MAX_VALUE
        // so that we get the largest processed lsn so far
        if (upperBound == null) {
            upperBound = Long.MAX_VALUE;
        }

        // Define a const for no matching LSN found, and initialize the largest
        // lsn to that constant
        final Long NO_MATCH = -1L;
        Long largestLsnInBounds = NO_MATCH;

        // Iterate through the processed LSNs to find the largest one that's
        // less than the upper bound
        Enumeration<Long> lsnsProcessedIterator = lsnsProcessed.keys();
        while (lsnsProcessedIterator.hasMoreElements()) {
            Long lsn = lsnsProcessedIterator.nextElement();
            if (lsn > largestLsnInBounds && lsn < upperBound) {
                largestLsnInBounds = lsn;
            }
        }

        // Return the largest matching LSN, or null if none was found
        if (largestLsnInBounds == NO_MATCH) {
            return null;
        } else {
            return largestLsnInBounds;
        }
    }

    /**
     * 
     */
    private void removeProcessedLsnsLessThan(Long upperBound) {
        if (upperBound == null) {
            return;
        }

        lsnsProcessed.keySet().removeIf((lsn) -> lsn < upperBound);
    }

    public String toString() {
        StringWriter sw = new StringWriter(92 + lsnsInProgress.size() * 24 + lsnsProcessed.size() * 20);
        sw.append("PostgresPendingLsnStore [lsnsInProgress (");
        sw.append(Integer.toString(lsnsInProgress.size()));
        sw.append(" items) [");
        lsnsInProgress.forEach((lsn, count) -> sw.append(LogSequenceNumber.valueOf(lsn) + "=" + count + "; "));
        sw.append("]; lsnsProcessed (");
        sw.append(Integer.toString(lsnsProcessed.size()));
        sw.append(" items) [");
        lsnsProcessed.forEach((lsn, count) -> sw.append(LogSequenceNumber.valueOf(lsn) + "; "));
        sw.append("]]");
        return sw.toString();
    }

    private Long getLsnFromRecord(SourceRecord record) {
        return (Long) record.sourceOffset().get(SourceInfo.LSN_KEY);
    }
}
