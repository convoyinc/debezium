/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;

public class ChangeEventSourcePendingOffsetStore {
    @FunctionalInterface
    public interface SourceRecordToLongOffsetOperator {
        public long convert(SourceRecord sourceRecord);
    }

    @FunctionalInterface
    public interface LongOffsetToStringOperator {
        public String convert(long offsetToString);
    }

    private static final String OFFSET_PROCESSED = "OFFSET_PROCESSED";

    private final ConcurrentHashMap<Long, Integer> offsetsInProgress = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> offsetsProcessed = new ConcurrentHashMap<>();

    private SourceRecordToLongOffsetOperator sourceRecordToOffset;
    private LongOffsetToStringOperator offsetToString;

    public ChangeEventSourcePendingOffsetStore(SourceRecordToLongOffsetOperator sourceRecordToOffset, LongOffsetToStringOperator offsetToString) {
        this.sourceRecordToOffset = sourceRecordToOffset;
        this.offsetToString = offsetToString;
    }

    public void recordPolledRecord(SourceRecord record) {
        Long offset = sourceRecordToOffset.convert(record);
        if (offset == null) {
            return;
        }

        offsetsInProgress.compute(offset, (_offset, count) -> {
            if (count == null) {
                return 1;
            }
            else {
                return count + 1;
            }
        });
    }

    public void recordPolledRecords(Collection<SourceRecord> records) {
        records.forEach(this::recordPolledRecord);
    }

    public void recordProcessedRecord(SourceRecord record) throws IllegalStateException {
        Long offset = sourceRecordToOffset.convert(record);
        if (offset == null) {
            return;
        }

        AtomicBoolean isFullyProcessed = new AtomicBoolean(false);
        offsetsInProgress.compute(offset, (_offset, count) -> {
            if (count == null || count < 0) {
                throw new IllegalStateException("Attempted to record processed offset when it hasn't been polled");
            }
            else if (count == 1) {
                isFullyProcessed.set(true);
                return null;
            }
            else {
                return count - 1;
            }
        });

        if (isFullyProcessed.get()) {
            offsetsProcessed.put(offset, OFFSET_PROCESSED);
        }
    }

    /**
     * Get the largest offset which has been processed, and for which all events before it have been processed.
     * @return the Long offset value, or null if there are no events that have been processed yet.
     */
    public Long getFullyProcessedOffset() {
        Long earliestUnprocessedOffset = getEarliestUnprocessedOffset();
        Long largestProcessedOffset = getLargestProcessedOffsetLessThan(earliestUnprocessedOffset);
        removeProcessedOffsetsLessThan(largestProcessedOffset);
        return largestProcessedOffset;
    }

    /**
     * Get the earliest polled offset from an event that has yet to be committed to Kafka.
     * @return the Long offset value, or null if there are no unprocessed offsets
     */
    private Long getEarliestUnprocessedOffset() {
        // If there are no elements, return null
        Enumeration<Long> offsetIterator = offsetsInProgress.keys();
        if (!offsetIterator.hasMoreElements()) {
            return null;
        }

        // Otherwise, iterate through each offset to find the smallest
        Long earliestOffset = offsetIterator.nextElement();
        while (offsetIterator.hasMoreElements()) {
            Long offset = offsetIterator.nextElement();
            if (offset < earliestOffset) {
                earliestOffset = offset;
            }
        }

        return earliestOffset;
    }

    /**
     * Get the largest committed LSN less than the passed upper bound. If no
     * upper bound is passed, then just get the largest committed LSN.
     * @param upperBound
     * @return the Long offset value, or null if there are no unprocessed LSNs
     */
    private Long getLargestProcessedOffsetLessThan(Long upperBound) {
        // If there was no upper bound passed in, then set it to Long.MAX_VALUE
        // so that we get the largest processed offset so far
        if (upperBound == null) {
            upperBound = Long.MAX_VALUE;
        }

        // Define a const for no matching LSN found, and initialize the largest
        // offset to that constant
        final Long NO_MATCH = -1L;
        Long largestOffsetInBounds = NO_MATCH;

        // Iterate through the processed LSNs to find the largest one that's
        // less than the upper bound
        Enumeration<Long> offsetsProcessedIterator = offsetsProcessed.keys();
        while (offsetsProcessedIterator.hasMoreElements()) {
            Long offset = offsetsProcessedIterator.nextElement();
            if (offset > largestOffsetInBounds && offset < upperBound) {
                largestOffsetInBounds = offset;
            }
        }

        // Return the largest matching LSN, or null if none was found
        if (largestOffsetInBounds == NO_MATCH) {
            return null;
        }
        else {
            return largestOffsetInBounds;
        }
    }

    /**
     * Removes all processed offsets lower than the passed upper bound. This is a clean-up
     * operation. The passed LSN should be the largest LSN that we've processed everything
     * up through.
     */
    private void removeProcessedOffsetsLessThan(Long upperBound) {
        if (upperBound == null) {
            return;
        }

        offsetsProcessed.keySet().removeIf((offset) -> offset < upperBound);
    }

    public String toString() {
        // Estimate how long the stringified dump is going to be
        final int fixedCharacterLength = 100;
        final int offsetLength = String.valueOf(Long.MAX_VALUE).length();
        final int offsetInProgressLength = offsetLength + 8;
        final int offsetProcessedLength = offsetLength + 2;
        final int estimatedStringLength = fixedCharacterLength + offsetsInProgress.size() * offsetInProgressLength + offsetsProcessed.size() * offsetProcessedLength;

        //
        StringWriter sw = new StringWriter(estimatedStringLength);
        sw.append("ChangeEventPendingOffsetStore [offsetsInProgress (");
        sw.append(Integer.toString(offsetsInProgress.size()));
        sw.append(" items) [");
        offsetsInProgress.forEach((offset, count) -> sw.append(offsetToString.convert(offset) + "=" + count + "; "));
        sw.append("]; offsetsProcessed (");
        sw.append(Integer.toString(offsetsProcessed.size()));
        sw.append(" items) [");
        offsetsProcessed.forEach((offset, count) -> sw.append(offsetToString.convert(offset) + "; "));
        sw.append("]]");
        return sw.toString();
    }
}
