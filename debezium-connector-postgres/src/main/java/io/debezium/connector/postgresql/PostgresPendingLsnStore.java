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

import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.replication.LogSequenceNumber;

public class PostgresPendingLsnStore {
    private final ConcurrentHashMap<Long, Integer> lsnsInProgress = new ConcurrentHashMap<>();

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

        lsnsInProgress.compute(lsn, (_lsn, count) -> {
            if (count == null || count < 0) {
                throw new IllegalStateException("Attempted to record processed LSN when it hasn't been polled");
            } else if (count == 1) {
                return null;
            } else {
                return count - 1;
            }
        });
    }

    public Long getEarliestUnprocessedLsn() {
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

    public String toString() {
        StringWriter sw = new StringWriter(50 + lsnsInProgress.size() * 22);
        sw.append("PostgresPendingLsnStore (");
        sw.append(Integer.toString(lsnsInProgress.size()));
        sw.append(" items) [");
        lsnsInProgress.forEach((lsn, count) -> sw.append(LogSequenceNumber.valueOf(lsn) + "=" + count + "; "));
        sw.append("]");
        return sw.toString();
    }

    private Long getLsnFromRecord(SourceRecord record) {
        return (Long) record.sourceOffset().get(SourceInfo.LSN_KEY);
    }
}
