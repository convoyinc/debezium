/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

/**
 * Integration test for {@link public class PostgresPendingLsnStore} class.
 */
public class PostgresPendingLsnStoreIT {

    @Test
    public void shouldReturnNullOnInitialization() throws Exception {
        PostgresPendingLsnStore lsnStore = new PostgresPendingLsnStore();
        assertNull(lsnStore.getFullyProcessedLsn());
    }

    @Test
    public void shouldReturnNullWhenNothingProcessed() throws Exception {
        PostgresPendingLsnStore lsnStore = new PostgresPendingLsnStore();
        lsnStore.recordPolledLsn(mockSourceRecord(1));
        assertNull(lsnStore.getFullyProcessedLsn());
    }

    @Test
    public void shouldReturnNullIfHaveNotProcessedSmallestLsn() throws Exception {
        // Generate the store and mocked records
        PostgresPendingLsnStore lsnStore = new PostgresPendingLsnStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);

        // Poll 3 LSNs, and process the largest 2 of them
        lsnStore.recordPolledLsns(Arrays.asList(mock1, mock2, mock3));
        lsnStore.recordProcessedLsn(mock2);
        lsnStore.recordProcessedLsn(mock3);

        // Expect null
        assertNull(lsnStore.getFullyProcessedLsn());
    }

    @Test
    public void shouldReturnLargestProcessedWhenAllProcessed() throws Exception {
        // Generate the store and mocked records
        PostgresPendingLsnStore lsnStore = new PostgresPendingLsnStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);
        SourceRecord mock4 = mockSourceRecord(4);

        // Poll 4 LSNs, and process all of them
        lsnStore.recordPolledLsns(Arrays.asList(mock1, mock2, mock3, mock4));
        lsnStore.recordProcessedLsn(mock4);
        lsnStore.recordProcessedLsn(mock1);
        lsnStore.recordProcessedLsn(mock3);
        lsnStore.recordProcessedLsn(mock2);

        // Expect the largest LSN to return
        assertEquals(new Long(4), lsnStore.getFullyProcessedLsn());
    }

    @Test
    public void shouldReturnLargestProcessedWithGap() throws Exception {
        // Generate the store and mocked records
        PostgresPendingLsnStore lsnStore = new PostgresPendingLsnStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);
        SourceRecord mock4 = mockSourceRecord(4);

        // Poll 4 LSNs, and process only 3 of them (with a gap, and out of order)
        lsnStore.recordPolledLsns(Arrays.asList(mock1, mock2, mock3, mock4));
        lsnStore.recordProcessedLsn(mock4);
        lsnStore.recordProcessedLsn(mock1);
        lsnStore.recordProcessedLsn(mock2);

        // Expect the 2nd LSN to be the largest processed, since all below it are also processed
        // Should return the same value, even after multiple calls
        assertEquals(new Long(2), lsnStore.getFullyProcessedLsn());
        assertEquals(new Long(2), lsnStore.getFullyProcessedLsn());
    }

    @Test
    public void shouldKeepStateAcrossCommits() throws Exception {
        // Generate the store and mocked records
        PostgresPendingLsnStore lsnStore = new PostgresPendingLsnStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);
        SourceRecord mock4 = mockSourceRecord(4);
        SourceRecord mock5 = mockSourceRecord(5);
        SourceRecord mock6 = mockSourceRecord(6);

        // Poll 4 LSNs, and process only 3 of them (with a gap, and out of order)
        lsnStore.recordPolledLsns(Arrays.asList(mock1, mock2, mock3, mock4));
        lsnStore.recordProcessedLsn(mock3);
        lsnStore.recordPolledLsns(Arrays.asList(mock5));
        lsnStore.recordProcessedLsn(mock1);
        assertEquals(new Long(1), lsnStore.getFullyProcessedLsn());

        lsnStore.recordProcessedLsn(mock5);
        lsnStore.recordProcessedLsn(mock2);
        assertEquals(new Long(3), lsnStore.getFullyProcessedLsn());

        lsnStore.recordProcessedLsn(mock4);
        assertEquals(new Long(5), lsnStore.getFullyProcessedLsn());

        lsnStore.recordPolledLsns(Arrays.asList(mock6));
        assertEquals(new Long(5), lsnStore.getFullyProcessedLsn());

        lsnStore.recordProcessedLsn(mock6);
        assertEquals(new Long(6), lsnStore.getFullyProcessedLsn());
    }

    private static SourceRecord mockSourceRecord(long lsn) {
        Map<String, Long> sourceOffset = Collections.singletonMap(SourceInfo.LSN_KEY, lsn);
        return new SourceRecord(null, sourceOffset, null, null, null);
    }
}
