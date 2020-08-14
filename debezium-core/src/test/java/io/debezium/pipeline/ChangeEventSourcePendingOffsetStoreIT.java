/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

/**
 * Integration test for {@link public class ChangeEventSourcePendingOffsetStore} class.
 */
public class ChangeEventSourcePendingOffsetStoreIT {

    @Test
    public void shouldReturnNullOnInitialization() throws Exception {
        ChangeEventSourcePendingOffsetStore offsetStore = createMockOffsetStore();
        assertNull(offsetStore.getFullyProcessedOffset());
    }

    @Test
    public void shouldReturnNullWhenNothingProcessed() throws Exception {
        ChangeEventSourcePendingOffsetStore offsetStore = createMockOffsetStore();
        offsetStore.recordPolledRecord(mockSourceRecord(1));
        assertNull(offsetStore.getFullyProcessedOffset());
    }

    @Test
    public void shouldReturnNullIfHaveNotProcessedSmallestOffset() throws Exception {
        // Generate the store and mocked records
        ChangeEventSourcePendingOffsetStore offsetStore = createMockOffsetStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);

        // Poll 3 offsets, and process the largest 2 of them
        offsetStore.recordPolledRecords(Arrays.asList(mock1, mock2, mock3));
        offsetStore.recordProcessedRecord(mock2);
        offsetStore.recordProcessedRecord(mock3);

        // Expect null
        assertNull(offsetStore.getFullyProcessedOffset());
    }

    @Test
    public void shouldReturnLargestProcessedWhenAllProcessed() throws Exception {
        // Generate the store and mocked records
        ChangeEventSourcePendingOffsetStore offsetStore = createMockOffsetStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);
        SourceRecord mock4 = mockSourceRecord(4);

        // Poll 4 offsets, and process all of them
        offsetStore.recordPolledRecords(Arrays.asList(mock1, mock2, mock3, mock4));
        offsetStore.recordProcessedRecord(mock4);
        offsetStore.recordProcessedRecord(mock1);
        offsetStore.recordProcessedRecord(mock3);
        offsetStore.recordProcessedRecord(mock2);

        // Expect the largest LSN to return
        assertEquals(new Long(4), offsetStore.getFullyProcessedOffset());
    }

    @Test
    public void shouldReturnLargestProcessedWithGap() throws Exception {
        // Generate the store and mocked records
        ChangeEventSourcePendingOffsetStore offsetStore = createMockOffsetStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);
        SourceRecord mock4 = mockSourceRecord(4);

        // Poll 4 offsets, and process only 3 of them (with a gap, and out of order)
        offsetStore.recordPolledRecords(Arrays.asList(mock1, mock2, mock3, mock4));
        offsetStore.recordProcessedRecord(mock4);
        offsetStore.recordProcessedRecord(mock1);
        offsetStore.recordProcessedRecord(mock2);

        // Expect the 2nd LSN to be the largest processed, since all below it are also processed
        // Should return the same value, even after multiple calls
        assertEquals(new Long(2), offsetStore.getFullyProcessedOffset());
        assertEquals(new Long(2), offsetStore.getFullyProcessedOffset());
    }

    @Test
    public void shouldKeepStateAcrossCommits() throws Exception {
        // Generate the store and mocked records
        ChangeEventSourcePendingOffsetStore offsetStore = createMockOffsetStore();
        SourceRecord mock1 = mockSourceRecord(1);
        SourceRecord mock2 = mockSourceRecord(2);
        SourceRecord mock3 = mockSourceRecord(3);
        SourceRecord mock4 = mockSourceRecord(4);
        SourceRecord mock5 = mockSourceRecord(5);
        SourceRecord mock6 = mockSourceRecord(6);

        // Poll 4 offsets, and process only 3 of them (with a gap, and out of order)
        offsetStore.recordPolledRecords(Arrays.asList(mock1, mock2, mock3, mock4));
        offsetStore.recordProcessedRecord(mock3);
        offsetStore.recordPolledRecords(Arrays.asList(mock5));
        offsetStore.recordProcessedRecord(mock1);
        assertEquals(new Long(1), offsetStore.getFullyProcessedOffset());

        offsetStore.recordProcessedRecord(mock5);
        offsetStore.recordProcessedRecord(mock2);
        assertEquals(new Long(3), offsetStore.getFullyProcessedOffset());

        offsetStore.recordProcessedRecord(mock4);
        assertEquals(new Long(5), offsetStore.getFullyProcessedOffset());

        offsetStore.recordPolledRecords(Arrays.asList(mock6));
        assertEquals(new Long(5), offsetStore.getFullyProcessedOffset());

        offsetStore.recordProcessedRecord(mock6);
        assertEquals(new Long(6), offsetStore.getFullyProcessedOffset());
    }

    private static ChangeEventSourcePendingOffsetStore createMockOffsetStore() {
        return new ChangeEventSourcePendingOffsetStore(
                record -> (Long) record.sourceOffset().get("MOCK_OFFSET"),
                offset -> String.valueOf(offset));
    }

    private static SourceRecord mockSourceRecord(long offset) {
        Map<String, Long> sourceOffset = Collections.singletonMap("MOCK_OFFSET", offset);
        return new SourceRecord(null, sourceOffset, null, null, null);
    }
}
