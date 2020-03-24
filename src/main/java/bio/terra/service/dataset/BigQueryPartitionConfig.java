package bio.terra.service.dataset;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;

public final class BigQueryPartitionConfig {

    public enum Mode { NONE, INGEST_DATE, DATE, INT_RANGE }

    private Mode mode;
    private String columnName;
    private Long intMin;
    private Long intMax;
    private Long intInterval;

    private BigQueryPartitionConfig(Mode mode, String columnName, Long intMin, Long intMax, Long intInterval) {
        this.mode = mode;
        this.columnName = columnName;
        this.intMin = intMin;
        this.intMax = intMax;
        this.intInterval = intInterval;
    }

    private static final BigQueryPartitionConfig NONE_INSTANCE =
        new BigQueryPartitionConfig(Mode.NONE, null, null, null, null);

    private static final BigQueryPartitionConfig INGEST_DATE_INSTANCE =
        new BigQueryPartitionConfig(Mode.INGEST_DATE, null, null, null, null);

    public static BigQueryPartitionConfig none() {
        return NONE_INSTANCE;
    }

    public static BigQueryPartitionConfig ingestDate() {
        return INGEST_DATE_INSTANCE;
    }

    public static BigQueryPartitionConfig date(String columnName) {
        return new BigQueryPartitionConfig(Mode.DATE, columnName, null, null, null);
    }

    public static BigQueryPartitionConfig intRange(String columnName, long min, long max, long interval) {
        return new BigQueryPartitionConfig(Mode.INT_RANGE, columnName, min, max, interval);
    }

    public Mode getMode() {
        return this.mode;
    }

    public TimePartitioning asTimePartitioning() {
        if (mode == Mode.INGEST_DATE || mode == Mode.DATE) {
            // TimePartitioning only supports the "DAY" type right now, so we hard-code it here.
            // They might have it as an enum to prep for adding a "TIME" type in the future?
            return TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                // NOTE: When mode == INGEST_DATE, the column name will be null. That's ok because
                // Google uses null to represent partition-by-ingest-time, for some reason.
                .setField(columnName)
                .build();
        } else {
            return null;
        }
    }

    public RangePartitioning asRangePartitioning() {
        if (mode == Mode.INT_RANGE) {
            RangePartitioning.Range range = RangePartitioning.Range.newBuilder()
                .setStart(intMin)
                .setEnd(intMax)
                .setInterval(intInterval)
                .build();
            return RangePartitioning.newBuilder()
                .setField(columnName)
                .setRange(range)
                .build();
        } else {
            return null;
        }
    }
}
