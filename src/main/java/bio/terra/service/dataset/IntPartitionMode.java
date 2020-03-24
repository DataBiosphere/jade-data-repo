package bio.terra.service.dataset;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;

/** TODO */
public class IntPartitionMode implements PartitionMode {

    static final String NAME = "int";

    private String column;
    private long min;
    private long max;
    private long interval;

    IntPartitionMode(String column, long min, long max, long interval) {
        this.column = column;
        this.min = min;
        this.max = max;
        this.interval = interval;
    }

    String getColumn() {
        return column;
    }

    long getMin() {
        return min;
    }

    long getMax() {
        return max;
    }

    long getInterval() {
        return interval;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public TimePartitioning asTimePartitioning() {
        return null;
    }

    @Override
    public RangePartitioning asRangePartitioning() {
        RangePartitioning.Range range = RangePartitioning.Range.newBuilder()
            .setStart(min)
            .setEnd(max)
            .setInterval(interval)
            .build();
        return RangePartitioning.newBuilder()
            .setField(column)
            .setRange(range)
            .build();
    }

    @Override
    public String getGeneratedColumn() {
        return null;
    }
}
