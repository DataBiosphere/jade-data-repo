package bio.terra.service.dataset;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;

/** Fallback partition mode that does not enable partitioning. */
public final class NoPartitionMode implements PartitionMode {

    private NoPartitionMode() {}

    static final String NAME = "none";

    public static final PartitionMode INSTANCE = new NoPartitionMode();

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
        return null;
    }

    @Override
    public String getGeneratedColumn() {
        return null;
    }
}
