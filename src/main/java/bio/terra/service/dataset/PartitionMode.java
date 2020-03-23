package bio.terra.service.dataset;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;

/** TODO */
public interface PartitionMode {

    /** TODO */
    String getName();

    /** TODO */
    TimePartitioning asTimePartitioning();

    /** TODO */
    RangePartitioning asRangePartitioning();
}
