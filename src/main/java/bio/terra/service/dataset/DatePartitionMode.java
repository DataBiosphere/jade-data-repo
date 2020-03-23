package bio.terra.service.dataset;

import bio.terra.common.PdaoConstant;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;

/** TODO */
public class DatePartitionMode implements PartitionMode {

    static final String NAME = "date";

    private String column;

    DatePartitionMode(String column) {
        this.column = column;
    }

    String getColumn() {
        return column;
    }

    public boolean partitionByIngestTime() {
        return column.equals(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public TimePartitioning asTimePartitioning() {
        // Google uses null to represent partition-by-ingest-time, for some reason.
        String partitionColumn = partitionByIngestTime() ? null : column;

        // TimePartitioning only supports the "DAY" type right now, so we hard-code it here.
        // They might have it as an enum to prep for adding a "TIME" type in the future?
        return TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
            .setField(partitionColumn)
            .build();
    }

    @Override
    public RangePartitioning asRangePartitioning() {
        return null;
    }
}
