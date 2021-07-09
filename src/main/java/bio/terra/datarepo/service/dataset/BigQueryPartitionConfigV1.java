package bio.terra.service.dataset;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;
import java.util.Objects;

public final class BigQueryPartitionConfigV1 {

  public enum Mode {
    NONE,
    INGEST_DATE,
    DATE,
    INT_RANGE
  }

  // NOTE: Version is here so we have some marker in the DB, so if
  // we want or need to change the settings we collect about BQ
  // partitioning in the future, we'll still be able to distinguish
  // the "old" style of config stored in the DB.
  @JsonProperty private long version;
  @JsonProperty private Mode mode;
  @JsonProperty private String columnName;
  @JsonProperty private Long intMin;
  @JsonProperty private Long intMax;
  @JsonProperty private Long intInterval;

  private BigQueryPartitionConfigV1(
      Mode mode, String columnName, Long intMin, Long intMax, Long intInterval) {
    this.version = 1;
    this.mode = mode;
    this.columnName = columnName;
    this.intMin = intMin;
    this.intMax = intMax;
    this.intInterval = intInterval;
  }

  public BigQueryPartitionConfigV1() {}

  String getColumnName() {
    return this.columnName;
  }

  Long getIntMin() {
    return this.intMin;
  }

  Long getIntMax() {
    return this.intMax;
  }

  Long getIntInterval() {
    return this.intInterval;
  }

  private static final BigQueryPartitionConfigV1 NONE_INSTANCE =
      new BigQueryPartitionConfigV1(Mode.NONE, null, null, null, null);

  private static final BigQueryPartitionConfigV1 INGEST_DATE_INSTANCE =
      new BigQueryPartitionConfigV1(Mode.INGEST_DATE, null, null, null, null);

  public static BigQueryPartitionConfigV1 none() {
    return NONE_INSTANCE;
  }

  public static BigQueryPartitionConfigV1 ingestDate() {
    return INGEST_DATE_INSTANCE;
  }

  public static BigQueryPartitionConfigV1 date(String columnName) {
    return new BigQueryPartitionConfigV1(Mode.DATE, columnName, null, null, null);
  }

  public static BigQueryPartitionConfigV1 intRange(
      String columnName, long min, long max, long interval) {
    return new BigQueryPartitionConfigV1(Mode.INT_RANGE, columnName, min, max, interval);
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
      RangePartitioning.Range range =
          RangePartitioning.Range.newBuilder()
              .setStart(intMin)
              .setEnd(intMax)
              .setInterval(intInterval)
              .build();
      return RangePartitioning.newBuilder().setField(columnName).setRange(range).build();
    } else {
      return null;
    }
  }

  // Auto-generated the methods below to make unit-testing easier.

  @Override
  public String toString() {
    return "BigQueryPartitionConfigV1{"
        + "mode="
        + mode
        + ", columnName='"
        + columnName
        + '\''
        + ", intMin="
        + intMin
        + ", intMax="
        + intMax
        + ", intInterval="
        + intInterval
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BigQueryPartitionConfigV1 that = (BigQueryPartitionConfigV1) o;
    return mode == that.mode
        && Objects.equals(columnName, that.columnName)
        && Objects.equals(intMin, that.intMin)
        && Objects.equals(intMax, that.intMax)
        && Objects.equals(intInterval, that.intInterval);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, columnName, intMin, intMax, intInterval);
  }
}
