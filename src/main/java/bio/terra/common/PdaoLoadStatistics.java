package bio.terra.common;

import com.google.cloud.bigquery.JobStatistics;
import java.time.Instant;

public class PdaoLoadStatistics {
  private final long badRecords;
  private final long rowCount;
  private final Instant startTime;
  private final Instant endTime;

  public PdaoLoadStatistics(JobStatistics.LoadStatistics loadStatistics) {
    this.badRecords = loadStatistics.getBadRecords();
    this.rowCount = loadStatistics.getOutputRows();
    this.startTime = Instant.ofEpochMilli(loadStatistics.getStartTime());
    this.endTime = Instant.ofEpochMilli(loadStatistics.getEndTime());
  }

  public long getBadRecords() {
    return badRecords;
  }

  public long getRowCount() {
    return rowCount;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }
}
