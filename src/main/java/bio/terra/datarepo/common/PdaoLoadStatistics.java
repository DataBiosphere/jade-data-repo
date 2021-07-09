package bio.terra.common;

import java.time.Instant;

public class PdaoLoadStatistics {
  private long badRecords;
  private long rowCount;
  private Instant startTime;
  private Instant endTime;

  public long getBadRecords() {
    return badRecords;
  }

  public PdaoLoadStatistics badRecords(long badRecords) {
    this.badRecords = badRecords;
    return this;
  }

  public long getRowCount() {
    return rowCount;
  }

  public PdaoLoadStatistics rowCount(long rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public PdaoLoadStatistics startTime(Instant startTime) {
    this.startTime = startTime;
    return this;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public PdaoLoadStatistics endTime(Instant endTime) {
    this.endTime = endTime;
    return this;
  }
}
