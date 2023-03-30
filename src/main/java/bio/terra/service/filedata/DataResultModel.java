package bio.terra.service.filedata;

import java.util.Map;

public class DataResultModel {
  private Map<String, Object> rowResult;
  private int totalCount;
  private int filteredCount;

  public DataResultModel() {}

  public DataResultModel(Map<String, Object> rowResult, int totalCount, int filteredCount) {
    this.rowResult = rowResult;
    this.totalCount = totalCount;
    this.filteredCount = filteredCount;
  }

  public Map<String, Object> getRowResult() {
    return rowResult;
  }

  public DataResultModel rowResult(Map<String, Object> rowResult) {
    this.rowResult = rowResult;
    return this;
  }

  public int getTotalCount() {
    return totalCount;
  }

  public DataResultModel totalCount(int totalCount) {
    this.totalCount = totalCount;
    return this;
  }

  public int getFilteredCount() {
    return filteredCount;
  }

  public DataResultModel filteredCount(int filteredCount) {
    this.filteredCount = filteredCount;
    return this;
  }
}
