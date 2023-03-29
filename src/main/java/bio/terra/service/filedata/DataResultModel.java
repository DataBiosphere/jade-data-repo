package bio.terra.service.filedata;

import java.util.Map;
import java.util.Optional;

public class DataResultModel {
  private Map<String, Optional<Object>> rowResult;
  private int totalCount;
  private int filteredCount;

  public DataResultModel(
      Map<String, Optional<Object>> rowResult, int totalCount, int filteredCount) {
    this.rowResult = rowResult;
    this.totalCount = totalCount;
    this.filteredCount = filteredCount;
  }

  public Map<String, Optional<Object>> getRowResult() {
    return rowResult;
  }

  public int getTotalCount() {
    return totalCount;
  }

  public int getFilteredCount() {
    return filteredCount;
  }
}
