package bio.terra.service.filedata;

import java.util.Map;
import java.util.Optional;

public class SynapseDataResultModel extends DataResultModel {
  Map<String, Optional<Object>> rowResult;

  public SynapseDataResultModel() {}

  public SynapseDataResultModel(Map<String, Optional<Object>> rowResult, int filteredRowCount) {
    this.rowResult = rowResult;
    this.filteredCount = filteredRowCount;
  }

  public Map<String, Optional<Object>> getRowResult() {
    return rowResult;
  }

  public SynapseDataResultModel rowResult(Map<String, Optional<Object>> rowResult) {
    this.rowResult = rowResult;
    return this;
  }

  public SynapseDataResultModel totalCount(int totalCount) {
    this.totalCount = totalCount;
    return this;
  }

  public int getFilteredCount() {
    return filteredCount;
  }

  public SynapseDataResultModel filteredCount(int filteredCount) {
    this.filteredCount = filteredCount;
    return this;
  }
}
