package bio.terra.service.filedata.google.bq;

import bio.terra.service.filedata.DataResultModel;
import java.util.Map;

public class BigQueryDataResultModel extends DataResultModel {
  Map<String, Object> rowResult;

  public BigQueryDataResultModel() {}

  public Map<String, Object> getRowResult() {
    return rowResult;
  }

  public BigQueryDataResultModel rowResult(Map<String, Object> rowResult) {
    this.rowResult = rowResult;
    return this;
  }

  public BigQueryDataResultModel totalCount(int totalCount) {
    this.totalCount = totalCount;
    return this;
  }

  public int getFilteredCount() {
    return filteredCount;
  }

  public BigQueryDataResultModel filteredCount(int filteredCount) {
    this.filteredCount = filteredCount;
    return this;
  }
}
