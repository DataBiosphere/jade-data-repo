package bio.terra.service.tabulardata;

public abstract class DataResultModel {
  protected int totalCount;
  protected int filteredCount;

  public int getTotalCount() {
    return this.totalCount;
  }

  public int getFilteredCount() {
    return filteredCount;
  }
}
