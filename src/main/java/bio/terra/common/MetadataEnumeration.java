package bio.terra.common;

import java.util.List;

public class MetadataEnumeration<T> {
  private int total;
  private int filteredTotal;
  private List<T> items;

  public int getTotal() {
    return total;
  }

  public MetadataEnumeration<T> total(int total) {
    this.total = total;
    return this;
  }

  public int getFilteredTotal() {
    return filteredTotal;
  }

  public MetadataEnumeration<T> filteredTotal(int filteredTotal) {
    this.filteredTotal = filteredTotal;
    return this;
  }

  public List<T> getItems() {
    return items;
  }

  public MetadataEnumeration<T> items(List<T> items) {
    this.items = items;
    return this;
  }
}
