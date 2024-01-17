package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;

public class NotFilterVariable implements FilterVariable {
  private final FilterVariable subFilter;

  public NotFilterVariable(FilterVariable subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  public String renderSQL() {
    return "(NOT " + subFilter.renderSQL() + ")";
  }

  @Override
  public List<TableVariable> getTables() {
    return subFilter.getTables();
  }
}
