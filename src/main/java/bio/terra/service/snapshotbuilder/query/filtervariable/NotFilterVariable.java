package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;

public class NotFilterVariable implements FilterVariable {
  private final FilterVariable subFilter;

  public NotFilterVariable(FilterVariable subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return "(NOT " + subFilter.renderSQL(context) + ")";
  }
}
