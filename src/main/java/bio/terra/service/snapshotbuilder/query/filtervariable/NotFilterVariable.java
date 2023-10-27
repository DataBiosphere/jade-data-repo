package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;

public class NotFilterVariable implements FilterVariable {
  private final FilterVariable subFilter;

  public NotFilterVariable(FilterVariable subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return "(NOT " + subFilter.renderSQL(platform) + ")";
  }
}
