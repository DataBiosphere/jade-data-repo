package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.SqlPlatform;

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
