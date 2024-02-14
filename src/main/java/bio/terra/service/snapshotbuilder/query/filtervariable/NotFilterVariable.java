package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.snapshotbuilder.query.FilterVariable;

public class NotFilterVariable implements FilterVariable {
  private final FilterVariable subFilter;

  public NotFilterVariable(FilterVariable subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    return "(NOT " + subFilter.renderSQL(platform) + ")";
  }
}
