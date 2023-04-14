package bio.terra.tanagra.query.filtervariable;

import bio.terra.model.CloudPlatform;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import java.util.List;

public class NotFilterVariable extends FilterVariable {
  private final FilterVariable subFilter;

  public NotFilterVariable(FilterVariable subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  protected String getSubstitutionTemplate(CloudPlatform platform) {
    return null;
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return subFilter.getFieldVariables();
  }

  @Override
  public String renderSQL(CloudPlatform platform) {
    return "(NOT " + subFilter.renderSQL(platform) + ")";
  }
}
