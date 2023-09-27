package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.SqlPlatform;
import java.util.List;
import org.stringtemplate.v4.ST;

public class NotFilterVariable extends FilterVariable {
  private final FilterVariable subFilter;

  public NotFilterVariable(FilterVariable subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  protected ST getSubstitutionTemplate(SqlPlatform platform) {
    return null;
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return subFilter.getFieldVariables();
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return "(NOT " + subFilter.renderSQL(platform) + ")";
  }
}
