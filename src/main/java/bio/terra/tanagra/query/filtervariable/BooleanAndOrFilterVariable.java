package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.SqlPlatform;
import java.util.List;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public class BooleanAndOrFilterVariable extends FilterVariable {
  private final LogicalOperator operator;
  private final List<FilterVariable> subFilters;

  public BooleanAndOrFilterVariable(LogicalOperator operator, List<FilterVariable> subFilters) {
    this.operator = operator;
    this.subFilters = subFilters;
  }

  @Override
  protected ST getSubstitutionTemplate(SqlPlatform platform) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return subFilters.stream().flatMap(filter -> filter.getFieldVariables().stream()).toList();
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return subFilters.stream()
        .map(sf -> sf.renderSQL(platform))
        .collect(Collectors.joining(" " + operator.renderSQL(platform) + " ", "(", ")"));
  }

  public enum LogicalOperator implements SQLExpression {
    AND,
    OR;

    @Override
    public String renderSQL(SqlPlatform platform) {
      return name();
    }
  }
}
