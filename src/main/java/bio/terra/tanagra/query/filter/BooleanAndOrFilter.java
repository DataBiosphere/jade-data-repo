package bio.terra.tanagra.query.filter;

import bio.terra.tanagra.query.Filter;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.BooleanAndOrFilterVariable;
import java.util.List;

public final class BooleanAndOrFilter extends Filter {
  private final BooleanAndOrFilterVariable.LogicalOperator operator;
  private final List<Filter> subFilters;

  public BooleanAndOrFilter(
      BooleanAndOrFilterVariable.LogicalOperator operator, List<Filter> subFilters) {
    super(Type.BOOLEAN_AND_OR);
    this.operator = operator;
    this.subFilters = subFilters;
  }

  @Override
  public BooleanAndOrFilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables) {
    return new BooleanAndOrFilterVariable(
        operator, subFilters.stream().map(sf -> sf.buildVariable(primaryTable, tables)).toList());
  }
}
