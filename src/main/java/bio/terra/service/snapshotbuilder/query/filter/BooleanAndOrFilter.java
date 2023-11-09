package bio.terra.service.snapshotbuilder.query.filter;

import bio.terra.service.snapshotbuilder.query.Filter;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import java.util.List;

public record BooleanAndOrFilter(
    BooleanAndOrFilterVariable.LogicalOperator operator, List<Filter> subFilters)
    implements Filter {

  @Override
  public BooleanAndOrFilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables) {
    return new BooleanAndOrFilterVariable(
        operator, subFilters.stream().map(sf -> sf.buildVariable(primaryTable, tables)).toList());
  }
}
