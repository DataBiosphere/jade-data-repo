package bio.terra.tanagra.query.filter;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.Filter;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.tanagra.serialization.filter.UFBooleanAndOrFilter;
import java.util.List;

public final class BooleanAndOrFilter implements Filter {
  private final BooleanAndOrFilterVariable.LogicalOperator operator;
  private final List<Filter> subfilters;

  private BooleanAndOrFilter(
      BooleanAndOrFilterVariable.LogicalOperator operator, List<Filter> subfilters) {
    this.operator = operator;
    this.subfilters = subfilters;
  }

  public static BooleanAndOrFilter fromSerialized(
      UFBooleanAndOrFilter serialized, TablePointer tablePointer) {
    if (serialized.getOperator() == null) {
      throw new InvalidConfigException("Boolean and/or filter operator is undefined");
    }
    if (serialized.getSubfilters() == null || serialized.getSubfilters().isEmpty()) {
      throw new InvalidConfigException("Boolean and/or filter has no sub-filters defined");
    }
    List<Filter> subFilters =
        serialized.getSubfilters().stream()
            .map(sf -> sf.deserializeToInternal(tablePointer))
            .toList();
    return new BooleanAndOrFilter(serialized.getOperator(), subFilters);
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_AND_OR;
  }

  @Override
  public BooleanAndOrFilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables) {
    return new BooleanAndOrFilterVariable(
        operator, subfilters.stream().map(sf -> sf.buildVariable(primaryTable, tables)).toList());
  }

  public BooleanAndOrFilterVariable.LogicalOperator getOperator() {
    return operator;
  }

  public List<Filter> getSubfilters() {
    return List.copyOf(subfilters);
  }
}
