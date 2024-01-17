package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;

/** Example: HAVING COUNT(*) > 1 */
public class HavingFilterVariable implements FilterVariable {
  private final BinaryFilterVariable.BinaryOperator operator;
  private final int value;

  public HavingFilterVariable(BinaryFilterVariable.BinaryOperator operator, int value) {
    this.operator = operator;
    this.value = value;
  }

  @Override
  public String renderSQL() {
    return String.format("HAVING COUNT(*) %s %s", operator.renderSQL(), value);
  }

  @Override
  public List<TableVariable> getTables() {
    return List.of();
  }
}
