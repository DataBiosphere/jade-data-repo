package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.SQLExpression;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;

/** Example: HAVING COUNT(*) > 1 */
public class HavingFilterVariable implements SQLExpression {
  private final BinaryFilterVariable.BinaryOperator operator;
  private final int value;

  public HavingFilterVariable(BinaryFilterVariable.BinaryOperator operator, int value) {
    this.operator = operator;
    this.value = value;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return String.format("HAVING COUNT(*) %s %s", operator.renderSQL(platform), value);
  }
}
