package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.SqlPlatform;

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
