package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;

public class OrderByVariable implements SQLExpression {
  private final FieldVariable fieldVariable;
  private final OrderByDirection direction;
  private final boolean isRandom;

  public OrderByVariable(FieldVariable fieldVariable) {
    this.fieldVariable = fieldVariable;
    this.direction = OrderByDirection.ASCENDING;
    this.isRandom = false;
  }

  public OrderByVariable(FieldVariable fieldVariable, OrderByDirection direction) {
    this.fieldVariable = fieldVariable;
    this.direction = direction;
    this.isRandom = false;
  }

  private OrderByVariable() {
    this.fieldVariable = null;
    this.direction = null;
    this.isRandom = true;
  }

  public static OrderByVariable forRandom() {
    return new OrderByVariable();
  }

  @Override
  public String renderSQL(CloudPlatform platform) {
    return isRandom
        ? "RAND()"
        : fieldVariable.renderSqlForOrderBy() + " " + direction.renderSQL(platform);
  }
}
