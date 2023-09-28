package bio.terra.tanagra.query;

public class OrderByVariable implements SQLExpression {
  private final FieldVariable fieldVariable;
  private final OrderByDirection direction;
  private final boolean isRandom;

  private OrderByVariable(FieldVariable fieldVariable, OrderByDirection direction, boolean isRandom) {
    this.fieldVariable = fieldVariable;
    this.direction = direction;
    this.isRandom = isRandom;
  }

  public OrderByVariable(FieldVariable fieldVariable) {
    this(fieldVariable, OrderByDirection.ASCENDING);
  }

  public OrderByVariable(FieldVariable fieldVariable, OrderByDirection direction) {
    this(fieldVariable, direction, false);
  }

  public static OrderByVariable random() {
    return new OrderByVariable(null, null, true);
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    return isRandom
        ? "RAND()"
        : fieldVariable.renderSqlForOrderBy() + " " + direction.renderSQL(platform);
  }
}
