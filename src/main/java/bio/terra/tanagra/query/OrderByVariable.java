package bio.terra.tanagra.query;

public class OrderByVariable {
  protected final FieldVariable fieldVariable;
  private final OrderByDirection direction;
  private final boolean isRandom;

  private OrderByVariable(
      FieldVariable fieldVariable, OrderByDirection direction, boolean isRandom) {
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

  public String renderSQL(SqlPlatform platform, boolean isIncludedInSelect) {
    return isRandom
        ? "RAND()"
        : fieldVariable.renderSqlForOrderOrGroupBy(isIncludedInSelect)
            + " "
            + direction.renderSQL(platform);
  }
}
