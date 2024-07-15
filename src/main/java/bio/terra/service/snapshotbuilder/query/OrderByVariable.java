package bio.terra.service.snapshotbuilder.query;

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

  public String renderSQL(boolean isIncludedInSelect, SqlRenderContext context) {
    return isRandom
        ? "RAND()"
        : fieldVariable.renderSqlForOrderOrGroupBy(isIncludedInSelect, context)
            + " "
            + direction.renderSQL(context);
  }
}
