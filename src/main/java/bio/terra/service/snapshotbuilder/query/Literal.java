package bio.terra.service.snapshotbuilder.query;

import java.sql.Date;

public record Literal(
    DataType dataType,
    String stringVal,
    long int64Val,
    boolean booleanVal,
    Date dateVal,
    double doubleVal)
    implements SqlExpression {

  /** Enum for the data types supported by Tanagra. */
  public enum DataType {
    INT64,
    STRING,
    BOOLEAN,
    DATE,
    DOUBLE
  }

  public Literal(String stringVal) {
    this(DataType.STRING, stringVal, 0, false, null, 0.0);
  }

  public Literal(long int64Val) {
    this(DataType.INT64, null, int64Val, false, null, 0.0);
  }

  public Literal(boolean booleanVal) {
    this(DataType.BOOLEAN, null, 0, booleanVal, null, 0.0);
  }

  public Literal(Date dateVal) {
    this(DataType.DATE, null, 0, false, dateVal, 0.0);
  }

  public Literal(double doubleVal) {
    this(DataType.DOUBLE, null, 0, false, null, doubleVal);
  }

  // TODO: use named parameters for literals to protect against SQL injection
  // FIXME: for now, "escape" sql strings by mapping single quote to curly quote.
  private static String sqlEscape(String s) {
    return s.replace("'", "’");
  }

  @Override
  public String renderSQL() {
    return switch (dataType) {
      case STRING -> stringVal == null ? "NULL" : "'" + sqlEscape(stringVal) + "'";
      case INT64 -> String.valueOf(int64Val);
      case BOOLEAN -> String.valueOf(booleanVal);
      case DATE -> "DATE('" + dateVal.toString() + "')";
      case DOUBLE -> "FLOAT('" + doubleVal + "')";
    };
  }
}
