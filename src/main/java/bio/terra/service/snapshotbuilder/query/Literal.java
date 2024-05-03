package bio.terra.service.snapshotbuilder.query;

import java.sql.Date;

public class Literal implements SelectExpression {
  private final DataType dataType;
  private final String stringVal;
  private final long int64Val;
  private final boolean booleanVal;
  private final Date dateVal;
  private final double doubleVal;

  /** Enum for the data types supported by Tanagra. */
  public enum DataType {
    INT64,
    STRING,
    BOOLEAN,
    DATE,
    DOUBLE
  }

  private Literal(
      DataType dataType,
      String stringVal,
      long int64Val,
      boolean booleanVal,
      Date dateVal,
      double doubleVal) {
    this.dataType = dataType;
    this.stringVal = stringVal;
    this.int64Val = int64Val;
    this.booleanVal = booleanVal;
    this.dateVal = dateVal;
    this.doubleVal = doubleVal;
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
    this(DataType.DATE, null, 0, false, new Date(dateVal.getTime()), 0.0);
  }

  public Literal(double doubleVal) {
    this(DataType.DOUBLE, null, 0, false, null, doubleVal);
  }

  /** Make a literal for NULL. */
  public Literal() {
    this(DataType.STRING, null, 0, false, null, 0.0);
  }

  // TODO: use named parameters for literals to protect against SQL injection
  // FIXME: for now, "escape" sql strings by mapping single quote to curly quote.
  private static String sqlEscape(String s) {
    return s.replace("'", "’");
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return switch (dataType) {
      case STRING -> stringVal == null ? "NULL" : "'" + sqlEscape(stringVal) + "'";
      case INT64 -> String.valueOf(int64Val);
      case BOOLEAN -> context
          .getPlatform()
          // In T-SQL, TRUE is not a keyword, so we need to use 1 instead. The JDBC API converts
          // the 1 to a boolean TRUE when the query result is processed.
          .choose(() -> String.valueOf(booleanVal), () -> booleanVal ? "1" : "0");
      case DATE -> "DATE('" + dateVal.toString() + "')";
      case DOUBLE -> "FLOAT('" + doubleVal + "')";
    };
  }
}
