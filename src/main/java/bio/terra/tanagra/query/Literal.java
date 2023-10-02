package bio.terra.tanagra.query;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Date;

@SuppressFBWarnings(
    value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
    justification = "Date is mutable; ingore for now")
public record Literal(
    DataType dataType,
    String stringVal,
    long int64Val,
    boolean booleanVal,
    Date dateVal,
    double doubleVal)
    implements SQLExpression {

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

  private Literal(Date dateVal) {
    this(DataType.DATE, null, 0, false, dateVal, 0.0);
  }

  public Literal(double doubleVal) {
    this(DataType.DOUBLE, null, 0, false, null, doubleVal);
  }

  public static Literal forDate(String dateVal) {
    return new Literal(Date.valueOf(dateVal));
  }

  // FIXME: for now, "escape" sql strings by mapping single quote to curly quote.
  private static String sqlEscape(String s) {
    return s.replace("'", "’");
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    // TODO: use named parameters for literals to protect against SQL injection
    return switch (dataType) {
      case STRING -> stringVal == null ? "NULL" : "'" + sqlEscape(stringVal) + "'";
      case INT64 -> String.valueOf(int64Val);
      case BOOLEAN -> String.valueOf(booleanVal);
      case DATE -> "DATE('" + dateVal.toString() + "')";
      case DOUBLE -> "FLOAT('" + doubleVal + "')";
    };
  }

  @Override
  public String toString() {
    return switch (dataType) {
      case STRING -> stringVal;
      case INT64 -> String.valueOf(int64Val);
      case BOOLEAN -> String.valueOf(booleanVal);
      case DATE -> dateVal.toString();
      case DOUBLE -> String.valueOf(doubleVal);
    };
  }

  public String getStringVal() {
    return dataType == DataType.STRING ? stringVal : null;
  }

  public Long getInt64Val() {
    return dataType == DataType.INT64 ? int64Val : null;
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      value = "NP_BOOLEAN_RETURN_NULL",
      justification =
          "This value will be used in constructing a SQL string, not used directly in a Java conditional")
  public Boolean getBooleanVal() {
    return dataType == DataType.BOOLEAN ? booleanVal : null;
  }

  public Date getDateVal() {
    return dataType == DataType.DATE ? dateVal : null;
  }

  public String getDateValAsString() {
    return dataType == DataType.DATE ? dateVal.toString() : null;
  }

  public Double getDoubleVal() {
    return dataType == DataType.DOUBLE ? doubleVal : null;
  }

  public DataType getDataType() {
    return dataType;
  }

  public static class Builder {
    private DataType dataType;
    private String stringVal;
    private long int64Val;
    private boolean booleanVal;
    private Date dateVal;
    private double doubleVal;

    public Builder dataType(DataType dataType) {
      this.dataType = dataType;
      return this;
    }

    public Builder stringVal(String stringVal) {
      this.stringVal = stringVal;
      return this;
    }

    public Builder int64Val(long int64Val) {
      this.int64Val = int64Val;
      return this;
    }

    public Builder booleanVal(boolean booleanVal) {
      this.booleanVal = booleanVal;
      return this;
    }

    public Builder dateVal(Date dateVal) {
      this.dateVal = dateVal == null ? null : (Date) dateVal.clone();
      return this;
    }

    public Builder doubleVal(double doubleVal) {
      this.doubleVal = doubleVal;
      return this;
    }

    public Literal build() {
      return new Literal(dataType, stringVal, int64Val, booleanVal, dateVal, doubleVal);
    }
  }
}
