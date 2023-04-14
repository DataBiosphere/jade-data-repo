package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;
import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.serialization.UFLiteral;
import com.google.common.base.Strings;
import java.sql.Date;
import java.util.Objects;
import java.util.stream.Stream;

public class Literal implements SQLExpression {
  /** Enum for the data types supported by Tanagra. */
  public enum DataType {
    INT64,
    STRING,
    BOOLEAN,
    DATE,
    DOUBLE
  }

  private final DataType dataType;
  private String stringVal;
  private long int64Val;
  private boolean booleanVal;
  private Date dateVal;
  private double doubleVal;

  public Literal(String stringVal) {
    this.dataType = DataType.STRING;
    this.stringVal = stringVal;
  }

  public Literal(long int64Val) {
    this.dataType = DataType.INT64;
    this.int64Val = int64Val;
  }

  public Literal(boolean booleanVal) {
    this.dataType = DataType.BOOLEAN;
    this.booleanVal = booleanVal;
  }

  private Literal(Date dateVal) {
    this.dataType = DataType.DATE;
    this.dateVal = dateVal;
  }

  public Literal(double doubleVal) {
    this.dataType = DataType.DOUBLE;
    this.doubleVal = doubleVal;
  }

  public static Literal forDate(String dateVal) {
    return new Literal(Date.valueOf(dateVal));
  }

  private Literal(Builder builder) {
    this.dataType = builder.dataType;
    this.stringVal = builder.stringVal;
    this.int64Val = builder.int64Val;
    this.booleanVal = builder.booleanVal;
    this.dateVal = builder.dateVal;
    this.doubleVal = builder.doubleVal;
  }

  public static Literal fromSerialized(UFLiteral serialized) {
    boolean stringValDefined = !Strings.isNullOrEmpty(serialized.getStringVal());
    boolean int64ValDefined = serialized.getInt64Val() != null;
    boolean booleanValDefined = serialized.getBooleanVal() != null;
    boolean dateValDefiend = serialized.getDateVal() != null;
    boolean doubleValDefined = serialized.getDoubleVal() != null;

    long numDefined =
        Stream.of(
                stringValDefined,
                int64ValDefined,
                booleanValDefined,
                dateValDefiend,
                doubleValDefined)
            .filter(b -> b)
            .count();
    if (numDefined == 0) {
      // TODO: Make a static NULL Literal instance, instead of overloading the String value.
      return new Literal((String) null);
    } else if (numDefined > 1) {
      throw new InvalidConfigException("More than one literal value defined");
    }

    if (stringValDefined) {
      return new Literal(serialized.getStringVal());
    } else if (int64ValDefined) {
      return new Literal(serialized.getInt64Val());
    } else if (booleanValDefined) {
      return new Literal(serialized.getBooleanVal());
    } else if (dateValDefiend) {
      return Literal.forDate(serialized.getDateVal());
    } else {
      return new Literal(serialized.getDoubleVal());
    }
  }

  // FIXME: for now, "escape" sql strings by mapping single quote to curly quote.
  private static String sqlEscape(String s) {
    return s.replace("'", "’");
  }

  @Override
  public String renderSQL(CloudPlatform platform) {
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

  public int compareTo(Literal value) {
    if (dataType != value.getDataType()) {
      return -1;
    }
    return switch (dataType) {
      case STRING -> stringVal.compareTo(value.getStringVal());
      case INT64 -> Long.compare(int64Val, value.getInt64Val());
      case BOOLEAN -> Boolean.compare(booleanVal, value.getBooleanVal());
      case DATE -> dateVal.compareTo(value.getDateVal());
      case DOUBLE -> Double.compare(doubleVal, value.getDoubleVal());
    };
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Literal)) {
      return false;
    }
    return compareTo((Literal) obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataType, stringVal, int64Val, booleanVal, dateVal, doubleVal);
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
      return new Literal(this);
    }
  }
}
