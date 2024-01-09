package bio.terra.tanagra.serialization;

import bio.terra.tanagra.query.Literal;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a literal value.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFLiteral.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFLiteral {
  private final String stringVal;
  private final Long int64Val;
  private final Boolean booleanVal;
  private final String dateVal;
  private final Double doubleVal;

  public UFLiteral(Literal literal) {
    this.stringVal = literal.getStringVal();
    this.int64Val = literal.getInt64Val();
    this.booleanVal = literal.getBooleanVal();
    this.dateVal = literal.getDateValAsString();
    this.doubleVal = literal.getDoubleVal();
  }

  private UFLiteral(Builder builder) {
    this.stringVal = builder.stringVal;
    this.int64Val = builder.int64Val;
    this.booleanVal = builder.booleanVal;
    this.dateVal = builder.dateVal;
    this.doubleVal = builder.doubleVal;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private String stringVal;
    private Long int64Val;
    private Boolean booleanVal;
    private String dateVal;
    private Double doubleVal;

    public Builder stringVal(String stringVal) {
      this.stringVal = stringVal;
      return this;
    }

    public Builder int64Val(Long int64Val) {
      this.int64Val = int64Val;
      return this;
    }

    public Builder booleanVal(Boolean booleanVal) {
      this.booleanVal = booleanVal;
      return this;
    }

    public Builder dateVal(String dateVal) {
      this.dateVal = dateVal;
      return this;
    }

    public Builder doubleVal(Double doubleVal) {
      this.doubleVal = doubleVal;
      return this;
    }

    /** Call the private constructor. */
    public UFLiteral build() {
      return new UFLiteral(this);
    }
  }

  public String getStringVal() {
    return stringVal;
  }

  public Long getInt64Val() {
    return int64Val;
  }

  public Boolean getBooleanVal() {
    return booleanVal;
  }

  public String getDateVal() {
    return dateVal;
  }

  public Double getDoubleVal() {
    return doubleVal;
  }
}
