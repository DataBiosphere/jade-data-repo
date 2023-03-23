package bio.terra.tanagra.serialization.filter;

import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.filter.BinaryFilter;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import bio.terra.tanagra.serialization.UFFieldPointer;
import bio.terra.tanagra.serialization.UFFilter;
import bio.terra.tanagra.serialization.UFLiteral;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a binary table filter: column operator value (e.g. domain_id =
 * "Condition").
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFBinaryFilter.Builder.class)
public class UFBinaryFilter extends UFFilter {
  private final UFFieldPointer field;
  private final BinaryFilterVariable.BinaryOperator operator;
  private final UFLiteral value;

  public UFBinaryFilter(BinaryFilter binaryFilter) {
    super(binaryFilter);
    this.field = new UFFieldPointer(binaryFilter.getField());
    this.operator = binaryFilter.getOperator();
    this.value = new UFLiteral(binaryFilter.getValue());
  }

  private UFBinaryFilter(Builder builder) {
    super(builder);
    this.field = builder.field;
    this.operator = builder.operator;
    this.value = builder.value;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder extends UFFilter.Builder {
    private UFFieldPointer field;
    private BinaryFilterVariable.BinaryOperator operator;
    private UFLiteral value;

    public Builder field(UFFieldPointer field) {
      this.field = field;
      return this;
    }

    public Builder operator(BinaryFilterVariable.BinaryOperator operator) {
      this.operator = operator;
      return this;
    }

    public Builder value(UFLiteral value) {
      this.value = value;
      return this;
    }

    /** Call the private constructor. */
    @Override
    public UFBinaryFilter build() {
      return new UFBinaryFilter(this);
    }
  }

  /** Deserialize to the internal representation of the table filter. */
  @Override
  public BinaryFilter deserializeToInternal(TablePointer tablePointer) {
    return BinaryFilter.fromSerialized(this, tablePointer);
  }

  public UFFieldPointer getField() {
    return field;
  }

  public BinaryFilterVariable.BinaryOperator getOperator() {
    return operator;
  }

  public UFLiteral getValue() {
    return value;
  }
}
