package bio.terra.tanagra.serialization.displayhint;

import bio.terra.tanagra.serialization.UFDisplayHint;
import bio.terra.tanagra.underlay.displayhint.NumericRange;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a numeric range display hint.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFNumericRange.Builder.class)
public class UFNumericRange extends UFDisplayHint {
  private final Double minVal;
  private final Double maxVal;

  public UFNumericRange(NumericRange displayHint) {
    super(displayHint);
    this.minVal = displayHint.getMinVal();
    this.maxVal = displayHint.getMaxVal();
  }

  private UFNumericRange(Builder builder) {
    super(builder);
    this.minVal = builder.minVal;
    this.maxVal = builder.maxVal;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder extends UFDisplayHint.Builder {
    private Double minVal;
    private Double maxVal;

    public Builder minVal(Double minVal) {
      this.minVal = minVal;
      return this;
    }

    public Builder maxVal(Double maxVal) {
      this.maxVal = maxVal;
      return this;
    }

    /** Call the private constructor. */
    @Override
    public UFNumericRange build() {
      return new UFNumericRange(this);
    }
  }

  /** Deserialize to the internal representation of the display hint. */
  @Override
  public NumericRange deserializeToInternal() {
    return NumericRange.fromSerialized(this);
  }

  public Double getMinVal() {
    return minVal;
  }

  public Double getMaxVal() {
    return maxVal;
  }
}
