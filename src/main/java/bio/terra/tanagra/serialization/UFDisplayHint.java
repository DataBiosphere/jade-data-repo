package bio.terra.tanagra.serialization;

import bio.terra.tanagra.serialization.displayhint.UFEnumVals;
import bio.terra.tanagra.serialization.displayhint.UFNumericRange;
import bio.terra.tanagra.underlay.DisplayHint;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of an attribute display hint.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = UFEnumVals.class, name = "ENUM"),
  @JsonSubTypes.Type(value = UFNumericRange.class, name = "RANGE")
})
@JsonDeserialize(builder = UFDisplayHint.Builder.class)
public abstract class UFDisplayHint {
  private final DisplayHint.Type type;

  protected UFDisplayHint(DisplayHint displayHint) {
    this.type = displayHint.getType();
  }

  protected UFDisplayHint(Builder builder) {
    this.type = builder.type;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public abstract static class Builder {
    private DisplayHint.Type type;

    public Builder type(DisplayHint.Type type) {
      this.type = type;
      return this;
    }

    /** Call the private constructor. */
    public abstract UFDisplayHint build();
  }

  /** Deserialize to the internal representation of the display hint. */
  public abstract DisplayHint deserializeToInternal();

  public DisplayHint.Type getType() {
    return type;
  }
}
