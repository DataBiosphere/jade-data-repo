package bio.terra.tanagra.serialization.displayhint;

import bio.terra.tanagra.serialization.UFLiteral;
import bio.terra.tanagra.underlay.ValueDisplay;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a value + display pair.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFValueDisplay.Builder.class)
public class UFValueDisplay {
  private final UFLiteral value;
  private final String display;

  public UFValueDisplay(ValueDisplay valueDisplay) {
    this.value = new UFLiteral(valueDisplay.getValue());
    this.display = valueDisplay.getDisplay();
  }

  private UFValueDisplay(Builder builder) {
    this.value = builder.value;
    this.display = builder.display;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFLiteral value;
    private String display;

    public Builder value(UFLiteral value) {
      this.value = value;
      return this;
    }

    public Builder display(String display) {
      this.display = display;
      return this;
    }

    /** Call the private constructor. */
    public UFValueDisplay build() {
      return new UFValueDisplay(this);
    }
  }

  public UFLiteral getValue() {
    return value;
  }

  public String getDisplay() {
    return display;
  }
}
