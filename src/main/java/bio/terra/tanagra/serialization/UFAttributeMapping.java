package bio.terra.tanagra.serialization;

import bio.terra.tanagra.underlay.AttributeMapping;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a mapping between an entity attribute and the underlying data.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFAttributeMapping.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFAttributeMapping {
  private final UFFieldPointer value;
  private final UFFieldPointer display;

  public UFAttributeMapping(AttributeMapping attributeMapping) {
    this.value = new UFFieldPointer(attributeMapping.getValue());
    this.display =
        attributeMapping.hasDisplay() ? new UFFieldPointer(attributeMapping.getDisplay()) : null;
  }

  protected UFAttributeMapping(Builder builder) {
    this.value = builder.value;
    this.display = builder.display;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFFieldPointer value;
    private UFFieldPointer display;

    public Builder value(UFFieldPointer value) {
      this.value = value;
      return this;
    }

    public Builder display(UFFieldPointer display) {
      this.display = display;
      return this;
    }

    /** Call the private constructor. */
    public UFAttributeMapping build() {
      return new UFAttributeMapping(this);
    }
  }

  public UFFieldPointer getValue() {
    return value;
  }

  public UFFieldPointer getDisplay() {
    return display;
  }
}
