package bio.terra.tanagra.serialization;

import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.underlay.Attribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of an entity attribute.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFAttribute.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFAttribute {
  private final Attribute.Type type;
  private final String name;
  private final Literal.DataType dataType;
  private final UFDisplayHint displayHint;

  public UFAttribute(Attribute attribute) {
    this.type = attribute.getType();
    this.name = attribute.getName();
    this.dataType = attribute.getDataType();
    this.displayHint =
        attribute.getDisplayHint() == null ? null : attribute.getDisplayHint().serialize();
  }

  protected UFAttribute(Builder builder) {
    this.type = builder.type;
    this.name = builder.name;
    this.dataType = builder.dataType;
    this.displayHint = builder.displayHint;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private Attribute.Type type;
    private String name;
    private Literal.DataType dataType;
    private UFDisplayHint displayHint;

    public Builder type(Attribute.Type type) {
      this.type = type;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder dataType(Literal.DataType dataType) {
      this.dataType = dataType;
      return this;
    }

    public Builder displayHint(UFDisplayHint displayHint) {
      this.displayHint = displayHint;
      return this;
    }

    /** Call the private constructor. */
    public UFAttribute build() {
      return new UFAttribute(this);
    }
  }

  public Attribute.Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public Literal.DataType getDataType() {
    return dataType;
  }

  public UFDisplayHint getDisplayHint() {
    return displayHint;
  }
}
