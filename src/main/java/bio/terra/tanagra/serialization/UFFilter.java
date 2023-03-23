package bio.terra.tanagra.serialization;

import bio.terra.tanagra.query.Filter;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.serialization.filter.UFBinaryFilter;
import bio.terra.tanagra.serialization.filter.UFBooleanAndOrFilter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a table filter.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = UFBinaryFilter.class, name = "BINARY"),
  @JsonSubTypes.Type(value = UFBooleanAndOrFilter.class, name = "BOOLEAN_AND_OR")
})
@JsonDeserialize(builder = UFFilter.Builder.class)
public abstract class UFFilter {
  private final Filter.Type type;

  public UFFilter(Filter filter) {
    this.type = filter.getType();
  }

  protected UFFilter(Builder builder) {
    this.type = builder.type;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public abstract static class Builder {
    private Filter.Type type;

    public Builder type(Filter.Type type) {
      this.type = type;
      return this;
    }

    /** Call the private constructor. */
    public abstract UFFilter build();
  }

  /** Deserialize to the internal representation of the table filter. */
  public abstract Filter deserializeToInternal(TablePointer tablePointer);

  public Filter.Type getType() {
    return type;
  }
}
