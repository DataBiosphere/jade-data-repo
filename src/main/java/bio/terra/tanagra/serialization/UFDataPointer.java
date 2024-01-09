package bio.terra.tanagra.serialization;

import bio.terra.tanagra.serialization.datapointer.UFAzureDataset;
import bio.terra.tanagra.serialization.datapointer.UFBigQueryDataset;
import bio.terra.tanagra.underlay.DataPointer;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a data pointer configuration.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = UFBigQueryDataset.class, name = "BQ_DATASET"),
  @JsonSubTypes.Type(value = UFAzureDataset.class, name = "AZURE_DATASET")
})
@JsonDeserialize(builder = UFDataPointer.Builder.class)
public abstract class UFDataPointer {
  private final DataPointer.Type type;
  private final String name;

  protected UFDataPointer(DataPointer dataPointer) {
    this.type = dataPointer.getType();
    this.name = dataPointer.getName();
  }

  protected UFDataPointer(Builder builder) {
    this.type = builder.type;
    this.name = builder.name;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public abstract static class Builder {
    private DataPointer.Type type;
    private String name;

    public Builder type(DataPointer.Type type) {
      this.type = type;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    /** Call the private constructor. */
    public abstract UFDataPointer build();
  }

  /** Deserialize to the internal representation of the data pointer. */
  public abstract DataPointer deserializeToInternal();

  public DataPointer.Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }
}
