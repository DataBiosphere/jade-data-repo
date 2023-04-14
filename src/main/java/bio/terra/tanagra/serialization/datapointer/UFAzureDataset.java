package bio.terra.tanagra.serialization.datapointer;

import bio.terra.tanagra.serialization.UFDataPointer;
import bio.terra.tanagra.underlay.datapointer.AzureDataset;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.UUID;

/**
 * External representation of a data pointer to a BigQuery dataset.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFAzureDataset.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFAzureDataset extends UFDataPointer {
  private final UUID datasetId;
  private final String datasetName;
  private final String userName;

  public UFAzureDataset(AzureDataset dataPointer) {
    super(dataPointer);
    this.datasetId = dataPointer.getDatasetId();
    this.datasetName = dataPointer.getDatasetName();
    this.userName = dataPointer.getUserName();
  }

  private UFAzureDataset(Builder builder) {
    super(builder);
    this.datasetId = builder.datasetId;
    this.datasetName = builder.datasetName;
    this.userName = builder.userName;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder extends UFDataPointer.Builder {
    private UUID datasetId;
    private String datasetName;
    private String userName;

    public Builder datasetId(UUID datasetId) {
      this.datasetId = datasetId;
      return this;
    }

    public Builder datasetName(String datasetName) {
      this.datasetName = datasetName;
      return this;
    }

    public Builder userName(String userName) {
      this.userName = userName;
      return this;
    }

    /** Call the private constructor. */
    @Override
    public UFAzureDataset build() {
      return new UFAzureDataset(this);
    }
  }

  /** Deserialize to the internal representation of the data pointer. */
  @Override
  public AzureDataset deserializeToInternal() {
    return new AzureDataset(getName(), datasetId, datasetName, userName);
  }

  public UUID getDatasetId() {
    return datasetId;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public String getUserName() {
    return userName;
  }
}
