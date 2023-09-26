package bio.terra.tanagra.serialization;

import bio.terra.tanagra.underlay.RollupInformation;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of rollup information.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFRollupInformation.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFRollupInformation {
  private final UFTablePointer table;
  private final UFFieldPointer id;
  private final UFFieldPointer count;
  private final UFFieldPointer displayHints;

  public UFRollupInformation(RollupInformation rollupInformation) {
    this.table = new UFTablePointer(rollupInformation.getTable());
    this.id = new UFFieldPointer(rollupInformation.getId());
    this.count = new UFFieldPointer(rollupInformation.getCount());
    this.displayHints = new UFFieldPointer(rollupInformation.getDisplayHints());
  }

  private UFRollupInformation(Builder builder) {
    this.table = builder.table;
    this.id = builder.id;
    this.count = builder.count;
    this.displayHints = builder.displayHints;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFTablePointer table;
    private UFFieldPointer id;
    private UFFieldPointer count;
    private UFFieldPointer displayHints;

    public Builder table(UFTablePointer table) {
      this.table = table;
      return this;
    }

    public Builder id(UFFieldPointer id) {
      this.id = id;
      return this;
    }

    public Builder count(UFFieldPointer count) {
      this.count = count;
      return this;
    }

    public Builder displayHints(UFFieldPointer displayHints) {
      this.displayHints = displayHints;
      return this;
    }

    public UFRollupInformation build() {
      return new UFRollupInformation(this);
    }
  }

  public UFTablePointer getTable() {
    return table;
  }

  public UFFieldPointer getId() {
    return id;
  }

  public UFFieldPointer getCount() {
    return count;
  }

  public UFFieldPointer getDisplayHints() {
    return displayHints;
  }
}
