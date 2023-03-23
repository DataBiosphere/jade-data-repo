package bio.terra.tanagra.serialization;

import bio.terra.tanagra.underlay.AuxiliaryDataMapping;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.HashMap;
import java.util.Map;

/**
 * External representation of an auxiliary data mapping.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFAuxiliaryDataMapping.Builder.class)
public class UFAuxiliaryDataMapping {
  private final UFTablePointer tablePointer;
  private final Map<String, UFFieldPointer> fieldPointers;

  public UFAuxiliaryDataMapping(AuxiliaryDataMapping auxiliaryDataMapping) {
    this.tablePointer = new UFTablePointer(auxiliaryDataMapping.getTablePointer());
    Map<String, UFFieldPointer> fieldPointers = new HashMap<>();
    auxiliaryDataMapping.getFieldPointers().entrySet().stream()
        .forEach(
            fp -> {
              fieldPointers.put(fp.getKey(), new UFFieldPointer(fp.getValue()));
            });
    this.fieldPointers = fieldPointers;
  }

  private UFAuxiliaryDataMapping(Builder builder) {
    this.tablePointer = builder.tablePointer;
    this.fieldPointers = builder.fieldPointers;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private UFTablePointer tablePointer;
    private Map<String, UFFieldPointer> fieldPointers;

    public Builder tablePointer(UFTablePointer tablePointer) {
      this.tablePointer = tablePointer;
      return this;
    }

    public Builder fieldPointers(Map<String, UFFieldPointer> fieldPointers) {
      this.fieldPointers = fieldPointers;
      return this;
    }

    /** Call the private constructor. */
    public UFAuxiliaryDataMapping build() {
      return new UFAuxiliaryDataMapping(this);
    }
  }

  public UFTablePointer getTablePointer() {
    return tablePointer;
  }

  public Map<String, UFFieldPointer> getFieldPointers() {
    return fieldPointers;
  }
}
