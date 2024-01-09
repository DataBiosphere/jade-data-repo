package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.serialization.UFEntityGroupMapping;
import java.util.Map;

public final class EntityGroupMapping {
  private final DataPointer dataPointer;
  private final Underlay.MappingType mappingType;
  private EntityGroup entityGroup;

  private EntityGroupMapping(DataPointer dataPointer, Underlay.MappingType mappingType) {
    this.dataPointer = dataPointer;
    this.mappingType = mappingType;
  }

  public void initialize(EntityGroup entityGroup) {
    this.entityGroup = entityGroup;
  }

  public static EntityGroupMapping fromSerialized(
      UFEntityGroupMapping serialized,
      Map<String, DataPointer> dataPointers,
      Underlay.MappingType mappingType) {
    if (serialized.getDataPointer() == null || serialized.getDataPointer().isEmpty()) {
      throw new InvalidConfigException("No Data Pointer defined");
    }
    if (!dataPointers.containsKey(serialized.getDataPointer())) {
      throw new InvalidConfigException("Data Pointer not found: " + serialized.getDataPointer());
    }
    DataPointer dataPointer = dataPointers.get(serialized.getDataPointer());

    return new EntityGroupMapping(dataPointer, mappingType);
  }

  public DataPointer getDataPointer() {
    return dataPointer;
  }

  public Underlay.MappingType getMappingType() {
    return mappingType;
  }

  public EntityGroup getEntityGroup() {
    return entityGroup;
  }
}
