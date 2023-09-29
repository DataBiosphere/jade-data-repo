package bio.terra.tanagra.underlay;

public final class EntityGroupMapping {
  private final DataPointer dataPointer;
  private final Underlay.MappingType mappingType;
  private EntityGroup entityGroup;

  public EntityGroupMapping(DataPointer dataPointer, Underlay.MappingType mappingType) {
    this.dataPointer = dataPointer;
    this.mappingType = mappingType;
  }

  public void initialize(EntityGroup entityGroup) {
    this.entityGroup = entityGroup;
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
