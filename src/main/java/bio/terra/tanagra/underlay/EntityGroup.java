package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.serialization.UFAuxiliaryDataMapping;
import bio.terra.tanagra.serialization.UFEntityGroup;
import bio.terra.tanagra.serialization.UFRelationshipMapping;
import bio.terra.tanagra.utils.FileIO;
import bio.terra.tanagra.utils.JacksonMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class EntityGroup {
  /** Enum for the types of entity groups supported by Tanagra. */
  public enum Type {
    GROUP_ITEMS,
    CRITERIA_OCCURRENCE
  }

  public static final String ENTITY_GROUP_DIRECTORY_NAME = "entitygroup";

  protected String name;
  protected Map<String, Relationship> relationships;
  protected EntityGroupMapping sourceDataMapping;
  protected EntityGroupMapping indexDataMapping;

  protected EntityGroup(Builder builder) {
    this.name = builder.name;
    this.relationships = builder.relationships;
    this.sourceDataMapping = builder.sourceDataMapping;
    this.indexDataMapping = builder.indexDataMapping;
  }

  public static EntityGroup fromJSON(
      String entityGroupFileName,
      Map<String, DataPointer> dataPointers,
      Map<String, Entity> entities,
      String primaryEntityName)
      throws IOException {
    // Read in entity group file.
    Path entityGroupFilePath =
        FileIO.getInputParentDir()
            .resolve(ENTITY_GROUP_DIRECTORY_NAME)
            .resolve(entityGroupFileName);
    UFEntityGroup serialized =
        JacksonMapper.readFileIntoJavaObject(
            FileIO.getGetFileInputStreamFunction().apply(entityGroupFilePath), UFEntityGroup.class);
    return serialized.deserializeToInternal(dataPointers, entities, primaryEntityName);
  }

  protected static void deserializeRelationshipMappings(
      UFEntityGroup serialized, EntityGroup entityGroup) {
    // Source+index relationship mappings.
    if (serialized.getSourceDataMapping().getRelationshipMappings() == null) {
      return;
    }
    for (Relationship relationship : entityGroup.getRelationships().values()) {
      UFRelationshipMapping serializedSourceMapping =
          serialized.getSourceDataMapping().getRelationshipMappings().get(relationship.getName());
      DataPointer sourceDataPointer =
          entityGroup.getMapping(Underlay.MappingType.SOURCE).getDataPointer();
      if (serializedSourceMapping == null) {
        throw new InvalidConfigException(
            String.format(
                "%s: Relationship mapping %s is undefined",
                entityGroup.getName(), relationship.getName()));
      }
      RelationshipMapping sourceMapping =
          RelationshipMapping.fromSerialized(serializedSourceMapping, sourceDataPointer);

      DataPointer indexDataPointer =
          entityGroup.getMapping(Underlay.MappingType.INDEX).getDataPointer();
      Map<String, UFRelationshipMapping> indexRelationshipMappings =
          serialized.getIndexDataMapping().getRelationshipMappings();
      RelationshipMapping indexMapping =
          indexRelationshipMappings == null || indexRelationshipMappings.isEmpty()
              ? RelationshipMapping.defaultIndexMapping(indexDataPointer, relationship)
              : RelationshipMapping.fromSerialized(
                  serialized
                      .getIndexDataMapping()
                      .getRelationshipMappings()
                      .get(relationship.getName()),
                  indexDataPointer);

      relationship.initialize(sourceMapping, indexMapping, entityGroup);
    }
  }

  protected static void deserializeAuxiliaryDataMappings(
      UFEntityGroup serialized, EntityGroup entityGroup) {
    DataPointer sourceDataPointer =
        entityGroup.getMapping(Underlay.MappingType.SOURCE).getDataPointer();
    DataPointer indexDataPointer =
        entityGroup.getMapping(Underlay.MappingType.INDEX).getDataPointer();

    // Source+index auxiliary data mappings.
    for (AuxiliaryData auxData : entityGroup.getAuxiliaryData()) {
      Map<String, UFAuxiliaryDataMapping> sourceAuxDataMappings =
          serialized.getSourceDataMapping().getAuxiliaryDataMappings();
      AuxiliaryDataMapping sourceMapping =
          sourceAuxDataMappings == null || sourceAuxDataMappings.isEmpty()
              ? null
              : AuxiliaryDataMapping.fromSerialized(
                  serialized
                      .getSourceDataMapping()
                      .getAuxiliaryDataMappings()
                      .get(auxData.getName()),
                  sourceDataPointer,
                  auxData);

      Map<String, UFAuxiliaryDataMapping> indexAuxDataMappings =
          serialized.getIndexDataMapping().getAuxiliaryDataMappings();
      AuxiliaryDataMapping indexMapping =
          indexAuxDataMappings == null || indexAuxDataMappings.isEmpty()
              ? AuxiliaryDataMapping.defaultIndexMapping(
                  auxData, entityGroup.getName() + "_", indexDataPointer)
              : AuxiliaryDataMapping.fromSerialized(
                  serialized
                      .getIndexDataMapping()
                      .getAuxiliaryDataMappings()
                      .get(auxData.getName()),
                  indexDataPointer,
                  auxData);

      auxData.initialize(sourceMapping, indexMapping);
    }
  }

  public abstract Type getType();

  public abstract Map<String, Entity> getEntityMap();

  public boolean includesEntity(Entity entity) {
    return getEntityMap().values().stream().filter(e -> entity.equals(e)).findFirst().isPresent();
  }

  public String getName() {
    return name;
  }

  public Map<String, Relationship> getRelationships() {
    return Collections.unmodifiableMap(relationships);
  }

  public Optional<Relationship> getRelationship(Entity fromEntity, Entity toEntity) {
    for (Relationship relationship : relationships.values()) {
      if ((relationship.getEntityA().equals(fromEntity)
              && relationship.getEntityB().equals(toEntity))
          || (relationship.getEntityB().equals(fromEntity)
              && relationship.getEntityA().equals(toEntity))) {
        return Optional.of(relationship);
      }
    }
    return Optional.empty();
  }

  public EntityGroupMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE.equals(mappingType) ? sourceDataMapping : indexDataMapping;
  }

  public List<AuxiliaryData> getAuxiliaryData() {
    return Collections.emptyList();
  }

  public abstract UFEntityGroup serialize();

  protected abstract static class Builder {
    private String name;
    private Map<String, Relationship> relationships;
    private EntityGroupMapping sourceDataMapping;
    private EntityGroupMapping indexDataMapping;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder relationships(Map<String, Relationship> relationships) {
      this.relationships = relationships;
      return this;
    }

    public Builder sourceDataMapping(EntityGroupMapping sourceDataMapping) {
      this.sourceDataMapping = sourceDataMapping;
      return this;
    }

    public Builder indexDataMapping(EntityGroupMapping indexDataMapping) {
      this.indexDataMapping = indexDataMapping;
      return this;
    }

    public abstract EntityGroup build();
  }
}
