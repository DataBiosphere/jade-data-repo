package bio.terra.tanagra.underlay;

import static bio.terra.tanagra.underlay.Entity.ENTITY_DIRECTORY_NAME;
import static bio.terra.tanagra.underlay.EntityGroup.ENTITY_GROUP_DIRECTORY_NAME;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.serialization.UFEntity;
import bio.terra.tanagra.serialization.UFEntityGroup;
import bio.terra.tanagra.serialization.UFUnderlay;
import bio.terra.tanagra.utils.FileIO;
import bio.terra.tanagra.utils.FileUtils;
import bio.terra.tanagra.utils.JacksonMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class Underlay {
  public enum MappingType {
    SOURCE,
    INDEX
  }

  public static final String OUTPUT_UNDERLAY_FILE_EXTENSION = ".json";
  private static final String UI_CONFIG_DIRECTORY_NAME = "ui";

  private final String name;
  private final Map<String, DataPointer> dataPointers;
  private final Map<String, Entity> entities;
  private final String primaryEntityName;
  private final Map<String, EntityGroup> entityGroups;
  private final String uiConfig;
  private final Map<String, String> metadata;

  private Underlay(
      String name,
      Map<String, DataPointer> dataPointers,
      Map<String, Entity> entities,
      String primaryEntityName,
      Map<String, EntityGroup> entityGroups,
      String uiConfig,
      Map<String, String> metadata) {
    this.name = name;
    this.dataPointers = dataPointers;
    this.entities = entities;
    this.primaryEntityName = primaryEntityName;
    this.entityGroups = entityGroups;
    this.uiConfig = uiConfig;
    this.metadata = metadata;
  }

  public static Underlay fromJSON(String underlayFileName) throws IOException {
    // read in the top-level underlay file
    Path underlayFilePath = FileIO.getInputParentDir().resolve(underlayFileName);
    UFUnderlay serialized =
        JacksonMapper.readFileIntoJavaObject(
            FileIO.getGetFileInputStreamFunction().apply(underlayFilePath), UFUnderlay.class);

    // deserialize data pointers
    if (serialized.getDataPointers() == null || serialized.getDataPointers().size() == 0) {
      throw new InvalidConfigException("No DataPointer defined");
    }
    Map<String, DataPointer> dataPointers = new HashMap<>();
    serialized
        .getDataPointers()
        .forEach(dps -> dataPointers.put(dps.getName(), dps.deserializeToInternal()));

    // deserialize entities
    if (serialized.getEntities() == null || serialized.getEntities().size() == 0) {
      throw new InvalidConfigException("No Entity defined");
    }
    Map<String, Entity> entities = new HashMap<>();
    for (String entityFile : serialized.getEntities()) {
      Entity entity = Entity.fromJSON(entityFile, dataPointers);
      entities.put(entity.getName(), entity);
    }

    String primaryEntity = serialized.getPrimaryEntity();
    if (primaryEntity == null || primaryEntity.isEmpty()) {
      throw new InvalidConfigException("No primary Entity defined");
    }
    if (!entities.containsKey(primaryEntity)) {
      throw new InvalidConfigException("Primary Entity not found in the set of Entities");
    }

    // deserialize entity groups
    Map<String, EntityGroup> entityGroups = new HashMap<>();
    if (serialized.getEntityGroups() != null) {
      for (String entityGroupFile : serialized.getEntityGroups()) {
        EntityGroup entityGroup =
            EntityGroup.fromJSON(entityGroupFile, dataPointers, entities, primaryEntity);
        entityGroups.put(entityGroup.getName(), entityGroup);
      }
    }

    String uiConfig = serialized.getUiConfig();
    if (uiConfig == null && serialized.getUiConfigFile() != null) {
      // read in UI config from file
      Path uiConfigFilePath =
          FileIO.getInputParentDir()
              .resolve(UI_CONFIG_DIRECTORY_NAME)
              .resolve(serialized.getUiConfigFile());
      uiConfig =
          FileUtils.readStringFromFile(
              FileIO.getGetFileInputStreamFunction().apply(uiConfigFilePath));
    }
    Map<String, String> metadata =
        serialized.getMetadata() != null ? serialized.getMetadata() : new HashMap<>();

    Underlay underlay =
        new Underlay(
            serialized.getName(),
            dataPointers,
            entities,
            primaryEntity,
            entityGroups,
            uiConfig,
            metadata);

    for (Entity entity : underlay.getEntities().values()) {
      entity.initialize(underlay);
    }

    return underlay;
  }

  /** Convert the internal objects, now expanded, back to POJOs. Write out the expanded POJOs. */
  public void serializeAndWriteToFile() throws IOException {
    UFUnderlay expandedUnderlay = new UFUnderlay(this);
    List<UFEntity> expandedEntities =
        getEntities().values().stream().map(UFEntity::new).collect(Collectors.toList());
    List<UFEntityGroup> expandedEntityGroups =
        getEntityGroups().values().stream()
            .map(EntityGroup::serialize)
            .collect(Collectors.toList());

    // Write out the underlay POJO to the top-level directory.
    Path underlayPath =
        FileIO.getOutputParentDir()
            .resolve(expandedUnderlay.getName() + OUTPUT_UNDERLAY_FILE_EXTENSION);
    JacksonMapper.writeJavaObjectToFile(underlayPath, expandedUnderlay);

    // Write out the entity POJOs to the entity/ sub-directory.
    Path entitySubDir = FileIO.getOutputParentDir().resolve(ENTITY_DIRECTORY_NAME);
    for (UFEntity expandedEntity : expandedEntities) {
      JacksonMapper.writeJavaObjectToFile(
          entitySubDir.resolve(expandedEntity.getName() + OUTPUT_UNDERLAY_FILE_EXTENSION),
          expandedEntity);
    }

    // Write out the entity group POJOs to the entity_group/ sub-directory.
    Path entityGroupSubDir = FileIO.getOutputParentDir().resolve(ENTITY_GROUP_DIRECTORY_NAME);
    for (UFEntityGroup expandedEntityGroup : expandedEntityGroups) {
      JacksonMapper.writeJavaObjectToFile(
          entityGroupSubDir.resolve(expandedEntityGroup.getName() + OUTPUT_UNDERLAY_FILE_EXTENSION),
          expandedEntityGroup);
    }
  }

  public String getName() {
    return name;
  }

  public Map<String, DataPointer> getDataPointers() {
    return Collections.unmodifiableMap(dataPointers);
  }

  public Map<String, Entity> getEntities() {
    return Collections.unmodifiableMap(entities);
  }

  public Entity getPrimaryEntity() {
    return entities.get(primaryEntityName);
  }

  public Entity getEntity(String name) {
    if (!entities.containsKey(name)) {
      throw new SystemException("Entity not found: " + name);
    }
    return entities.get(name);
  }

  public Map<String, EntityGroup> getEntityGroups() {
    return Collections.unmodifiableMap(entityGroups);
  }

  public EntityGroup getEntityGroup(String name) {
    if (!entityGroups.containsKey(name)) {
      throw new SystemException("Entity group not found: " + name);
    }
    return entityGroups.get(name);
  }

  public EntityGroup getEntityGroup(EntityGroup.Type type, Entity entity) {
    return entityGroups.values().stream()
        .filter(
            entityGroup -> type.equals(entityGroup.getType()) && entityGroup.includesEntity(entity))
        .findFirst()
        .get();
  }

  public String getUIConfig() {
    return uiConfig;
  }

  public Map<String, String> getMetadata() {
    return Collections.unmodifiableMap(metadata);
  }
}
