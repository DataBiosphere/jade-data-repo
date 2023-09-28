package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.SystemException;
import java.util.Collections;
import java.util.Map;

public final class Underlay {
  public enum MappingType {
    SOURCE,
    INDEX
  }

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
            entityGroup -> type == entityGroup.getType() && entityGroup.includesEntity(entity))
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
