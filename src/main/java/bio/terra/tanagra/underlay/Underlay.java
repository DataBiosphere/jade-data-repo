package bio.terra.tanagra.underlay;

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

  public Underlay(
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
    return Map.copyOf(dataPointers);
  }

  public Map<String, Entity> getEntities() {
    return Map.copyOf(entities);
  }

  public Entity getPrimaryEntity() {
    return entities.get(primaryEntityName);
  }

  public Entity getEntity(String name) {
    Entity entity = entities.get(name);
    if (entity == null) {
      throw new IllegalArgumentException("Entity not found: " + name);
    }
    return entity;
  }

  public Map<String, EntityGroup> getEntityGroups() {
    return Map.copyOf(entityGroups);
  }

  public EntityGroup getEntityGroup(String name) {
    EntityGroup entityGroup = entityGroups.get(name);
    if (entityGroup == null) {
      throw new IllegalArgumentException("Entity group not found: " + name);
    }
    return entityGroup;
  }

  public EntityGroup getEntityGroup(EntityGroup.Type type, Entity entity) {
    return entityGroups.values().stream()
        .filter(entityGroup -> type == entityGroup.getType() && entityGroup.includesEntity(entity))
        .findFirst()
        .orElseThrow();
  }

  public String getUIConfig() {
    return uiConfig;
  }

  public Map<String, String> getMetadata() {
    return Map.copyOf(metadata);
  }
}
