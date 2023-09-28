package bio.terra.tanagra.underlay;

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

  protected final String name;
  protected final Map<String, Relationship> relationships;
  protected final EntityGroupMapping sourceDataMapping;
  protected final EntityGroupMapping indexDataMapping;

  protected EntityGroup(String name, Map<String, Relationship> relationships, EntityGroupMapping sourceDataMapping, EntityGroupMapping indexDataMapping) {
    this.name = name;
    this.relationships = relationships;
    this.sourceDataMapping = sourceDataMapping;
    this.indexDataMapping = indexDataMapping;
  }

  public abstract Type getType();

  public abstract Map<String, Entity> getEntityMap();

  public boolean includesEntity(Entity entity) {
    return getEntityMap().values().stream().anyMatch(entity::equals);
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
    return Underlay.MappingType.SOURCE == mappingType ? sourceDataMapping : indexDataMapping;
  }

  public List<AuxiliaryData> getAuxiliaryData() {
    return Collections.emptyList();
  }

  protected abstract static class Builder {
    protected String name;
    protected Map<String, Relationship> relationships;
    protected EntityGroupMapping sourceDataMapping;
    protected EntityGroupMapping indexDataMapping;

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
