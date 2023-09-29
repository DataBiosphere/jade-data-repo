package bio.terra.tanagra.underlay;

import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Relationship {
  private static final Logger LOGGER = LoggerFactory.getLogger(Relationship.class);
  private final String name;
  private final Entity entityA;
  private final Entity entityB;
  private final List<RelationshipField> fields;

  private RelationshipMapping sourceMapping;
  private RelationshipMapping indexMapping;
  private EntityGroup entityGroup;

  public Relationship(String name, Entity entityA, Entity entityB, List<RelationshipField> fields) {
    this.name = name;
    this.entityA = entityA;
    this.entityB = entityB;
    this.fields = fields;
  }

  public void initialize(
      RelationshipMapping sourceMapping,
      RelationshipMapping indexMapping,
      EntityGroup entityGroup) {
    this.sourceMapping = sourceMapping;
    this.indexMapping = indexMapping;
    this.entityGroup = entityGroup;
    sourceMapping.initialize(this);
    indexMapping.initialize(this);
    fields.forEach(field -> field.initialize(this));
  }

  public String getName() {
    return name;
  }

  public Entity getRelatedEntity(Entity entity) {
    return entityA.equals(entity) ? entityB : entityA;
  }

  public Entity getEntityA() {
    return entityA;
  }

  public Entity getEntityB() {
    return entityB;
  }

  public List<RelationshipField> getFields() {
    return Collections.unmodifiableList(fields);
  }

  public RelationshipField getField(
      RelationshipField.Type type, Entity entity, Hierarchy hierarchy) {
    LOGGER.info(
        "get relationship field: {}, {}, {}",
        type,
        entity.getName(),
        hierarchy == null ? "null" : hierarchy.getName());
    return fields.stream()
        .filter(field -> field.matches(type, entity, hierarchy))
        .findFirst()
        .orElseThrow();
  }

  public EntityGroup getEntityGroup() {
    return entityGroup;
  }

  public boolean includesEntity(Entity entity) {
    return entityA.equals(entity) || entityB.equals(entity);
  }

  public RelationshipMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE == mappingType ? sourceMapping : indexMapping;
  }
}
