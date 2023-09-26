package bio.terra.tanagra.underlay;

import static bio.terra.tanagra.query.Query.TANAGRA_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.RelationshipMapping.COUNT_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.RelationshipMapping.DISPLAY_HINTS_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.RelationshipMapping.NO_HIERARCHY_KEY;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.TableVariable;
import java.util.List;

public abstract class RelationshipField {
  public enum Type {
    COUNT,
    DISPLAY_HINTS
  }

  private final Entity entity;
  private final Hierarchy hierarchy;
  private Relationship relationship;

  protected RelationshipField(Entity entity) {
    this.entity = entity;
    this.hierarchy = null;
  }

  protected RelationshipField(Entity entity, Hierarchy hierarchy) {
    this.entity = entity;
    this.hierarchy = hierarchy;
  }

  public void initialize(Relationship relationship) {
    this.relationship = relationship;
  }

  public abstract Type getType();

  public abstract ColumnSchema buildColumnSchema();

  public abstract FieldVariable buildFieldVariableFromEntityId(
      RelationshipMapping relationshipMapping,
      TableVariable entityTableVar,
      List<TableVariable> tableVars);

  public String getFieldAlias() {
    return getFieldAlias(getType(), relationship.getRelatedEntity(entity), hierarchy);
  }

  public static String getFieldAlias(Type fieldType, Entity relatedEntity, Hierarchy hierarchy) {
    String fieldNamePrefix;
    switch (fieldType) {
      case COUNT:
        fieldNamePrefix = COUNT_FIELD_PREFIX;
        break;
      case DISPLAY_HINTS:
        fieldNamePrefix = DISPLAY_HINTS_FIELD_PREFIX;
        break;
      default:
        throw new SystemException("Unknown relationship field type: " + fieldType);
    }
    return TANAGRA_FIELD_PREFIX
        + fieldNamePrefix
        + relatedEntity.getName()
        + (hierarchy == null ? "" : ("_" + hierarchy.getName()));
  }

  public Entity getEntity() {
    return entity;
  }

  public Hierarchy getHierarchy() {
    return hierarchy;
  }

  public String getHierarchyName() {
    return hierarchy == null ? NO_HIERARCHY_KEY : hierarchy.getName();
  }

  public Relationship getRelationship() {
    return relationship;
  }

  public String getName() {
    return relationship.getName() + "_" + entity.getName() + "_" + getHierarchyName();
  }
}
