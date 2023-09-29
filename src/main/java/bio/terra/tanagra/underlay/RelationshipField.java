package bio.terra.tanagra.underlay;

import static bio.terra.tanagra.query.Query.TANAGRA_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.RelationshipMapping.COUNT_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.RelationshipMapping.DISPLAY_HINTS_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.RelationshipMapping.NO_HIERARCHY_KEY;

import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.TableVariable;
import java.util.List;

public abstract class RelationshipField {

  public enum Type {
    COUNT(COUNT_FIELD_PREFIX),
    DISPLAY_HINTS(DISPLAY_HINTS_FIELD_PREFIX);
    final String prefix;

    Type(String prefix) {
      this.prefix = prefix;
    }
  }

  private final Type type;
  private final Entity entity;
  private final Hierarchy hierarchy;
  private Relationship relationship;

  protected RelationshipField(Type type, Entity entity) {
    this(type, entity, null);
  }

  protected RelationshipField(Type type, Entity entity, Hierarchy hierarchy) {
    this.type = type;
    this.entity = entity;
    this.hierarchy = hierarchy;
  }

  public void initialize(Relationship relationship) {
    this.relationship = relationship;
  }

  public abstract ColumnSchema buildColumnSchema();

  public abstract FieldVariable buildFieldVariableFromEntityId(
      RelationshipMapping relationshipMapping,
      TableVariable entityTableVar,
      List<TableVariable> tableVars);

  public String getFieldAlias() {
    return getFieldAlias(type, relationship.getRelatedEntity(entity), hierarchy);
  }

  public static String getFieldAlias(Type fieldType, Entity relatedEntity, Hierarchy hierarchy) {
    return TANAGRA_FIELD_PREFIX
        + fieldType.prefix
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

  public boolean matches(Type otherType, Entity otherEntity, Hierarchy otherHierarchy) {
    return type == otherType
        && entity.equals(otherEntity)
        && ((otherHierarchy == null && hierarchy == null)
            || (otherHierarchy != null && otherHierarchy.equals(hierarchy)));
  }
}
