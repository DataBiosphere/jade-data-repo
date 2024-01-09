package bio.terra.tanagra.underlay;

import static bio.terra.tanagra.query.Query.TANAGRA_FIELD_PREFIX;
import static bio.terra.tanagra.underlay.HierarchyMapping.IS_MEMBER_FIELD_NAME;
import static bio.terra.tanagra.underlay.HierarchyMapping.IS_ROOT_FIELD_NAME;
import static bio.terra.tanagra.underlay.HierarchyMapping.NUM_CHILDREN_FIELD_NAME;
import static bio.terra.tanagra.underlay.HierarchyMapping.PATH_FIELD_NAME;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.TableVariable;
import java.util.List;

public abstract class HierarchyField {
  public enum Type {
    IS_MEMBER,
    PATH,
    NUM_CHILDREN,
    IS_ROOT
  }

  private Hierarchy hierarchy;

  public void initialize(Hierarchy hierarchy) {
    this.hierarchy = hierarchy;
  }

  public abstract Type getType();

  public abstract ColumnSchema buildColumnSchema();

  public abstract FieldVariable buildFieldVariableFromEntityId(
      HierarchyMapping hierarchyMapping,
      TableVariable entityTableVar,
      List<TableVariable> tableVars);

  public String getFieldAlias() {
    return getFieldAlias(getHierarchy().getName(), getType());
  }

  public static String getFieldAlias(String hierarchyName, Type fieldType) {
    String suffix;
    switch (fieldType) {
      case IS_MEMBER:
        suffix = IS_MEMBER_FIELD_NAME;
        break;
      case IS_ROOT:
        suffix = IS_ROOT_FIELD_NAME;
        break;
      case PATH:
        suffix = PATH_FIELD_NAME;
        break;
      case NUM_CHILDREN:
        suffix = NUM_CHILDREN_FIELD_NAME;
        break;
      default:
        throw new SystemException("Unknown hierarchy field type: " + fieldType);
    }
    return TANAGRA_FIELD_PREFIX + hierarchyName + "_" + suffix;
  }

  public Hierarchy getHierarchy() {
    return hierarchy;
  }
}
