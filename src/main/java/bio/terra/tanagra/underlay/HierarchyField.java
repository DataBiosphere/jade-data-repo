package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TableVariable;
import java.util.List;

public abstract class HierarchyField {
  public enum Type {
    IS_MEMBER(HierarchyMapping.IS_MEMBER_FIELD_NAME),
    PATH(HierarchyMapping.PATH_FIELD_NAME),
    NUM_CHILDREN(HierarchyMapping.NUM_CHILDREN_FIELD_NAME),
    IS_ROOT(HierarchyMapping.IS_ROOT_FIELD_NAME);

    final String suffix;

    Type(String suffix) {
      this.suffix = suffix;
    }
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
    return Query.TANAGRA_FIELD_PREFIX + hierarchyName + "_" + fieldType.suffix;
  }

  public Hierarchy getHierarchy() {
    return hierarchy;
  }
}
