package bio.terra.tanagra.underlay.hierarchyfield;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.HierarchyMapping;
import java.util.List;

public class IsMember extends HierarchyField {
  @Override
  public Type getType() {
    return Type.IS_MEMBER;
  }

  @Override
  public ColumnSchema buildColumnSchema() {
    return new ColumnSchema(getFieldAlias(), CellValue.SQLDataType.BOOLEAN);
  }

  @Override
  public FieldVariable buildFieldVariableFromEntityId(
      HierarchyMapping hierarchyMapping,
      TableVariable entityTableVar,
      List<TableVariable> tableVars) {
    // Currently, this is a calculated field. IS_MEMBER means path IS NOT NULL.
    FieldPointer pathFieldPointer = hierarchyMapping.getPathField();

    return new FieldPointer.Builder()
        .tablePointer(pathFieldPointer.getTablePointer())
        .columnName(pathFieldPointer.getColumnName())
        .foreignTablePointer(pathFieldPointer.getForeignTablePointer())
        .foreignKeyColumnName(pathFieldPointer.getForeignKeyColumnName())
        .foreignColumnName(pathFieldPointer.getForeignColumnName())
        .sqlFunctionWrapper("(${fieldSql} IS NOT NULL)")
        .build()
        .buildVariable(entityTableVar, tableVars, getFieldAlias());
  }
}
