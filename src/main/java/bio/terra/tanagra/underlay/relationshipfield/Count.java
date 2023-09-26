package bio.terra.tanagra.underlay.relationshipfield;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.RelationshipField;
import bio.terra.tanagra.underlay.RelationshipMapping;
import java.util.List;

public class Count extends RelationshipField {
  public Count(Entity entity) {
    super(entity);
  }

  public Count(Entity entity, Hierarchy hierarchy) {
    super(entity, hierarchy);
  }

  @Override
  public Type getType() {
    return Type.COUNT;
  }

  @Override
  public ColumnSchema buildColumnSchema() {
    return new ColumnSchema(getFieldAlias(), CellValue.SQLDataType.INT64);
  }

  @Override
  public FieldVariable buildFieldVariableFromEntityId(
      RelationshipMapping relationshipMapping,
      TableVariable entityTableVar,
      List<TableVariable> tableVars) {
    return relationshipMapping
        .getRollupInfo(getEntity(), getHierarchy())
        .getCount()
        .buildVariable(entityTableVar, tableVars, getFieldAlias());
  }
}
