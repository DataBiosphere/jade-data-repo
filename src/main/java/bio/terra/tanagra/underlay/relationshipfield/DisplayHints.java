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

public class DisplayHints extends RelationshipField {
  public DisplayHints(Entity entity) {
    super(entity);
  }

  public DisplayHints(Entity entity, Hierarchy hierarchy) {
    super(entity, hierarchy);
  }

  @Override
  public Type getType() {
    return Type.DISPLAY_HINTS;
  }

  @Override
  public ColumnSchema buildColumnSchema() {
    return new ColumnSchema(getFieldAlias(), CellValue.SQLDataType.STRING);
  }

  @Override
  public FieldVariable buildFieldVariableFromEntityId(
      RelationshipMapping relationshipMapping,
      TableVariable entityTableVar,
      List<TableVariable> tableVars) {
    return relationshipMapping
        .getRollupInfo(getEntity(), getHierarchy())
        .getDisplayHints()
        .buildVariable(entityTableVar, tableVars, getFieldAlias());
  }
}
