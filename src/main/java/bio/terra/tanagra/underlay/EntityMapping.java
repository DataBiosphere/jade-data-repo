package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import java.util.ArrayList;
import java.util.List;

public final class EntityMapping {
  private final TablePointer tablePointer;
  private Entity entity;
  private final Underlay.MappingType mappingType;

  public EntityMapping(TablePointer tablePointer, Underlay.MappingType mappingType) {
    this.tablePointer = tablePointer;
    this.mappingType = mappingType;
  }

  public void initialize(Entity entity) {
    this.entity = entity;
  }

  public Query queryIds(String alias) {
    List<TableVariable> tables = new ArrayList<>();
    TableVariable primaryTable = TableVariable.forPrimary(tablePointer);
    tables.add(primaryTable);

    FieldVariable idFieldVar =
        getEntity()
            .getIdAttribute()
            .getMapping(mappingType)
            .getValue()
            .buildVariable(primaryTable, tables, alias);
    return new Query(List.of(idFieldVar), tables);
  }

  public Query queryAllAttributes() {
    List<TableVariable> tables = new ArrayList<>();
    TableVariable primaryTable = TableVariable.forPrimary(tablePointer);
    tables.add(primaryTable);

    List<FieldVariable> select = new ArrayList<>();
    getEntity()
        .getAttributes()
        .forEach(
            attribute ->
                select.addAll(
                    attribute.getMapping(mappingType).buildFieldVariables(primaryTable, tables)));
    return new Query(select, tables);
  }

  public Underlay.MappingType getMappingType() {
    return mappingType;
  }

  public TablePointer getTablePointer() {
    return tablePointer;
  }

  public Entity getEntity() {
    return entity;
  }
}
