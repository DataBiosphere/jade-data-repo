package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.serialization.UFEntityMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class EntityMapping {
  private final TablePointer tablePointer;
  private Entity entity;
  private final Underlay.MappingType mappingType;

  private EntityMapping(TablePointer tablePointer, Underlay.MappingType mappingType) {
    this.tablePointer = tablePointer;
    this.mappingType = mappingType;
  }

  public void initialize(Entity entity) {
    this.entity = entity;
  }

  public static EntityMapping fromSerialized(
      UFEntityMapping serialized,
      Map<String, DataPointer> dataPointers,
      String entityName,
      Underlay.MappingType mappingType) {
    if (serialized.getDataPointer() == null || serialized.getDataPointer().isEmpty()) {
      throw new InvalidConfigException("No Data Pointer defined");
    }
    if (!dataPointers.containsKey(serialized.getDataPointer())) {
      throw new InvalidConfigException("Data Pointer not found: " + serialized.getDataPointer());
    }
    DataPointer dataPointer = dataPointers.get(serialized.getDataPointer());

    // if the table is defined, then deserialize it
    // otherwise generate a default table pointer: a table with the same name as the entity
    TablePointer tablePointer =
        serialized.getTablePointer() != null
            ? TablePointer.fromSerialized(serialized.getTablePointer(), dataPointer)
            : TablePointer.fromTableName(entityName, dataPointer);

    return new EntityMapping(tablePointer, mappingType);
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
    return new Query.Builder().select(List.of(idFieldVar)).tables(tables).build();
  }

  public Query queryAllAttributes() {
    List<TableVariable> tables = new ArrayList<>();
    TableVariable primaryTable = TableVariable.forPrimary(tablePointer);
    tables.add(primaryTable);

    List<FieldVariable> select = new ArrayList<>();
    getEntity().getAttributes().stream()
        .forEach(
            attribute ->
                select.addAll(
                    attribute.getMapping(mappingType).buildFieldVariables(primaryTable, tables)));
    return new Query.Builder().select(select).tables(tables).build();
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
