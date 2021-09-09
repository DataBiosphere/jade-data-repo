package bio.terra.common;

import bio.terra.model.ColumnModel;
import bio.terra.model.TableDataType;
import java.util.UUID;

public class Column {
  private UUID id;
  private Table table;
  private String name;
  private TableDataType type;
  private boolean arrayOf;

  public Column() {}

  public Column(Column fromColumn) {
    this.id = fromColumn.id;
    this.table = fromColumn.table;
    this.name = fromColumn.name;
    this.type = fromColumn.type;
    this.arrayOf = fromColumn.arrayOf;
  }

  public static Column toSnapshotColumn(Column datasetColumn) {
    return new Column()
        .name(datasetColumn.getName())
        .type(datasetColumn.getType())
        .arrayOf(datasetColumn.isArrayOf());
  }

  public static SynapseColumn toSynapseColumn(Column datasetColumn) {
    return (SynapseColumn)
        new SynapseColumn()
            .synapseDataType(
                SynapseColumn.translateDataType(datasetColumn.getType(), datasetColumn.isArrayOf()))
            .requiresCollate(
                SynapseColumn.checkForCollateArgRequirement(
                    datasetColumn.getType(), datasetColumn.isArrayOf()))
            .requiresJSONCast(
                SynapseColumn.checkForJSONCastRequirement(
                    datasetColumn.getType(), datasetColumn.isArrayOf()))
            .name(datasetColumn.getName())
            .type(datasetColumn.getType())
            .arrayOf(datasetColumn.isArrayOf());
  }

  public UUID getId() {
    return id;
  }

  public Column id(UUID id) {
    this.id = id;
    return this;
  }

  public Table getTable() {
    return table;
  }

  public Column table(Table table) {
    this.table = table;
    return this;
  }

  public String getName() {
    return name;
  }

  public Column name(String name) {
    this.name = name;
    return this;
  }

  public TableDataType getType() {
    return type;
  }

  public Column type(TableDataType type) {
    this.type = type;
    return this;
  }

  public boolean isArrayOf() {
    return arrayOf;
  }

  public Column arrayOf(boolean arrayOf) {
    this.arrayOf = arrayOf;
    return this;
  }

  public ColumnModel toColumnModel() {
    return new ColumnModel().name(name).datatype(type).arrayOf(arrayOf);
  }

  public boolean isFileOrDirRef() {
    return type == TableDataType.FILEREF || type == TableDataType.DIRREF;
  }
}
