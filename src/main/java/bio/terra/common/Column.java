package bio.terra.common;

import bio.terra.model.ColumnModel;
import bio.terra.model.TableDataType;
import java.util.Objects;
import java.util.UUID;

public class Column {
  private UUID id;
  private Table table;
  private String name;
  private TableDataType type;
  private boolean arrayOf;
  private boolean required;

  public Column() {}

  public Column(Column fromColumn) {
    this.id = fromColumn.id;
    this.table = fromColumn.table;
    this.name = fromColumn.name;
    this.type = fromColumn.type;
    this.arrayOf = fromColumn.arrayOf;
    this.required = fromColumn.required;
  }

  public static Column toSnapshotColumn(Column datasetColumn) {
    return new Column()
        .name(datasetColumn.getName())
        .type(datasetColumn.getType())
        .arrayOf(datasetColumn.isArrayOf())
        .required(datasetColumn.isRequired());
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
            .arrayOf(datasetColumn.isArrayOf())
            .required(datasetColumn.isRequired());
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

  public boolean isTextType() {
    return switch (this.type) {
      case TEXT, STRING, DIRREF, FILEREF -> true;
      default -> false;
    };
  }

  public boolean isDoubleType() {
    return switch (this.type) {
      case NUMERIC, FLOAT, FLOAT64 -> true;
      default -> false;
    };
  }

  public boolean isIntType() {
    return switch (this.type) {
      case INT64, INTEGER -> true;
      default -> false;
    };
  }

  public boolean isArrayOf() {
    return arrayOf;
  }

  public Column arrayOf(boolean arrayOf) {
    this.arrayOf = arrayOf;
    return this;
  }

  public boolean isRequired() {
    return required;
  }

  public Column required(boolean required) {
    this.required = required;
    return this;
  }

  public ColumnModel toColumnModel() {
    return new ColumnModel().name(name).datatype(type).arrayOf(arrayOf).required(required);
  }

  public boolean isFileOrDirRef() {
    return type == TableDataType.FILEREF || type == TableDataType.DIRREF;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column column = (Column) o;
    return arrayOf == column.arrayOf
        && required == column.required
        && id.equals(column.id)
        && Objects.equals(table, column.table)
        && Objects.equals(name, column.name)
        && type == column.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, table, name, type, arrayOf, required);
  }
}
