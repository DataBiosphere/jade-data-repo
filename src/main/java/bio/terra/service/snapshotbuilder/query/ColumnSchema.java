package bio.terra.service.snapshotbuilder.query;

/** The schema for a column in a {@link RowResult} describing the data in a column. */
public record ColumnSchema(String columnName, CellValue.SQLDataType sqlDataType) {}
