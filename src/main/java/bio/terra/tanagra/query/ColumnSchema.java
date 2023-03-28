package bio.terra.tanagra.query;

/** The schema for a column in a {@link RowResult} describing the data in a column. */
public class ColumnSchema {
  private final String columnName;
  private final CellValue.SQLDataType sqlDataType;

  public ColumnSchema(String columnName, CellValue.SQLDataType sqlDataType) {
    this.columnName = columnName;
    this.sqlDataType = sqlDataType;
  }

  public String getColumnName() {
    return columnName;
  }

  public CellValue.SQLDataType getSqlDataType() {
    return sqlDataType;
  }
}
