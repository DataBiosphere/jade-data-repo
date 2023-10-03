package bio.terra.tanagra.query.bigquery;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.RowResult;
import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.FieldValueList;

/** A {@link RowResult} for BigQuery's {@link FieldValueList}. */
class BigQueryRowResult implements RowResult {
  private final FieldValueList fieldValues;
  private final ColumnHeaderSchema columnHeaderSchema;

  BigQueryRowResult(FieldValueList fieldValues, ColumnHeaderSchema columnHeaderSchema) {
    Preconditions.checkArgument(
        fieldValues.size() == columnHeaderSchema.columnSchemas().size(),
        "Field values size %d did not match column schemas size %d.",
        fieldValues.size(),
        columnHeaderSchema.columnSchemas().size());
    this.fieldValues = fieldValues;
    this.columnHeaderSchema = columnHeaderSchema;
  }

  @Override
  public CellValue get(int index) {
    return new BigQueryCellValue(
        fieldValues.get(index), columnHeaderSchema.columnSchemas().get(index));
  }

  @Override
  public CellValue get(String columnName) {
    int index = columnHeaderSchema.getIndex(columnName);
    return get(index);
  }

  @Override
  public int size() {
    return fieldValues.size();
  }
}
