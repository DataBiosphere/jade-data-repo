package bio.terra.tanagra.query.azure;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.RowResult;
import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.FieldValueList;
import java.util.Map;
import java.util.Optional;

/** A {@link RowResult} for BigQuery's {@link FieldValueList}. */
public class AzureRowResult implements RowResult {
  private final Map<String, Optional<Object>> fieldValues;
  private final ColumnHeaderSchema columnHeaderSchema;

  public AzureRowResult(
      Map<String, Optional<Object>> fieldValues, ColumnHeaderSchema columnHeaderSchema) {
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
    var schema = columnHeaderSchema.columnSchemas().get(index);
    return new AzureCellValue(fieldValues.get(schema.columnName()), schema);
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
