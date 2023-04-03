package bio.terra.tanagra.query.azure;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.Literal;
import com.google.cloud.bigquery.FieldValue;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/** A {@link CellValue} for BigQuery's {@link FieldValue}. */
public class AzureCellValue implements CellValue {
  private final Optional<Object> fieldValue;
  private final ColumnSchema columnSchema;

  AzureCellValue(Optional<Object> fieldValue, ColumnSchema columnSchema) {
    this.fieldValue = fieldValue;
    this.columnSchema = columnSchema;
  }

  @Override
  public SQLDataType dataType() {
    return columnSchema.getSqlDataType();
  }

  public String name() {
    return columnSchema.getColumnName();
  }

  @Override
  @SuppressWarnings("PMD.PreserveStackTrace")
  public OptionalLong getLong() {
    assertDataTypeIs(SQLDataType.INT64);
    try {
      return fieldValue
          .map(o -> OptionalLong.of(((Number) o).longValue()))
          .orElseGet(OptionalLong::empty);
    } catch (NumberFormatException nfEx) {
      throw new SystemException("Unable to format as number", nfEx);
    }
  }

  @Override
  public Optional<String> getString() {
    assertDataTypeIs(SQLDataType.STRING);
    return fieldValue.map(o -> (String) o);
  }

  @Override
  @SuppressWarnings("PMD.PreserveStackTrace")
  public OptionalDouble getDouble() {
    try {
      return fieldValue
          .map(o -> OptionalDouble.of(((Number) o).doubleValue()))
          .orElseGet(OptionalDouble::empty);
    } catch (NumberFormatException nfEx) {
      throw new SystemException("Unable to format as number", nfEx);
    }
  }

  @Override
  public Optional<Literal> getLiteral() {
    return fieldValue.map(
        value ->
            switch (dataType().toUnderlayDataType()) {
              case INT64 -> new Literal(((Number) value).longValue());
              case STRING -> new Literal((String) value);
              case BOOLEAN -> new Literal((boolean) value);
              case DATE -> Literal.forDate((String) value);
              case DOUBLE -> new Literal((double) value);
            });
  }

  /**
   * Checks that the {@link #dataType()} is what's expected, or else throws a {@link
   * SystemException}.
   */
  private void assertDataTypeIs(SQLDataType expected) {
    if (dataType() != expected) {
      throw new SystemException(
          String.format("SQLDataType is %s, not the expected %s", dataType(), expected));
    }
  }
}
