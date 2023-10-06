package bio.terra.tanagra.query.bigquery;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.Literal;
import com.google.cloud.bigquery.FieldValue;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/** A {@link CellValue} for BigQuery's {@link FieldValue}. */
class BigQueryCellValue implements CellValue {
  private final FieldValue fieldValue;
  private final ColumnSchema columnSchema;

  BigQueryCellValue(FieldValue fieldValue, ColumnSchema columnSchema) {
    this.fieldValue = fieldValue;
    this.columnSchema = columnSchema;
  }

  @Override
  public SQLDataType dataType() {
    return columnSchema.sqlDataType();
  }

  @Override
  public OptionalLong getLong() {
    assertDataTypeIs(SQLDataType.INT64);
    return fieldValue.isNull() ? OptionalLong.empty() : OptionalLong.of(fieldValue.getLongValue());
  }

  @Override
  public Optional<String> getString() {
    assertDataTypeIs(SQLDataType.STRING);
    return fieldValue.isNull() ? Optional.empty() : Optional.of(fieldValue.getStringValue());
  }

  @Override
  public OptionalDouble getDouble() {
    assertDataTypeIs(SQLDataType.FLOAT);
    return fieldValue.isNull()
        ? OptionalDouble.empty()
        : OptionalDouble.of(fieldValue.getDoubleValue());
  }

  @Override
  public Optional<Literal> getLiteral() {
    if (fieldValue.isNull()) {
      return Optional.empty();
    }

    Literal.DataType dataType = dataType().toUnderlayDataType();
    return switch (dataType) {
      case INT64 -> Optional.of(new Literal(fieldValue.getLongValue()));
      case STRING -> Optional.of(
          new Literal(fieldValue.isNull() ? null : fieldValue.getStringValue()));
      case BOOLEAN -> Optional.of(new Literal(fieldValue.getBooleanValue()));
      case DATE -> Optional.of(Literal.forDate(fieldValue.getStringValue()));
      case DOUBLE -> Optional.of(new Literal(fieldValue.getDoubleValue()));
    };
  }

  /**
   * Checks that the {@link #dataType()} is what's expected, or else throws an {@link
   * IllegalArgumentException}.
   */
  private void assertDataTypeIs(SQLDataType expected) {
    if (dataType() != expected) {
      throw new IllegalArgumentException(
          "SQLDataType is %s, not the expected %s".formatted(dataType(), expected));
    }
  }
}
