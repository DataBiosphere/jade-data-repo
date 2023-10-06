package bio.terra.tanagra.query.azure;

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
    return columnSchema.sqlDataType();
  }

  public String name() {
    return columnSchema.columnName();
  }

  @Override
  public OptionalLong getLong() {
    assertDataTypeIs(SQLDataType.INT64);
    return fieldValue
        .map(o -> OptionalLong.of(((Number) o).longValue()))
        .orElseGet(OptionalLong::empty);
  }

  @Override
  public Optional<String> getString() {
    assertDataTypeIs(SQLDataType.STRING);
    return fieldValue.map(o -> (String) o);
  }

  @Override
  public OptionalDouble getDouble() {
    assertDataTypeIs(SQLDataType.FLOAT);
    return fieldValue
        .map(o -> OptionalDouble.of(((Number) o).doubleValue()))
        .orElseGet(OptionalDouble::empty);
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
