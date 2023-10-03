package bio.terra.tanagra.query;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/** The schema of the columns in {@link RowResult}s. */
public record ColumnHeaderSchema(List<ColumnSchema> columnSchemas) {
  public ColumnHeaderSchema(List<ColumnSchema> columnSchemas) {
    // Sort the column schemas, so the list matches the order of the corresponding RowResult.
    this.columnSchemas =
        columnSchemas.stream().sorted(Comparator.comparing(ColumnSchema::columnName)).toList();
  }

  public int getIndex(String columnName) {
    return IntStream.range(0, columnSchemas.size())
        .filter(i -> columnSchemas.get(i).columnName().equals(columnName))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Column name '%s' not a part of the column schema.".formatted(columnName)));
  }
}
