package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/** The schema of the columns in {@link RowResult}s. */
public class ColumnHeaderSchema {
  private final List<ColumnSchema> columnSchemas;

  public ColumnHeaderSchema(List<ColumnSchema> columnSchemas) {
    this.columnSchemas =
        columnSchemas.stream()
            .sorted(Comparator.comparing(ColumnSchema::columnName))
            .toList();
  }

  public int getIndex(String columnName) {
    return IntStream.range(0, columnSchemas.size())
        .filter(i -> columnSchemas.get(i).columnName().equals(columnName))
        .findFirst()
        .orElseThrow(() -> new SystemException(
            String.format("Column name '%s' not a part of the column schema.", columnName)));
  }

  /** The list of column schemas. Must match the order of the corresponding {@link RowResult}. */
  public List<ColumnSchema> getColumnSchemas() {
    return columnSchemas;
  }
}
