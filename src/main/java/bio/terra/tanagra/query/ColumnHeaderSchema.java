package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** The schema of the columns in {@link RowResult}s. */
public class ColumnHeaderSchema {
  private final List<ColumnSchema> columnSchemas;

  public ColumnHeaderSchema(List<ColumnSchema> columnSchemas) {
    this.columnSchemas =
        columnSchemas.stream()
            .sorted(Comparator.comparing(ColumnSchema::getColumnName))
            .collect(Collectors.toList());
  }

  public int getIndex(String columnName) {
    for (int i = 0; i < columnSchemas.size(); ++i) {
      if (columnSchemas.get(i).getColumnName().equals(columnName)) {
        return i;
      }
    }
    throw new SystemException(
        String.format("Column name '%s' not a part of the column schema.", columnName));
  }

  /** The list of column schemas. Must match the order of the corresponding {@link RowResult}. */
  public List<ColumnSchema> getColumnSchemas() {
    return Collections.unmodifiableList(columnSchemas);
  }
}
