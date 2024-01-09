package bio.terra.tanagra.query;

/**
 * An interface for a row within a result table.
 *
 * <p>This interface allows us to read data from different databases in a simple but uniform way.
 * Each supported database must implement this for returning rows.
 */
public interface RowResult {

  /** Get {@link CellValue} by the index of the column in the table query. */
  CellValue get(int index);

  /** Get {@link CellValue} by the name column in the table query. */
  CellValue get(String columnName);

  /** Returns the number of {@link CellValue}s in this row. */
  int size();
}
