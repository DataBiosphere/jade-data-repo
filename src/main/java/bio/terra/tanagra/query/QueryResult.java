package bio.terra.tanagra.query;

import com.google.common.base.Preconditions;
import java.util.Iterator;

/**
 * The result of a data access query.
 */
public record QueryResult(Iterable<RowResult> rowResults, ColumnHeaderSchema columnHeaderSchema) {

  /**
   * The {@link RowResult}s that make of the data of the query result.
   */
  @Override
  public Iterable<RowResult> rowResults() {
    return rowResults;
  }

  /**
   * The {@link ColumnHeaderSchema}s for the {@link #rowResults ()}.
   */
  @Override
  public ColumnHeaderSchema columnHeaderSchema() {
    return columnHeaderSchema;
  }

  /**
   * Expect a single {@link RowResult} and return it.
   */
  public RowResult getSingleRowResult() {
    Iterator<RowResult> rowResultIter = rowResults().iterator();
    Preconditions.checkArgument(rowResultIter.hasNext(), "No row results were returned");
    RowResult rowResult = rowResultIter.next();
    Preconditions.checkArgument(!rowResultIter.hasNext(), "More than one row result was returned");
    return rowResult;
  }
}
