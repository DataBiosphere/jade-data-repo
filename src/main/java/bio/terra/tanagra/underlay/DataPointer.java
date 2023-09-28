package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.QueryExecutor;

public abstract class DataPointer {

  /** Enum for the types of external data pointers supported by Tanagra. */
  public enum Type {
    BQ_DATASET,
    AZURE_DATASET
  }

  private final String name;

  public DataPointer(String name) {
    this.name = name;
  }

  public abstract Type getType();

  public String getName() {
    return name;
  }

  public abstract String getTableSQL(String tableName);

  public abstract String getTablePathForIndexing(String tableName);

  public abstract Literal.DataType lookupDatatype(
      FieldPointer fieldPointer, QueryExecutor executor);
}
