package bio.terra.tanagra.query;

import java.util.List;

public abstract class Filter {
  /** Enum for the types of table filters supported by Tanagra. */
  public enum Type {
    BINARY,
    BOOLEAN_AND_OR
  }

  private final Type type;

  Type getType() {
    return type;
  }

  public abstract FilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables);

  protected Filter(Type type) {
    this.type = type;
  }
}
