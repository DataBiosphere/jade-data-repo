package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.filter.BinaryFilter;
import bio.terra.tanagra.query.filter.BooleanAndOrFilter;
import bio.terra.tanagra.serialization.UFFilter;
import bio.terra.tanagra.serialization.filter.UFBinaryFilter;
import bio.terra.tanagra.serialization.filter.UFBooleanAndOrFilter;
import java.util.List;

public abstract class Filter {
  /** Enum for the types of table filters supported by Tanagra. */
  public enum Type {
    BINARY,
    BOOLEAN_AND_OR
  }

  public abstract Type getType();

  public abstract FilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables);

  public UFFilter serialize() {
    switch (getType()) {
      case BINARY:
        return new UFBinaryFilter((BinaryFilter) this);
      case BOOLEAN_AND_OR:
        return new UFBooleanAndOrFilter((BooleanAndOrFilter) this);
      default:
        throw new SystemException("Unknown table filter type: " + getType());
    }
  }
}
