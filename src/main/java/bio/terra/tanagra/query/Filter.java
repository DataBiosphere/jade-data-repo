package bio.terra.tanagra.query;

import bio.terra.tanagra.query.filter.BinaryFilter;
import bio.terra.tanagra.query.filter.BooleanAndOrFilter;
import bio.terra.tanagra.serialization.UFFilter;
import bio.terra.tanagra.serialization.filter.UFBinaryFilter;
import bio.terra.tanagra.serialization.filter.UFBooleanAndOrFilter;
import java.util.List;

public interface Filter {
  /** Enum for the types of table filters supported by Tanagra. */
  enum Type {
    BINARY,
    BOOLEAN_AND_OR
  }

  Type getType();

  FilterVariable buildVariable(TableVariable primaryTable, List<TableVariable> tables);

  default UFFilter serialize() {
    return switch (getType()) {
      case BINARY -> new UFBinaryFilter((BinaryFilter) this);
      case BOOLEAN_AND_OR -> new UFBooleanAndOrFilter((BooleanAndOrFilter) this);
    };
  }
}
