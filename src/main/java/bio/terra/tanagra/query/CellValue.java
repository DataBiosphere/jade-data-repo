package bio.terra.tanagra.query;

import bio.terra.tanagra.exception.SystemException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * An interface for the value of a cell within a row within a result table.
 *
 * <p>This interface allows us to read data from different databases in a simple but uniform way.
 * Different database types should implement this for returning values.
 */
public interface CellValue {
  /** Enum for the SQL data types supported by Tanagra. */
  enum SQLDataType {
    INT64,
    STRING,
    BOOLEAN,
    DATE,
    FLOAT;

    public static SQLDataType fromUnderlayDataType(Literal.DataType underlayDataType) {
      switch (underlayDataType) {
        case INT64:
          return INT64;
        case STRING:
          return STRING;
        case BOOLEAN:
          return BOOLEAN;
        case DATE:
          return DATE;
        case DOUBLE:
          return FLOAT;
        default:
          throw new SystemException("Unknown underlay data type: " + underlayDataType);
      }
    }

    public Literal.DataType toUnderlayDataType() {
      switch (this) {
        case INT64:
          return Literal.DataType.INT64;
        case STRING:
          return Literal.DataType.STRING;
        case BOOLEAN:
          return Literal.DataType.BOOLEAN;
        case DATE:
          return Literal.DataType.DATE;
        case FLOAT:
          return Literal.DataType.DOUBLE;
        default:
          throw new SystemException("Unknown SQL data type: " + this);
      }
    }
  }

  /** The type of data in this cell. */
  SQLDataType dataType();

  /**
   * Returns this field's value as a long or empty if the value is null.
   *
   * @throws bio.terra.tanagra.exception.SystemException if the cell's value is not a long
   */
  OptionalLong getLong();

  /**
   * Returns this field's value as a string or empty if the value is null.
   *
   * @throws bio.terra.tanagra.exception.SystemException if the cell's value is not a string
   */
  Optional<String> getString();

  /**
   * Returns this field's value as a double or empty if the value is null.
   *
   * @throws bio.terra.tanagra.exception.SystemException if the cell's value is not a double
   */
  OptionalDouble getDouble();

  Optional<Literal> getLiteral();
}
