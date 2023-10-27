package bio.terra.service.snapshotbuilder.query;

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
      return switch (underlayDataType) {
        case INT64 -> INT64;
        case STRING -> STRING;
        case BOOLEAN -> BOOLEAN;
        case DATE -> DATE;
        case DOUBLE -> FLOAT;
      };
    }

    public Literal.DataType toUnderlayDataType() {
      return switch (this) {
        case INT64 -> Literal.DataType.INT64;
        case STRING -> Literal.DataType.STRING;
        case BOOLEAN -> Literal.DataType.BOOLEAN;
        case DATE -> Literal.DataType.DATE;
        case FLOAT -> Literal.DataType.DOUBLE;
      };
    }
  }

  /** The type of data in this cell. */
  SQLDataType dataType();

  /**
   * Returns this field's value as a long or empty if the value is null.
   *
   * @throws IllegalArgumentException if the cell's value is not a long
   */
  OptionalLong getLong();

  /**
   * Returns this field's value as a string or empty if the value is null.
   *
   * @throws IllegalArgumentException if the cell's value is not a string
   */
  Optional<String> getString();

  /**
   * Returns this field's value as a double or empty if the value is null.
   *
   * @throws IllegalArgumentException if the cell's value is not a double
   */
  OptionalDouble getDouble();

  Optional<Literal> getLiteral();
}
