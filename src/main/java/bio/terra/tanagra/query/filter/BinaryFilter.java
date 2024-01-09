package bio.terra.tanagra.query.filter;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.Filter;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import bio.terra.tanagra.serialization.filter.UFBinaryFilter;
import java.util.List;

public final class BinaryFilter extends Filter {
  private final FieldPointer field;
  private final BinaryFilterVariable.BinaryOperator operator;
  private final Literal value;

  private BinaryFilter(
      FieldPointer fieldPointer, BinaryFilterVariable.BinaryOperator operator, Literal value) {
    this.field = fieldPointer;
    this.operator = operator;
    this.value = value;
  }

  public static BinaryFilter fromSerialized(UFBinaryFilter serialized, TablePointer tablePointer) {
    if (serialized.getField() == null
        || serialized.getOperator() == null
        || serialized.getValue() == null) {
      throw new InvalidConfigException("Only some table filter fields are defined");
    }

    FieldPointer fieldPointer = FieldPointer.fromSerialized(serialized.getField(), tablePointer);
    Literal literal = Literal.fromSerialized(serialized.getValue());

    return new BinaryFilter(fieldPointer, serialized.getOperator(), literal);
  }

  @Override
  public Type getType() {
    return Type.BINARY;
  }

  @Override
  public BinaryFilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables) {
    return new BinaryFilterVariable(field.buildVariable(primaryTable, tables), operator, value);
  }

  public FieldPointer getField() {
    return field;
  }

  public BinaryFilterVariable.BinaryOperator getOperator() {
    return operator;
  }

  public Literal getValue() {
    return value;
  }
}
