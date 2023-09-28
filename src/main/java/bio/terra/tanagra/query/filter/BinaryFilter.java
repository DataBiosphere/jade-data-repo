package bio.terra.tanagra.query.filter;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.Filter;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import java.util.List;

public final class BinaryFilter extends Filter {
  private final FieldPointer fieldPointer;
  private final BinaryFilterVariable.BinaryOperator operator;
  private final Literal value;

  public BinaryFilter(
      FieldPointer fieldPointer, BinaryFilterVariable.BinaryOperator operator, Literal value) {
    super(Type.BINARY);
    this.fieldPointer = fieldPointer;
    this.operator = operator;
    this.value = value;
  }

  @Override
  public BinaryFilterVariable buildVariable(
      TableVariable primaryTable, List<TableVariable> tables) {
    return new BinaryFilterVariable(
        fieldPointer.buildVariable(primaryTable, tables), operator, value);
  }
}
