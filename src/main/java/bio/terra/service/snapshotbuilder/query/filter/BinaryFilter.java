package bio.terra.service.snapshotbuilder.query.filter;

import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.Filter;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import java.util.List;

public record BinaryFilter(
    FieldPointer fieldPointer, BinaryFilterVariable.BinaryOperator operator, Literal value)
    implements Filter {

  @Override
  public BinaryFilterVariable buildVariable(
      SourceVariable primaryTable, List<SourceVariable> tables) {
    return new BinaryFilterVariable(
        fieldPointer.buildVariable(primaryTable, tables), operator, value);
  }
}
