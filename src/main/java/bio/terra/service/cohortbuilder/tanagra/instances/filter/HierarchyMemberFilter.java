package bio.terra.service.cohortbuilder.tanagra.instances.filter;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.Underlay;
import java.util.List;

public class HierarchyMemberFilter extends EntityFilter {
  private final Hierarchy hierarchy;

  public HierarchyMemberFilter(Hierarchy hierarchy) {
    this.hierarchy = hierarchy;
  }

  @Override
  public FilterVariable getFilterVariable(
      TableVariable entityTableVar, List<TableVariable> tableVars) {
    HierarchyField pathField = hierarchy.getField(HierarchyField.Type.PATH);
    FieldVariable pathFieldVar =
        pathField.buildFieldVariableFromEntityId(
            hierarchy.getMapping(Underlay.MappingType.INDEX), entityTableVar, tableVars);

    // IS_MEMBER translates to path IS NOT NULL
    return new BinaryFilterVariable(
        pathFieldVar, BinaryFilterVariable.BinaryOperator.IS_NOT, new Literal(null));
  }
}
