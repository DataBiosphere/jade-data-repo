package bio.terra.service.cohortbuilder.tanagra.instances.filter;

import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.NotFilterVariable;
import java.util.List;

public class BooleanNotFilter extends EntityFilter {
  private final EntityFilter subFilter;

  public BooleanNotFilter(EntityFilter subFilter) {
    this.subFilter = subFilter;
  }

  @Override
  public FilterVariable getFilterVariable(
      TableVariable entityTableVar, List<TableVariable> tableVars) {
    return new NotFilterVariable(subFilter.getFilterVariable(entityTableVar, tableVars));
  }
}
