package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;

public class SimpleFilterVariableForTests implements FilterVariable {
  @Override
  public String renderSQL() {
    return "filter";
  }
}
