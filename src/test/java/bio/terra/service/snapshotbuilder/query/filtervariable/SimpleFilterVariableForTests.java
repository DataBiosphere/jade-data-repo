package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;

public class SimpleFilterVariableForTests implements FilterVariable {
  @Override
  public List<TableVariable> getTables() {
    return List.of();
  }

  @Override
  public String renderSQL() {
    return "filter";
  }
}
