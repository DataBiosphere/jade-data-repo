package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public interface FilterVariable extends SqlExpression {
  List<TableVariable> getTables();
}
