package bio.terra.service.snapshotbuilder.query;

import java.util.List;

public interface Filter {
  FilterVariable buildVariable(SourceVariable primaryTable, List<SourceVariable> tables);
}
