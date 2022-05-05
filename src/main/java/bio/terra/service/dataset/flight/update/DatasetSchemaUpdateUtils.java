package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.TableModel;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DatasetSchemaUpdateUtils {

  public static boolean hasTableAdditions(DatasetSchemaUpdateModel updateModel) {
    if (updateModel.getChanges() != null) {
      var changes = updateModel.getChanges();
      if (changes.getAddTables() != null) {
        return !changes.getAddTables().isEmpty();
      }
    }
    var changes =
        Objects.requireNonNullElse(updateModel.getChanges(), new DatasetSchemaUpdateModelChanges());
    var tables = changes.getAddTables();
    return !(tables == null || tables.isEmpty());
  }

  public static List<String> getNewTableNames(DatasetSchemaUpdateModel updateModel) {
    return updateModel.getChanges().getAddTables().stream().map(TableModel::getName).collect(Collectors.toList());
  }
}
