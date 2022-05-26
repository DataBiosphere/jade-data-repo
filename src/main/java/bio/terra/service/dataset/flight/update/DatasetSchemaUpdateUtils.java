package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.TableModel;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DatasetSchemaUpdateUtils {

  public static boolean hasTableAdditions(DatasetSchemaUpdateModel updateModel) {
    var changes =
        Objects.requireNonNullElse(updateModel.getChanges(), new DatasetSchemaUpdateModelChanges());
    var tables = changes.getAddTables();
    return !(tables == null || tables.isEmpty());
  }

  public static boolean hasColumnAdditions(DatasetSchemaUpdateModel updateModel) {
    var changes =
        Objects.requireNonNullElse(updateModel.getChanges(), new DatasetSchemaUpdateModelChanges());
    var columns = changes.getAddColumns();
    return !(columns == null || columns.isEmpty());
  }

  public static List<String> getNewTableNames(DatasetSchemaUpdateModel updateModel) {
    return updateModel.getChanges().getAddTables().stream()
        .map(TableModel::getName)
        .collect(Collectors.toList());
  }
}
