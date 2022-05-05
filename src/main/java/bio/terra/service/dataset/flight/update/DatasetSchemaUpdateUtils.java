package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import java.util.Objects;

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
}
