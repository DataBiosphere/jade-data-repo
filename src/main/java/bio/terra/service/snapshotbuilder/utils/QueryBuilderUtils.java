package bio.terra.service.snapshotbuilder.utils;

import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;

public class QueryBuilderUtils {

  private QueryBuilderUtils() {}

  public static FieldVariable makeFieldVariable(
      TablePointer tablePointer, TableVariable tableVariable, String fieldName) {
    FieldPointer fieldPointer = new FieldPointer(tablePointer, fieldName);
    return new FieldVariable(fieldPointer, tableVariable);
  }
}
