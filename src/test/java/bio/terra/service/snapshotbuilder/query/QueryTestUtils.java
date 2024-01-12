package bio.terra.service.snapshotbuilder.query;

import java.util.function.Function;

public class QueryTestUtils {
  public static TablePointer fromTableName(String tableName) {
    return TablePointer.fromTableName(tableName, Function.identity());
  }
}
