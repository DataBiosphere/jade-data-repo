package bio.terra.service.snapshotbuilder.query;

public class QueryTestUtils {
  public static TablePointer fromTableName(String tableName) {
    return TablePointer.fromTableName(tableName, s -> s);
  }
}
