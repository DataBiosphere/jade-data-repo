package bio.terra.service.snapshotbuilder.query;

public class QueryTestUtils {
  public static TablePointer fromTableName(String tableName) {
    return TablePointer.fromTableName(tableName, s -> s);
  }

  public static String collapseWhiteSpace(String string) {
    string = string.replaceAll("\\n", " ").replaceAll("\\s+", " ");
    return string;
  }
}
