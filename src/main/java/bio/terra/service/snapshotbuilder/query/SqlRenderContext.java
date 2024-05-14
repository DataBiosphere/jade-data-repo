package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlRenderContext {
  private final TableNameGenerator tableNameGenerator;
  private final CloudPlatformWrapper platform;
  private final Map<SourceVariable, String> aliases = new HashMap<>();

  public SqlRenderContext(TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform) {
    this.tableNameGenerator = tableNameGenerator;
    this.platform = platform;
  }

  public String getTableName(String tableName) {
    return tableNameGenerator.generate(tableName);
  }

  public CloudPlatformWrapper getPlatform() {
    return platform;
  }

  /**
   * The default table alias is the first letter of each element in table name, where elements are
   * separated by a `_`. For example, the default alias for table `concept_ancestor` will be `ca`.
   */
  @VisibleForTesting
  static String getDefaultAlias(String tableName) {
    return Arrays.stream(tableName.split("_"))
        .filter(part -> !part.isEmpty())
        .map(part -> part.toLowerCase().substring(0, 1))
        .collect(Collectors.joining());
  }

  /**
   * Given a {@link SourceVariable}, return an alias for a generated SQL query. Either an existing
   * alias is returned, or a new alias is generated based on the table name.
   */
  public String getAlias(SourceVariable sourceVariable) {
    return aliases.computeIfAbsent(
        sourceVariable,
        key -> {
          // If no alias exists, find the next unused one using the default alias as a base. Append
          // successively higher integers until we find one that doesn't conflict with any other
          // table aliases.
          String defaultAlias = getDefaultAlias(key.getSourcePointer().getSourceName());
          String alias = defaultAlias;
          int suffix = 1;
          while (aliases.containsValue(alias)) {
            alias = defaultAlias + suffix++;
          }
          return alias;
        });
  }
}
