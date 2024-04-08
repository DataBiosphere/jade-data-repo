package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import java.util.HashMap;
import java.util.Map;

public class SqlRenderContext {
  private final TableNameGenerator tableNameGenerator;
  private final CloudPlatformWrapper platform;
  private final Map<TableVariable, String> aliases = new HashMap<>();

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
   * Iterate through all the {@link TableVariable}s and generate a unique alias for each one. Start
   * with the default alias (= first letter of the table name) and if that's taken, append
   * successively higher integers until we find one that doesn't conflict with any other table
   * aliases.
   */
  public String getAlias(TableVariable tableVariable) {
    if (aliases.containsKey(tableVariable)) {
      return aliases.get(tableVariable);
    }
    String defaultAlias = tableVariable.getDefaultAlias();
    String alias = defaultAlias;
    int suffix = 0;
    while (aliases.containsValue(alias)) {
      alias = defaultAlias + suffix++;
    }
    aliases.put(tableVariable, alias);
    return alias;
  }
}
