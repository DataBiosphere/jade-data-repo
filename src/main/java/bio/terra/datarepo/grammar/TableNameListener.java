package bio.terra.grammar;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableNameListener extends SQLBaseListener {

  private final Set<String> tableNames = new HashSet<>();

  @Override
  public void enterTable_name(SQLParser.Table_nameContext ctx) {
    String name = ctx.getText();
    tableNames.add(name);
  }

  public List<String> getTableNames() {
    return new ArrayList<>(tableNames);
  }
}
