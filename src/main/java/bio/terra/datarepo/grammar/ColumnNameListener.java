package bio.terra.grammar;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ColumnNameListener extends SQLBaseListener {

  private final Set<String> columnNames = new HashSet<>();

  @Override
  public void enterColumn_name(SQLParser.Column_nameContext ctx) {
    String name = ctx.getText();
    columnNames.add(name);
  }

  public List<String> getColumnNames() {
    return new ArrayList<>(columnNames);
  }
}
