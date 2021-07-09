package bio.terra.datarepo.grammar;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DatasetNameListener extends SQLBaseListener {

  private final Set<String> datasetNames = new HashSet<>();

  @Override
  public void enterDataset_name(SQLParser.Dataset_nameContext ctx) {
    String name = ctx.getText();
    datasetNames.add(name);
  }

  public List<String> getDatasetNames() {
    return new ArrayList<>(datasetNames);
  }
}
