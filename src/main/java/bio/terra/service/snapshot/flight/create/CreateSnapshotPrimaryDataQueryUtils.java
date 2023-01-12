package bio.terra.service.snapshot.flight.create;

import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CreateSnapshotPrimaryDataQueryUtils {

  private final DatasetService datasetService;

  @Autowired
  public CreateSnapshotPrimaryDataQueryUtils(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  public Dataset retrieveDatasetSpecifiedByQuery(Query query) {
    String datasetName = query.getDatasetName();
    return datasetService.retrieveByName(datasetName);
  }

  public AssetSpecification retrieveAssetSpecification(Dataset dataset, String assetName) {
    return dataset
        .getAssetSpecificationByName(assetName)
        .orElseThrow(() -> new AssetNotFoundException("Expected asset specification"));
  }

  /**
   * validate that the root table is actually a table being queried in the query --> and the grammar
   * only picks up tables names in the from clause (though there may be more than one)
   */
  public void validateRootTable(Query query, AssetSpecification assetSpecification) {
    List<String> tableNames = query.getTableNames();
    String rootTableName = assetSpecification.getRootTable().getTable().getName();
    if (!tableNames.contains(rootTableName)) {
      throw new InvalidQueryException(
          "The root table of the selected asset is not present in this query");
    }
  }
}
