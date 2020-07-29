package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.DatasetModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testscripts.baseclasses.SimpleDataset;

public class RetrieveDataset extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(RetrieveDataset.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveDataset() {
    super();
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetSummaryModel.getId());
    logger.debug(
        "Successfully retrieved dataset: name = {}, data project = {}",
        datasetModel.getName(),
        datasetModel.getDataProject());
  }
}
