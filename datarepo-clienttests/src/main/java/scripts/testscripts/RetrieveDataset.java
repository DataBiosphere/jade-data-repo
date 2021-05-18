package scripts.testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.DatasetModel;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;

public class RetrieveDataset extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(RetrieveDataset.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveDataset() {
    super();
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    DatasetModel datasetModel =
        repositoryApi.retrieveDataset(datasetSummaryModel.getId(), Collections.emptyList());
    logger.info(
        "Successfully retrieved dataset: name = {}, data project = {}",
        datasetModel.getName(),
        datasetModel.getDataProject());
  }
}
