package testscripts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestScript;
import runner.config.TestUserSpecification;
import utils.SAMUtils;

public class DatasetPermissionsTerraAPIs extends TestScript {
  private static final Logger logger = LoggerFactory.getLogger(DatasetPermissionsTerraAPIs.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public DatasetPermissionsTerraAPIs() {
    super();
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    org.broadinstitute.dsde.workbench.client.sam.ApiClient samClient =
        SAMUtils.getClientForTestUser(testUser, server);
    logger.info("build sam resources api: {}", SAMUtils.getVersion(samClient));

    boolean isSteward = SAMUtils.isDataRepoSteward(samClient, server.samResourceIdForDatarepo);
    logger.info("testUser {} isSteward = {}", testUser.name, isSteward);

    //      ApiClient datarepoClient = DataRepoUtils.getClientForTestUser(testUser, server);
    //      RepositoryApi repositoryApi = new RepositoryApi(datarepoClient);
    //      DatasetModel datasetModel =
    //      repositoryApi.retrieveDataset(datasetSummaryModel.getId());
    //    logger.debug(
    //        "Successfully retrieved dataset: name = {}, data project = {}",
    //        datasetModel.getName(),
    //        datasetModel.getDataProject());
  }
}
