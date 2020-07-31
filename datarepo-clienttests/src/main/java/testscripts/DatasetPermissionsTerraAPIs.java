package testscripts;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.VersionApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestScript;
import runner.config.TestUserSpecification;
import utils.AuthenticationUtils;

public class DatasetPermissionsTerraAPIs extends TestScript {
  private static final Logger logger = LoggerFactory.getLogger(DatasetPermissionsTerraAPIs.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public DatasetPermissionsTerraAPIs() {
    super();
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    try {
      org.broadinstitute.dsde.workbench.client.sam.ApiClient samClient =
          new org.broadinstitute.dsde.workbench.client.sam.ApiClient();
      samClient.setBasePath("https://sam.dsde-dev.broadinstitute.org");

      GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser);
      AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);
      samClient.setAccessToken(userAccessToken.getTokenValue());
      samClient.setUserAgent("OpenAPI-Generator/1.0.0 java"); // only logs an error in sam
      logger.info("build sam client: {}", samClient.getBasePath());
      ResourcesApi samResourcesApi = new ResourcesApi(samClient);
      VersionApi samVersionApi =
          new org.broadinstitute.dsde.workbench.client.sam.api.VersionApi(samClient);
      logger.info("build sam resources api: {}", samVersionApi.samVersion());
      boolean isSteward =
          samResourcesApi.resourceAction("datarepo", "broad-jade-dev", "create_dataset");
      logger.info("isSteward = {}", isSteward);
    } catch (Exception ex) {
      logger.info("caught exception", ex);
    }

    //        ApiClient apiClient = new ApiClient();
    //        apiClient.setBasePath(config.server.uri);
    //        GoogleCredentials userCredential =
    // AuthenticationUtils.getDelegatedUserCredential(testUser);
    //        AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);
    //        apiClient.setAccessToken(userAccessToken.getTokenValue());

    //        RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    //        DatasetModel datasetModel =
    // repositoryApi.retrieveDataset(datasetSummaryModel.getId());
    //        logger.debug(
    //            "Successfully retrieved dataset: name = {}, data project = {}",
    //            datasetModel.getName(),
    //            datasetModel.getDataProject());
  }
}
