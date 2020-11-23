package scripts.disruptivescripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.testrunner.runner.DisruptiveScript;
import bio.terra.testrunner.runner.config.TestUserSpecification;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.utils.DataRepoUtils;

public class EnableFaults extends DisruptiveScript {
  private static final Logger logger = LoggerFactory.getLogger(EnableFaults.class);

  public EnableFaults() {
    super();
  }

  protected List<String> faultNames;

  public void setParameters(List<String> parameters) {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "At least one parameter is required: name of the fault(s) to enable (e.g. SAM_TIMEOUT_FAULT, CREATE_ASSET_FAULT)");
    }
    faultNames = parameters;
  }

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    logger.info("Starting disruptive script: Resetting config, enabling faults.");

    // just pick the first test user
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUsers.get(0), server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // reset the config
    repositoryApi.resetConfig();
    logger.info("Config reset");

    // enable the faults
    for (String faultName : faultNames) {
      DataRepoUtils.enableFault(repositoryApi, faultName);
      logger.info("Fault enabled: {}", faultName);
    }
  }
}
