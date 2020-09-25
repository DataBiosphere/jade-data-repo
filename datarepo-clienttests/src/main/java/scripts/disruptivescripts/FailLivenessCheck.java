package scripts.disruptivescripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DisruptiveScript;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;
import scripts.utils.DataRepoUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class FailLivenessCheck extends DisruptiveScript {
  public FailLivenessCheck() {
    super();
  }

  private static final Logger logger = LoggerFactory.getLogger(FailLivenessCheck.class);

  private int secondsBeforeDisruption = 20;

  public void setParameters(List<String> parameters) {

    if (parameters == null || parameters.size() != 1) {
      throw new IllegalArgumentException(
          "Must have 1 parameter in the parameters list: secondsBeforeDisruption");
    }
    secondsBeforeDisruption = Integer.parseInt(parameters.get(0));

    if (secondsBeforeDisruption <= 0) {
      throw new IllegalArgumentException("Time to wait before disruption must be greater than 0.");
    }
  }

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    // HACK: no way to specify the server for a disruptive script so jamming it in here
    String serverSpecificationFile = "dddev.json";
    ServerSpecification server = ServerSpecification.fromJSONFile(serverSpecificationFile);
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUsers.get(0), server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    logger.info("Liveness check disruption will start in {} seconds", secondsBeforeDisruption);
    TimeUnit.SECONDS.sleep(secondsBeforeDisruption);
    logger.info("Liveness check disruption starting");
    repositoryApi.setFault("LIVENESS_FAULT", true);
  }
}
