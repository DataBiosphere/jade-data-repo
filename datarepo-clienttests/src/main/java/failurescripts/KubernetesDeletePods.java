package failurescripts;

import bio.terra.datarepo.client.ApiClient;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.FailureScript;

// Failure script in a new folder - "failurescripts"
// Very similarly to TestScripts, but defined separately to handle the different parameters

public class KubernetesDeletePods extends FailureScript {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesDeletePods.class);

 // TODO set parameters -> how many pods to kill? How long do we wait before killing them?

  @Override
  public void userJourney(ApiClient apiClient) throws Exception {
    TimeUnit.SECONDS.sleep(30);
    logger.info("FAILING HERE!");
  }
}
