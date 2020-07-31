package failurescripts;

import bio.terra.datarepo.client.*;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.*;
import runner.*;

public class KubernetesDeletePods extends FailureScript {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesDeletePods.class);

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    logger.info("setup in Kubernetes DeletePods");
  }

  @Override
  public void userJourney(ApiClient apiClient) throws Exception {
    TimeUnit.SECONDS.sleep(30);
    logger.info("FAILING HERE!");
  }
}
