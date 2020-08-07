package failurescripts;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestScript;
import runner.config.TestUserSpecification;
import utils.KubernetesClientUtils;

public class RandomPodDelete extends TestScript {
  public RandomPodDelete() {
    super();
    manipulatesKubernetes = true;
  }

  private static final Logger logger = LoggerFactory.getLogger(RandomPodDelete.class);

  private int repeatFailureCount = 1;
  private int secondsWaitBeforeFailure = 30;

  public void setParameters(List<String> parameters) {

    if (parameters == null || parameters.size() != 2) {
      throw new IllegalArgumentException(
          "Must have 2 parameters in the parameters list: repeatFailureCount & secondsWaitBeforeFailure");
    }
    repeatFailureCount = Integer.parseInt(parameters.get(0));
    secondsWaitBeforeFailure = Integer.parseInt(parameters.get(1));

    if (repeatFailureCount <= 0) {
      throw new IllegalArgumentException("Total failure counts must be >=0.");
    }

    if (secondsWaitBeforeFailure <= 0) {
      throw new IllegalArgumentException("Time to wait for each failure must be >=0.");
    }
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    logger.debug("Starting failure script.");
    for (int i = 0; i < repeatFailureCount; i++) {
      TimeUnit.SECONDS.sleep(secondsWaitBeforeFailure);
      logger.debug("Deleting random pod.");
      KubernetesClientUtils.deleteRandomPod();
    }
  }
}
