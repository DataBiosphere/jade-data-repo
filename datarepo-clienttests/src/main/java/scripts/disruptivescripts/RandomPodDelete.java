package scripts.disruptivescripts;

import bio.terra.testrunner.common.utils.KubernetesClientUtils;
import bio.terra.testrunner.runner.DisruptiveScript;
import bio.terra.testrunner.runner.config.TestUserSpecification;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomPodDelete extends DisruptiveScript {
  private static final Logger logger = LoggerFactory.getLogger(RandomPodDelete.class);

  public RandomPodDelete() {
    super();
    manipulatesKubernetes = true;
  }

  private int repeatCount = 1;
  private int secondsBetweenRepeat = 30;
  protected static final int secondsToWaitBeforeStartingDisrupt = 15;

  public void setParameters(List<String> parameters) {

    if (parameters == null || parameters.size() != 2) {
      throw new IllegalArgumentException(
          "Must have 2 parameters in the parameters list: repeatCount & secondsBetweenRepeat");
    }
    repeatCount = Integer.parseInt(parameters.get(0));
    secondsBetweenRepeat = Integer.parseInt(parameters.get(1));

    if (repeatCount <= 0) {
      throw new IllegalArgumentException("Total disruption count must be greater than 0.");
    }

    if (secondsBetweenRepeat <= 0) {
      throw new IllegalArgumentException(
          "Time to wait between each disruption must be greater than 0.");
    }
  }

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    // give user journey threads time to get started before disruption
    TimeUnit.SECONDS.sleep(secondsToWaitBeforeStartingDisrupt);

    logger.info(
        "Starting disruption - A single random pod will be deleted {} times at {} second intervals.",
        repeatCount,
        secondsBetweenRepeat);
    for (int i = 0; i < repeatCount; i++) {
      logger.debug("Deleting random pod.");
      KubernetesClientUtils.deleteRandomPod();
      TimeUnit.SECONDS.sleep(secondsBetweenRepeat);
    }
  }
}
