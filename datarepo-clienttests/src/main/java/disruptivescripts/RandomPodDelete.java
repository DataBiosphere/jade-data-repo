package disruptivescripts;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DisruptiveScript;
import runner.config.TestUserSpecification;
import utils.KubernetesClientUtils;

public class RandomPodDelete extends DisruptiveScript {
  public RandomPodDelete() {
    super();
    manipulatesKubernetes = true;
  }

  private static final Logger logger = LoggerFactory.getLogger(RandomPodDelete.class);

  private int repeatCount = 1;
  private int secondsBetweenRepeat = 30;

  public void setParameters(List<String> parameters) {

    if (parameters == null || parameters.size() != 2) {
      throw new IllegalArgumentException(
          "Must have 2 parameters in the parameters list: repeatCount & secondsBetweenRepeat");
    }
    repeatCount = Integer.parseInt(parameters.get(0));
    secondsBetweenRepeat = Integer.parseInt(parameters.get(1));

    if (repeatCount <= 0) {
      throw new IllegalArgumentException("Total disruption count must be >=0.");
    }

    if (secondsBetweenRepeat <= 0) {
      throw new IllegalArgumentException("Time to wait between each disruption must be >=0.");
    }
  }

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    logger.info(
        "Starting disruption - A single random pod will be deleted {} times at {} second intervals.",
        repeatCount,
        secondsBetweenRepeat);
    for (int i = 0; i < repeatCount; i++) {
      TimeUnit.SECONDS.sleep(secondsBetweenRepeat);
      logger.debug("Deleting random pod.");
      KubernetesClientUtils.deleteRandomPod();
    }
  }
}
