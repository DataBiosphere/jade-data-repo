package disruptivescripts;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DisruptiveScript;
import runner.config.TestUserSpecification;
import utils.KubernetesClientUtils;

public class FIFOPodDelete extends DisruptiveScript {
  public FIFOPodDelete() {
    super();
    manipulatesKubernetes = true;
  }

  private static final Logger logger = LoggerFactory.getLogger(FIFOPodDelete.class);

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
      throw new IllegalArgumentException("Total disruption count must be greater than 0.");
    }

    if (secondsBetweenRepeat <= 0) {
      throw new IllegalArgumentException(
          "Time to wait between each disruption must be greater than 0.");
    }
  }

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    logger.info(
        "Starting disruption - {} pods will be deleted, first in, first out at {} second intervals.",
        repeatCount,
        secondsBetweenRepeat);
    for (int i = 0; i < repeatCount; i++) {
      TimeUnit.SECONDS.sleep(secondsBetweenRepeat);
      logger.debug("Deleting next pod.");
      KubernetesClientUtils.fifoDeletePod();
    }
  }
}
