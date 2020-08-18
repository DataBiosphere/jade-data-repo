package disruptivescripts;

import static utils.KubernetesClientUtils.componentLabel;

import io.kubernetes.client.openapi.models.V1Deployment;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DisruptiveScript;
import runner.config.TestUserSpecification;
import utils.KubernetesClientUtils;

public class DeleteInitialPods extends DisruptiveScript {
  public DeleteInitialPods() {
    super();
    manipulatesKubernetes = true;
  }

  private static final Logger logger = LoggerFactory.getLogger(DeleteInitialPods.class);

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
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }
    String deploymentComponentLabel = apiDeployment.getMetadata().getLabels().get(componentLabel);
    List<String> podsToDelete = new ArrayList<>();
    KubernetesClientUtils.listPods().stream()
        .filter(
            pod ->
                deploymentComponentLabel.equals(pod.getMetadata().getLabels().get(componentLabel)))
        .forEach(p -> podsToDelete.add(p.getMetadata().getName()));
    logger.debug("pods before delete");
    KubernetesClientUtils.printApiPods(apiDeployment);
    TimeUnit.SECONDS.sleep(30);
    for (String podName : podsToDelete) {
      logger.debug("delete pod: {}", podName);
      apiDeployment = KubernetesClientUtils.getApiDeployment();
      KubernetesClientUtils.printApiPods(apiDeployment);
      KubernetesClientUtils.deletePod(podName);
      KubernetesClientUtils.waitForReplicaSetSizeChange(apiDeployment, podsToDelete.size());
    }
    logger.debug("original pods:");
    podsToDelete.forEach(p -> logger.debug(p));
    KubernetesClientUtils.printApiPods(apiDeployment);
  }
}
