package scripts.disruptivescripts;

import common.utils.KubernetesClientUtils;
import io.kubernetes.client.openapi.models.V1Deployment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DisruptiveScript;
import runner.config.TestUserSpecification;

public class DeleteInitialPods extends DisruptiveScript {
  private static final Logger logger = LoggerFactory.getLogger(DeleteInitialPods.class);

  public DeleteInitialPods() {
    super();
    manipulatesKubernetes = true;
  }

  protected static final int secondsToWaitBeforeStartingDisrupt = 15;

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    // give user journey threads time to get started before disruption
    TimeUnit.SECONDS.sleep(secondsToWaitBeforeStartingDisrupt);

    logger.info(
        "Starting disruption - all initially created api pods will be deleted, one by one.");

    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }

    // get list of api pod names
    String deploymentComponentLabel =
        apiDeployment.getMetadata().getLabels().get(KubernetesClientUtils.apiComponentLabel);
    List<String> podsToDelete = new ArrayList<>();
    KubernetesClientUtils.listPods().stream()
        .filter(
            pod ->
                deploymentComponentLabel.equals(
                    pod.getMetadata().getLabels().get(KubernetesClientUtils.apiComponentLabel)))
        .forEach(p -> podsToDelete.add(p.getMetadata().getName()));

    // delete original pods, and give them a chance to recover
    for (String podName : podsToDelete) {
      logger.debug("delete pod: {}", podName);
      apiDeployment = KubernetesClientUtils.getApiDeployment();
      KubernetesClientUtils.logPodsWithLabel(apiDeployment);
      KubernetesClientUtils.deletePod(podName);
      KubernetesClientUtils.waitForReplicaSetSizeChange(apiDeployment, podsToDelete.size());
    }

    logger.debug("original pods:");
    podsToDelete.forEach(p -> logger.debug(p));
    KubernetesClientUtils.logPodsWithLabel(apiDeployment);
  }
}
