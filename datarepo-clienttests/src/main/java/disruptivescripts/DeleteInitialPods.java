package disruptivescripts;

import static utils.KubernetesClientUtils.componentLabel;

import io.kubernetes.client.openapi.models.V1Deployment;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
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

  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    logger.info(
        "Starting disruption - all initially created api pods will be deleted, one by one.");

    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }

    // get list of api pod names
    String deploymentComponentLabel = apiDeployment.getMetadata().getLabels().get(componentLabel);
    List<String> podsToDelete = new ArrayList<>();
    KubernetesClientUtils.listPods().stream()
        .filter(
            pod ->
                deploymentComponentLabel.equals(pod.getMetadata().getLabels().get(componentLabel)))
        .forEach(p -> podsToDelete.add(p.getMetadata().getName()));

    // delete original pods, and give them a chance to recover
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
