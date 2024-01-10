package common.utils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.openapi.models.V1DeploymentSpec;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.Call;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;

// TODO: add try/catch for refresh token around all common.utils methods
public final class KubernetesClientUtils {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesClientUtils.class);

  private static int maximumSecondsToWaitForReplicaSetSizeChange = 500;
  private static int secondsIntervalToPollReplicaSetSizeChange = 5;

  public static final String componentLabel = "app.kubernetes.io/component";
  public static final String apiComponentLabel = "api";

  private static String namespace;

  private static CoreV1Api kubernetesClientCoreObject;
  private static AppsV1Api kubernetesClientAppsObject;

  private KubernetesClientUtils() {}

  public static CoreV1Api getKubernetesClientCoreObject() {
    if (kubernetesClientCoreObject == null) {
      throw new UnsupportedOperationException(
          "Kubernetes client core object is not setup. Check the server configuration skipKubernetes property.");
    }
    return kubernetesClientCoreObject;
  }

  public static AppsV1Api getKubernetesClientAppsObject() {
    if (kubernetesClientAppsObject == null) {
      throw new UnsupportedOperationException(
          "Kubernetes client apps object is not setup. Check the server configuration skipKubernetes property.");
    }
    return kubernetesClientAppsObject;
  }

  /**
   * Build the singleton Kubernetes client objects. This method should be called once at the
   * beginning of a test run, and then all subsequent fetches should use the getter methods instead.
   *
   * @param server the server specification that points to the relevant Kubernetes cluster
   */
  public static void buildKubernetesClientObject(ServerSpecification server) throws Exception {
    // call the fetchGKECredentials script that uses gcloud to generate the kubeconfig file
    logger.debug(
        "Calling the fetchGKECredentials script that uses gcloud to generate the kubeconfig file");
    List<String> scriptArgs = new ArrayList<>();
    scriptArgs.add("tools/fetchGKECredentials.sh");
    scriptArgs.add(server.clusterShortName);
    scriptArgs.add(server.region);
    scriptArgs.add(server.project);
    Process fetchCredentialsProc = ProcessUtils.executeCommand("sh", scriptArgs);
    List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(fetchCredentialsProc);
    for (String cmdOutputLine : cmdOutputLines) {
      logger.debug(cmdOutputLine);
    }

    // path to kubeconfig file, that was just created/updated by gcloud get-credentials above
    String kubeConfigPath = System.getenv("HOME") + "/.kube/config";
    logger.debug("Kube config path: {}", kubeConfigPath);

    namespace = server.namespace;
    // load the kubeconfig object from the file
    InputStreamReader filereader =
        new InputStreamReader(new FileInputStream(kubeConfigPath), StandardCharsets.UTF_8);
    KubeConfig kubeConfig = KubeConfig.loadKubeConfig(filereader);

    // get a refreshed SA access token and its expiration time
    logger.debug("Getting a refreshed service account access token and its expiration time");
    GoogleCredentials applicationDefaultCredentials =
        AuthenticationUtils.getServiceAccountCredential(server.testRunnerServiceAccount);
    AccessToken accessToken = AuthenticationUtils.getAccessToken(applicationDefaultCredentials);
    Instant tokenExpiration = accessToken.getExpirationTime().toInstant();
    String expiryUTC = tokenExpiration.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);

    // USERS: build list of one user, the SA
    LinkedHashMap<String, Object> authConfigSA = new LinkedHashMap<>();
    authConfigSA.put("access-token", accessToken.getTokenValue());
    authConfigSA.put("expiry", expiryUTC);

    LinkedHashMap<String, Object> authProviderSA = new LinkedHashMap<>();
    authProviderSA.put("name", "gcp");
    authProviderSA.put("config", authConfigSA);

    LinkedHashMap<String, Object> userSA = new LinkedHashMap<>();
    userSA.put("auth-provider", authProviderSA);

    LinkedHashMap<String, Object> userWrapperSA = new LinkedHashMap<>();
    userWrapperSA.put("name", server.clusterName);
    userWrapperSA.put("user", userSA);

    ArrayList<Object> usersList = new ArrayList<>();
    usersList.add(userWrapperSA);

    // CONTEXTS: build list of one context, the specified cluster
    LinkedHashMap<String, Object> context = new LinkedHashMap<>();
    context.put("cluster", server.clusterName);
    context.put(
        "user", server.clusterName); // when is the user ever different from the cluster name?

    LinkedHashMap<String, Object> contextWrapper = new LinkedHashMap<>();
    contextWrapper.put("name", server.clusterName);
    contextWrapper.put("context", context);

    ArrayList<Object> contextsList = new ArrayList<>();
    contextsList.add(contextWrapper);

    // CLUSTERS: use the cluster list read in from the kubeconfig file, because I can't figure out
    // how to get the certificate-authority-data and server address for the cluster via the Java
    // client library, only with gcloud
    ArrayList<Object> clusters = kubeConfig.getClusters();

    // build the config object, replacing the contexts and users lists from the kubeconfig file with
    // the ones constructed programmatically above
    kubeConfig = new KubeConfig(contextsList, clusters, usersList);
    kubeConfig.setContext(server.clusterName);

    // build the client object from the config
    logger.debug("Building the client objects from the config");
    ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();

    // set the global default client to the one created above because the CoreV1Api and AppsV1Api
    // constructors get the client object from the global configuration
    Configuration.setDefaultApiClient(client);

    kubernetesClientCoreObject = new CoreV1Api();
    kubernetesClientAppsObject = new AppsV1Api();
  }

  /**
   * List all the pods in namespace defined in buildKubernetesClientObject by the server
   * specification, or in the whole cluster if the namespace is not specified (i.e. null or empty
   * string).
   *
   * @return list of Kubernetes pods
   */
  public static List<V1Pod> listPods() throws ApiException {
    V1PodList list;
    if (namespace == null || namespace.isEmpty()) {
      list =
          getKubernetesClientCoreObject()
              .listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
    } else {
      list =
          getKubernetesClientCoreObject()
              .listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null);
    }
    return list.getItems();
  }

  /**
   * List all the deployments in namespace defined in buildKubernetesClientObject by the server
   * specification, or in the whole cluster if the namespace is not specified (i.e. null or empty
   * string).
   *
   * @return list of Kubernetes deployments
   */
  public static List<V1Deployment> listDeployments() throws ApiException {
    V1DeploymentList list;
    if (namespace == null || namespace.isEmpty()) {
      list =
          getKubernetesClientAppsObject()
              .listDeploymentForAllNamespaces(null, null, null, null, null, null, null, null, null);
    } else {
      list =
          getKubernetesClientAppsObject()
              .listNamespacedDeployment(
                  namespace, null, null, null, null, null, null, null, null, null);
    }
    return list.getItems();
  }

  /**
   * Get the API deployment in the in the namespace defined in buildKubernetesClientObject by the
   * server specification, or in the whole cluster if the namespace is not specified (i.e. null or
   * empty string). This method expects that there is a single API deployment in the namespace.
   *
   * @return the API deployment, null if not found
   */
  public static V1Deployment getApiDeployment() throws ApiException {
    // loop through the deployments in the namespace
    // find the one that matches the api component label
    return listDeployments().stream()
        .filter(
            deployment ->
                deployment.getMetadata().getLabels().get(componentLabel).equals(apiComponentLabel))
        .findFirst()
        .orElse(null);
  }

  /**
   * Change the size of the replica set. Note that this just sends a request to change the size, it
   * does not wait to make sure the size is actually updated.
   *
   * @param deployment the deployment object to modify
   * @param numberOfReplicas the new size of the replica set to scale to
   */
  public static V1Deployment changeReplicaSetSize(V1Deployment deployment, int numberOfReplicas)
      throws ApiException {
    V1DeploymentSpec existingSpec = deployment.getSpec();
    deployment.setSpec(existingSpec.replicas(numberOfReplicas));
    return getKubernetesClientAppsObject()
        .replaceNamespacedDeployment(
            deployment.getMetadata().getName(),
            deployment.getMetadata().getNamespace(),
            deployment,
            null,
            null,
            null);
  }

  /** Select any pod from api pods and delete pod. */
  public static void deleteRandomPod() throws ApiException, IOException {
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }
    long podCount = countPodsWithLabel(apiDeployment);
    logger.debug("Pod Count: {}; Message: Before deleting pods", podCount);
    logPodsWithLabel(apiDeployment);
    String deploymentComponentLabel = apiDeployment.getMetadata().getLabels().get(componentLabel);

    // select a random pod from list of apis
    String randomPodName;
    randomPodName =
        listPods().stream()
            .filter(
                pod ->
                    deploymentComponentLabel.equals(
                        pod.getMetadata().getLabels().get(componentLabel)))
            .skip(new Random().nextInt((int) podCount))
            .findFirst()
            .get()
            .getMetadata()
            .getName();

    logger.info("delete random pod: {}", randomPodName);

    deletePod(randomPodName);
  }

  public static void deletePod(String podNameToDelete) throws ApiException, IOException {
    // known issue with java api "deleteNamespacedPod()" endpoint
    // https://github.com/kubernetes-client/java/issues/252
    // the following few lines were suggested as a workaround
    // https://github.com/kubernetes-client/java/issues/86
    Call call =
        getKubernetesClientCoreObject()
            .deleteNamespacedPodCall(
                podNameToDelete, namespace, null, null, null, null, null, null, null);
    Response response = call.execute();
    Configuration.getDefaultApiClient()
        .handleResponse(response, (new TypeToken<V1Pod>() {}).getType());
  }

  /**
   * Wait until the size of the replica set matches the specified number of running pods. Times out
   * after {@link KubernetesClientUtils#maximumSecondsToWaitForReplicaSetSizeChange} seconds. Polls
   * in intervals of {@link KubernetesClientUtils#secondsIntervalToPollReplicaSetSizeChange}
   * seconds.
   *
   * @param deployment the deployment object to poll
   * @param numberOfReplicas the eventual expected size of the replica set
   */
  public static void waitForReplicaSetSizeChange(V1Deployment deployment, int numberOfReplicas)
      throws Exception {
    // set values so that while conditions always true on first try
    // forces the first sleep statement to be hit giving the pods a chance to start any adjustments
    long numPods = -1;
    long numRunningPods = -2;
    int pollCtr =
        Math.floorDiv(
            maximumSecondsToWaitForReplicaSetSizeChange, secondsIntervalToPollReplicaSetSizeChange);

    while ((numPods != numRunningPods || numPods != numberOfReplicas) && pollCtr >= 0) {
      TimeUnit.SECONDS.sleep(secondsIntervalToPollReplicaSetSizeChange);
      // two checks to make sure we are fully back in working order
      // 1 - does the total number of pods match the replica count (for example, all terminating
      // pods have finished terminating & no longer show up in list)
      numPods = countPodsWithLabel(deployment);
      // 2 - does the number of pods in the "ready" state matches the replica count
      numRunningPods = countReadyPodsWithLabel(deployment);
      logger.debug("numPods: {}, numRunningPods: {}", numPods, numRunningPods);
      pollCtr--;
    }

    if (numPods != numberOfReplicas) {
      throw new RuntimeException(
          "Timed out waiting for replica set size to change. (numPods="
              + numPods
              + ", numberOfReplicas="
              + numberOfReplicas
              + ")");
    }
  }

  /**
   * Utilizing the other util functions to (1) fresh fetch of the api deployment, (2) scale the
   * replica count, (3) wait for replica count to update, and (4) print the results
   *
   * @param podCount count of pods to scale the kubernetes deployment to
   */
  public static void changeReplicaSetSizeAndWait(int podCount) throws Exception {
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }

    long apiPodCount = countPodsWithLabel(apiDeployment);
    logger.debug("Pod Count: {}; Message: Before scaling pod count", apiPodCount);
    apiDeployment = KubernetesClientUtils.changeReplicaSetSize(apiDeployment, podCount);
    KubernetesClientUtils.waitForReplicaSetSizeChange(apiDeployment, podCount);

    // print out the current pods
    apiPodCount = countPodsWithLabel(apiDeployment);
    logger.debug("Pod Count: {}; Message: After scaling pod count", apiPodCount);
    logPodsWithLabel(apiDeployment);
  }

  /**
   * Return a stream of the pods that match the deployment component label.
   *
   * @param deployment the deployment the pods belong to
   * @return stream of matching pods
   */
  private static Stream<V1Pod> listPodsWithLabel(V1Deployment deployment) throws ApiException {
    String deploymentComponentLabel = deployment.getMetadata().getLabels().get(componentLabel);
    // loop through the pods in the namespace
    // find the ones that match the deployment component label (e.g. find all the API pods)
    return listPods().stream()
        .filter(
            pod ->
                deploymentComponentLabel.equals(pod.getMetadata().getLabels().get(componentLabel)));
  }

  /**
   * Count the number of pods that match the deployment component label.
   *
   * @param deployment the deployment the pods belong to
   * @return the number of matching pods
   */
  private static long countPodsWithLabel(V1Deployment deployment) throws ApiException {
    return listPodsWithLabel(deployment).count();
  }

  /**
   * Count the number of pods that match the deployment component label and have the READY state.
   *
   * @param deployment the deployment the pods belong to
   * @return the number of matching READY pods
   */
  private static long countReadyPodsWithLabel(V1Deployment deployment) throws ApiException {
    return listPodsWithLabel(deployment)
        .filter(
            pod ->
                pod.getStatus().getContainerStatuses()
                        != null // this condition is true for evicted pods
                    && pod.getStatus().getContainerStatuses().get(0).getReady())
        .count();
  }

  /**
   * Loop through the pods that match the deployment component label, writing their name to
   * debug-level logging.
   *
   * @param deployment the deployment the pods below to
   */
  public static void logPodsWithLabel(V1Deployment deployment) throws ApiException {
    listPodsWithLabel(deployment).forEach(p -> logger.debug("Pod: {}", p.getMetadata().getName()));
  }
}
