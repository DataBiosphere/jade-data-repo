package utils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import runner.config.ServerSpecification;

// TODO: add try/catch for refresh token around all utils methods
public final class KubernetesClientUtils {

  private static int maximumSecondsToWaitForReplicaSetSizeChange = 500;
  private static int secondsIntervalToPollReplicaSetSizeChange = 5;

  public static final String componentLabel = "app.kubernetes.io/component";
  public static final String apiComponentLabel = "api";

  private static String namespace;

  private static CoreV1Api kubernetesClientCoreObject;
  private static AppsV1Api kubernetesClientAppsObject;

  private KubernetesClientUtils() {}

  public static CoreV1Api getKubernetesClientCoreObject() {
    return kubernetesClientCoreObject;
  }

  public static AppsV1Api getKubernetesClientAppsObject() {
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
    List<String> scriptArgs = new ArrayList<>();
    scriptArgs.add("tools/fetchGKECredentials.sh");
    scriptArgs.add(server.clusterShortName);
    scriptArgs.add(server.region);
    scriptArgs.add(server.project);
    ProcessUtils.executeCommand("sh", scriptArgs);

    namespace = server.namespace;

    // path to kubeconfig file, that was just created/updated by gcloud get-credentials above
    String kubeConfigPath = System.getProperty("user.home") + "/.kube/config";

    // load the kubeconfig object from the file
    InputStreamReader filereader =
        new InputStreamReader(new FileInputStream(kubeConfigPath), StandardCharsets.UTF_8);
    KubeConfig kubeConfig = KubeConfig.loadKubeConfig(filereader);

    // get a refreshed SA access token and its expiration time
    GoogleCredentials applicationDefaultCredentials =
        AuthenticationUtils.getApplicationDefaultCredential();
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
    ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();

    // set the global default client to the one created above because the CoreV1Api and AppsV1Api
    // constructors get the client object from the global configuration
    Configuration.setDefaultApiClient(client);

    kubernetesClientCoreObject = new CoreV1Api();
    kubernetesClientAppsObject = new AppsV1Api();
  }

  /**
   * List all the pods in namespace defined in buildKubernetesClientObject by the server specification,
   * or in the whole cluster if the namespace is not specified (i.e. null or empty string).
   *
   * @return list of Kubernetes pods
   */
  public static List<V1Pod> listPods() throws ApiException {
    V1PodList list;
    if (namespace == null || namespace.isEmpty()) {
      list =
          kubernetesClientCoreObject.listPodForAllNamespaces(
              null, null, null, null, null, null, null, null, null);
    } else {
      list =
          kubernetesClientCoreObject.listNamespacedPod(
              namespace, null, null, null, null, null, null, null, null, null);
    }
    return list.getItems();
  }

  /**
   * List all the deployments in namespace defined in buildKubernetesClientObject by the server specification,
   * or in the whole cluster if the namespace is not specified (i.e. null or empty string).
   *
   * @return list of Kubernetes deployments
   */
  public static List<V1Deployment> listDeployments() throws ApiException {
    V1DeploymentList list;
    if (namespace == null || namespace.isEmpty()) {
      list =
          kubernetesClientAppsObject.listDeploymentForAllNamespaces(
              null, null, null, null, null, null, null, null, null);
    } else {
      list =
          kubernetesClientAppsObject.listNamespacedDeployment(
              namespace, null, null, null, null, null, null, null, null, null);
    }
    return list.getItems();
  }

  /**
   * Get the API deployment in the in the namespace defined in buildKubernetesClientObject by the server specification,
   * or in the whole cluster if the namespace is not specified (i.e. null or empty string).
   * This method expects that there is a single API deployment in the namespace.
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
    V1Deployment deploy;
    try {
      V1DeploymentSpec existingSpec = deployment.getSpec();
      deployment.setSpec(existingSpec.replicas(numberOfReplicas));
      deploy =
          kubernetesClientAppsObject.replaceNamespacedDeployment(
              deployment.getMetadata().getName(),
              deployment.getMetadata().getNamespace(),
              deployment,
              null,
              null,
              null);
    } catch (ApiException ex) {
      System.out.println(
          "Scale the pod failed for Deployment. Exception: "
              + ex.getMessage()
              + ex.getStackTrace());
      throw ex;
    }
    return deploy;
  }

  /**
   * Wait until the size of the replica set matches the specified number of pods. Times out after
   * {@link KubernetesClientUtils#maximumSecondsToWaitForReplicaSetSizeChange} seconds. Polls in
   * intervals of {@link KubernetesClientUtils#secondsIntervalToPollReplicaSetSizeChange} seconds.
   *
   * @param deployment the deployment object to poll
   * @param numberOfReplicas the eventual expected size of the replica set
   */
  public static void waitForReplicaSetSizeChange(V1Deployment deployment, int numberOfReplicas)
      throws Exception {
    int pollCtr =
        Math.floorDiv(
            maximumSecondsToWaitForReplicaSetSizeChange, secondsIntervalToPollReplicaSetSizeChange);

    // get the component label from the deployment object
    // this will be "api" for most cases, since that's what we're interested in scaling.
    String deploymentComponentLabel = deployment.getMetadata().getLabels().get(componentLabel);
    String namespace = deployment.getMetadata().getNamespace();

    // loop through the pods in the namespace
    // find the ones that match the deployment component label (e.g. find all the API pods)
    long numPods =
        listPods().stream()
            .filter(
                pod ->
                    deploymentComponentLabel.equals(
                        pod.getMetadata().getLabels().get(componentLabel)))
            .count();

    while (numPods != numberOfReplicas && pollCtr >= 0) {
      TimeUnit.SECONDS.sleep(secondsIntervalToPollReplicaSetSizeChange);
      numPods =
          listPods().stream()
              .filter(
                  pod ->
                      deploymentComponentLabel.equals(
                          pod.getMetadata().getLabels().get(componentLabel)))
              .count();
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

  public static void modifyKubernetesPostDeployment(int podCount) throws Exception {
    // set the initial number of pods in the API deployment replica set
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }
    System.out.println(
        "pod count before set initial replica set size: "
            + KubernetesClientUtils.listPods().size());
    apiDeployment = KubernetesClientUtils.changeReplicaSetSize(apiDeployment, podCount);
    KubernetesClientUtils.waitForReplicaSetSizeChange(apiDeployment, podCount);

    // print out the current pods
    List<V1Pod> pods = KubernetesClientUtils.listPods();
    System.out.println("initial number of pods: " + pods.size());
    for (V1Pod pod : pods) {
      System.out.println("  pod: " + pod.getMetadata().getName());
    }
  }
}
