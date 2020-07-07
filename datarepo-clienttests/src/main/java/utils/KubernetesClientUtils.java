package utils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
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
import runner.config.ServerSpecification;

public final class KubernetesClientUtils {

  private static CoreV1Api kubernetesClientObject;

  private KubernetesClientUtils() {}

  public static CoreV1Api getKubernetesClientObject() {
    return kubernetesClientObject;
  }

  /**
   * Build the singleton Kubernetes client object. This method should be called once at the
   * beginning of a test run, and then all subsequent fetches should use the getter method instead.
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

    // set the global default client to the one created above because the CoreV1Api constructor gets
    // the client object from the global configuration
    Configuration.setDefaultApiClient(client);

    kubernetesClientObject = new CoreV1Api();
  }

  /**
   * List all the pods in the given namespace, or in the whole cluster if the namespace is not
   * specified (i.e. null or empty string).
   *
   * @param namespace to list the pods in. empty string or null to list the pods in the whole
   *     cluster instead
   * @return list of Kubernetes pods
   */
  public static List<V1Pod> listKubernetesPods(String namespace) throws ApiException {
    // TODO: add try/catch for refresh token
    V1PodList list;
    if (namespace == null || namespace.isEmpty()) {
      list =
          kubernetesClientObject.listPodForAllNamespaces(
              null, null, null, null, null, null, null, null, null);
    } else {
      list =
          kubernetesClientObject.listNamespacedPod(
              namespace, null, null, null, null, null, null, null, null, null);
    }
    return list.getItems();
  }
}
