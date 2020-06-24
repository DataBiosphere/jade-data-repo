package bio.terra.clienttests;

import bio.terra.service.kubernetes.KubeService;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public final class KubernetesClientUtils {

    private static GoogleCredentials applicationDefaultCredentials;
    private static CoreV1Api kubernetesClientObject;
    private static final String defaultNameSpace = "default";
    private static AppsV1Api appsV1Api;
    // private static ExtensionsV1beta1Api V1BetaApi;
    private static final Logger logger = LoggerFactory.getLogger(KubernetesClientUtils.class);

    private KubernetesClientUtils() { }

    public static AccessToken getApplicationDefaultAccessToken() throws IOException {
        if (applicationDefaultCredentials == null) {
            // get the application default credentials
            applicationDefaultCredentials = ServiceAccountCredentials.getApplicationDefault();

            // not sure when this case happens
            if (applicationDefaultCredentials.createScopedRequired()) {
                applicationDefaultCredentials = applicationDefaultCredentials.createScoped(
                    Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }
        }

        // refresh the token if needed before returning
        applicationDefaultCredentials.refreshIfExpired();
        return applicationDefaultCredentials.getAccessToken();
    }

    public static CoreV1Api getKubernetesClientObject() throws Exception {
        // TODO: move these to config (these values are for dev)
        String clusterName = "gke_broad-jade-dev_us-central1_dev-master";
        String region = "us-central1";
        String project = "broad-jade-dev";

        if (kubernetesClientObject == null) {
            kubernetesClientObject = buildKubernetesClientObject(clusterName, region, project);
        }
        return kubernetesClientObject;
    }

    private static CoreV1Api buildKubernetesClientObject(String clusterName, String region, String project)
        throws Exception {
        // call the fetchGKECrednetials script that uses gcloud to generate the kubeconfig file
        List<String> scriptArgs = new ArrayList<>();
        scriptArgs.add("tools/fetchGKECredentials.sh");
        scriptArgs.add(clusterName);
        scriptArgs.add(region);
        scriptArgs.add(project);
        executeCommand("sh", scriptArgs);

        // path to kubeconfig file, that was just created/updated by gcloud get-credentials above
        String kubeConfigPath = System.getProperty("user.home") + "/.kube/config";

        // load the kubeconfig object from the file
        InputStreamReader filereader =
            new InputStreamReader(new FileInputStream(kubeConfigPath), Charset.forName("UTF-8"));
        KubeConfig kubeConfig = KubeConfig.loadKubeConfig(filereader);

        // get a refreshed SA access token and its expiration time
        AccessToken accessToken = getApplicationDefaultAccessToken();
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
        userWrapperSA.put("name", clusterName);
        userWrapperSA.put("user", userSA);

        ArrayList<Object> usersList = new ArrayList<>();
        usersList.add(userWrapperSA);

        // CONTEXTS: build list of one context, the specified cluster
        LinkedHashMap<String, Object> context = new LinkedHashMap<>();
        context.put("cluster", clusterName);
        context.put("user", clusterName); // when is the user ever different from the cluster name?

        LinkedHashMap<String, Object> contextWrapper = new LinkedHashMap<>();
        contextWrapper.put("name", clusterName);
        contextWrapper.put("context", context);

        ArrayList<Object> contextsList = new ArrayList<>();
        contextsList.add(contextWrapper);

        // CLUSTERS: use the cluster list read in from the kubeconfig file, because I can't figure out how to get the
        // certificate-authority-data and server address for the cluster via the Java client library, only with gcloud
        ArrayList<Object> clusters = kubeConfig.getClusters();

        // build the config object, replacing the contexts and users lists from the kubeconfig file with the ones
        // constructed programmatically above
        kubeConfig = new KubeConfig(contextsList, clusters, usersList);
        kubeConfig.setContext(clusterName);

        // build the client object from the config
        ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();

        // set the global default client to the one created above because the CoreV1Api constructor gets the client
        // object from the global configuration
        Configuration.setDefaultApiClient(client);

        // ExtensionsV1beta1Api extensionV1Api = new ExtensionsV1beta1Api();
        // extensionV1Api.setApiClient(client);
        // V1BetaApi = extensionV1Api;
        appsV1Api = new AppsV1Api(client);

        return new CoreV1Api(client);
    }

    /**
     * Executes a command in a separate process.
     * @param cmdArgs a list of the command line arguments=
     * @return a List of the lines written to stdout
     * @throws IOException
     */
    private static List<String> executeCommand(String cmd, List<String> cmdArgs) throws IOException {
        // build and run process
        cmdArgs.add(0, cmd);
        ProcessBuilder procBuilder = new ProcessBuilder(cmdArgs);
        Process proc = procBuilder.start();

        // read in all lines written to stdout
        BufferedReader bufferedReader =
            new BufferedReader(new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()));
        String outputLine;
        List<String> outputLines = new ArrayList<>();
        while ((outputLine = bufferedReader.readLine()) != null) {
            outputLines.add(outputLine);
        }
        bufferedReader.close();

        return outputLines;
    }

    public static void scaleDeployment(String deploymentName, int numberOfReplicas)
        throws ApiException {

        V1Deployment deploy =
            appsV1Api.readNamespacedDeployment(
                deploymentName, defaultNameSpace, null, null, null);
        logger.info("exisiting deploy replicas: {}", deploy.getSpec().getReplicas());
        try {
            V1DeploymentSpec newSpec = deploy.getSpec().replicas(numberOfReplicas);
            V1Deployment newDeploy = deploy.spec(newSpec);
            appsV1Api.replaceNamespacedDeployment(
                deploymentName, defaultNameSpace, newDeploy, null, null, null);
            logger.info("new deploy replicas: {}", newDeploy.getSpec().getReplicas());
        } catch (ApiException ex) {
            logger.error("Scale the pod failed for Deployment:" + deploymentName, ex);
        }
    }

    public static List<V1Pod> listKubernetesPods(CoreV1Api k8sclient) throws ApiException {
        // TODO: add try/catch for refresh token
        V1PodList list =
            k8sclient.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
        return list.getItems();
    }


    // example usage. need to be on the Broad VPN to talk to the dev cluster because of IP whitelist
    public static void main(String[] args) throws Exception {
        CoreV1Api k8sclient = KubernetesClientUtils.getKubernetesClientObject();
        scaleDeployment("sh-jade-datarepo-api", 2);
        scaleDeployment("sh-jade-datarepo-api", 1);
        for (V1Pod item : KubernetesClientUtils.listKubernetesPods(k8sclient)) {
            System.out.println(item.getMetadata().getName());
        }
    }

}
