package bio.terra.service.kubernetes;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.kubernetes.exception.KubeApiException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * KubeService provides access to the Kubernetes environment from within DRmanager.
 * This is primarily used to detect DRmanager pods going away so that we can recover
 * their flights in Stairway.
 */
@Component
@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
public class KubeService {
    private static final Logger logger = LoggerFactory.getLogger(KubeService.class);
    private static final String KUBE_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";
    private static final String API_POD_FILTER = "datarepo-api";

    private final ApplicationConfiguration appConfig;
    private final String podName;
    private final String namespace;
    private final boolean inKubernetes;

    @Autowired
    public KubeService(ApplicationConfiguration appConfig) {
        this.appConfig = appConfig;
        this.namespace = readFileIntoString(KUBE_DIR + "/namespace");
        this.podName = appConfig.getPodName();
        this.inKubernetes = appConfig.isInKubernetes();

        logger.info("Kubernetes configuration: inKube: " + inKubernetes +
            "; namespace: " + namespace +
            "; podName: " + podName);
    }

    /**
     * Get a list of the API pods from Kubernetes
     * @return list of pod names containing the API_POD_FILTER string; null if not in kubernetes
     */
    public List<String> getApiPodList() {
        if (!inKubernetes) {
            return null;
        }

        List<String> podList = new ArrayList<>();
        try {
            ApiClient client = ClientBuilder.cluster().build();
            Configuration.setDefaultApiClient(client);
            CoreV1Api api = new CoreV1Api();

            V1PodList list = api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null);
            for (V1Pod item : list.getItems()) {
                String podName = item.getMetadata().getName();
                if (StringUtils.contains(podName, API_POD_FILTER)) {
                    podList.add(podName);
                }
            }
            return podList;
        } catch (ApiException | IOException ex) {
            throw new KubeApiException("Error listing pods", ex);
        }
    }

    private String readFileIntoString(String path) {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(path));
            return new String(encoded, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Failed to read file: " + path + "; ", e);
            return null;
        }
    }
}
