package bio.terra.service.kubernetes;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.job.JobShutdownState;
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
import java.util.concurrent.TimeUnit;

import static bio.terra.service.kubernetes.KubeConstants.API_POD_FILTER;
import static bio.terra.service.kubernetes.KubeConstants.KUBE_NAMESPACE_FILE;

/**
 * KubeService provides access to the Kubernetes environment from within DRmanager.
 * This is primarily used to detect DRmanager pods going away so that we can recover
 * their flights in Stairway.
 */
@Component
@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
public class KubeService {
    private static final Logger logger = LoggerFactory.getLogger(KubeService.class);

    private final ApplicationConfiguration appConfig;
    private final JobShutdownState jobShutdownState;
    private final String podName;
    private final String namespace;
    private final boolean inKubernetes;

    private KubePodListener podListener;
    private Thread podListenerThread;

    @Autowired
    public KubeService(ApplicationConfiguration appConfig, JobShutdownState jobShutdownState) {
        this.appConfig = appConfig;
        this.jobShutdownState = jobShutdownState;
        this.podName = appConfig.getPodName();
        this.inKubernetes = appConfig.isInKubernetes();
        if (inKubernetes) {
            this.namespace = readFileIntoString(KUBE_NAMESPACE_FILE);
        } else {
            this.namespace = "none";
        }

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
            CoreV1Api api = makeCoreApi();

            V1PodList list = api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null);
            for (V1Pod item : list.getItems()) {
                String podName = item.getMetadata().getName();
                if (StringUtils.contains(podName, API_POD_FILTER)) {
                    podList.add(podName);
                    logger.info("KubeService: Pod name {}", podName);
                }
            }
            return podList;
        } catch (ApiException ex) {
            throw new KubeApiException("Error listing pods", ex);
        }
    }

    /**
     * Launch the pod listener thread.
     */
    public void startPodListener() {
        if (inKubernetes) {
            podListener = new KubePodListener(jobShutdownState, namespace, podName);
            podListenerThread = new Thread(podListener);
            podListenerThread.start();
        }
    }

    /**
     * Stop the pod listener thread within a given time span.
     * @param timeUnit unit of the joinWait
     * @param joinWait number of units to wait for the listener thread to stop
     * @return true if the thread joined in the time span. False otherwise.
     */
    public boolean stopPodListener(TimeUnit timeUnit, long joinWait) {
        if (inKubernetes) {
            podListenerThread.interrupt();
            long waitMillis = timeUnit.toMillis(joinWait);
            try {
                podListenerThread.join(waitMillis);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return true;
    }

    CoreV1Api makeCoreApi() {
        try {
            ApiClient client = ClientBuilder.cluster().build();
            Configuration.setDefaultApiClient(client);
            return new CoreV1Api();
        } catch (IOException ex) {
            throw new KubeApiException("Error making core API", ex);
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
