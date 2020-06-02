package bio.terra.service.kubernetes;

import bio.terra.service.job.JobShutdownState;
import com.google.common.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import okhttp3.OkHttpClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static bio.terra.service.kubernetes.KubeConstants.API_POD_FILTER;

public class KubePodListener implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(KubePodListener.class);
    private final JobShutdownState jobShutdownState;
    private final String namespace;
    private final String thisPod;
    private Exception exception;
    private Map<String, Boolean> podMap; // pod name; true means running; false means deleted

    public KubePodListener(JobShutdownState jobShutdownState, String namespace, String thisPod) {
        this.jobShutdownState = jobShutdownState;
        this.namespace = namespace;
        this.thisPod = thisPod;
        this.exception = null;

        podMap = new HashMap<>();
    }

    @Override
    public void run() {
        logger.info("KubePodListener starting");

        exception = null;
        try {
            ApiClient client = Config.defaultClient();
            // infinite timeout
            OkHttpClient httpClient =
                client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
            client.setHttpClient(httpClient);
            Configuration.setDefaultApiClient(client);

            CoreV1Api api = new CoreV1Api();

            try (Watch<V1Namespace> watch =
                     Watch.createWatch(
                         client,
                         api.listNamespacedPodCall(namespace,
                             null,
                             null,
                             null,
                             null,
                             null,
                             5,
                             null,
                             null,
                             Boolean.TRUE,
                             null),
                         new TypeToken<Watch.Response<V1Namespace>>() { }.getType())) {
                for (Watch.Response<V1Namespace> item : watch) {
                    // If we are shutting down, we stop watching
                    if (jobShutdownState.isShutdown()) {
                        return;
                    }

                    String operation = item.type;
                    String podName = item.object.getMetadata().getName();
                    logger.info(String.format("%s : %s%n", operation, podName));

                    if (StringUtils.contains(podName, API_POD_FILTER)) {
                        if (StringUtils.equals(operation, "ADDED")) {
                            logger.info("Added api pod: " + podName);
                            podMap.put(podName, true);
                        } else if (StringUtils.equals(operation, "DELETED")) {
                            // TODO: call Stairway to inform it of a deleted pod
                            Boolean deletedPod = podMap.get(podName);
                            if (deletedPod != null && !deletedPod) {
                                logger.info("Deleted api pod: " + podName);
                                podMap.put(podName, false);
                            }
                        }
                    }
                }
            }
        } catch (ApiException | IOException ex) {
            exception = ex;
        }

        logger.info("KubePodListener exiting - exception: ", exception);
    }

    public Exception getException() {
        return exception;
    }

    public Map<String, Boolean> getPodMap() {
        return podMap;
    }

    public int getActivePodCount() {
        int count = 0;
        logger.info("KubePodListener getActivePodCount: {}", podMap.size());
        for (String k : podMap.keySet()) {
            Boolean d = podMap.get(k);
            logger.info("KubePodListener getActivePodCount: ({},{})", k, d);
            if (d) {
                count++;
                logger.info("KubePodListener getActivePodCount: count: {}", count);
            }
        }
        return count;
    }
}
