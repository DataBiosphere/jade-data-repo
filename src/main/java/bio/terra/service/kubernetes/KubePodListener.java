package bio.terra.service.kubernetes;

import bio.terra.service.job.JobShutdownState;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.StairwayExecutionException;
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
    private static final int WATCH_RETRIES = 10;
    private static final int WATCH_INITIAL_WAIT = 5;
    private static final int WATCH_MAX_WAIT = 30;

    private final Logger logger = LoggerFactory.getLogger(KubePodListener.class);
    private final JobShutdownState jobShutdownState;
    private final String namespace;
    private final String thisPod;
    private final Stairway stairway;
    private Exception exception;
    private Map<String, Boolean> podMap; // pod name; true means running; false means deleted

    public KubePodListener(JobShutdownState jobShutdownState, Stairway stairway, String namespace, String thisPod) {
        this.jobShutdownState = jobShutdownState;
        this.namespace = namespace;
        this.thisPod = thisPod;
        this.stairway = stairway;
        this.exception = null;

        podMap = new HashMap<>();
    }

    @Override
    public void run() {
        logger.info("KubePodListener starting");

        exception = null;
        int consecutiveRetryCount = 0;
        int retryWait = WATCH_INITIAL_WAIT;
        while (true) {
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
                             new TypeToken<Watch.Response<V1Namespace>>() {
                             }.getType())) {
                    for (Watch.Response<V1Namespace> item : watch) {
                        // If we are shutting down, we stop watching
                        if (jobShutdownState.isShutdown()) {
                            return;
                        }
                        // Reset retry if the watch worked
                        consecutiveRetryCount = 0;
                        retryWait = WATCH_INITIAL_WAIT;

                        String operation = item.type;
                        String podName = item.object.getMetadata().getName();
                        logger.info(String.format("%s : %s", operation, podName));

                        if (StringUtils.contains(podName, API_POD_FILTER)) {
                            if (StringUtils.equals(operation, "ADDED")) {
                                logger.info("Added api pod: " + podName);
                                podMap.put(podName, true);
                            } else if (StringUtils.equals(operation, "DELETED")) {
                                try {
                                    logger.info("Attempting clean up of deleted stairway instance: " + podName);
                                    stairway.recoverStairway(podName);
                                    Boolean deletedPod = podMap.get(podName);
                                    if (deletedPod != null && !deletedPod) {
                                        logger.info("Deleted api pod: " + podName);
                                        podMap.put(podName, false);
                                    }
                                } catch (DatabaseOperationException | StairwayExecutionException ex) {
                                    logger.error("Stairway recoveryStairway failed to recovery pod: " + podName, ex);
                                } catch (InterruptedException ex) {
                                    logger.info("KubePodListener interrupted - exiting", ex);
                                    exception = ex;
                                    return;
                                }
                            }
                        }
                    }
                }
            } catch (RuntimeException | ApiException | IOException ex) {
                exception = ex;
                logger.info("KubePodListener caught exception: " + exception);
            }

            // Exponential backoff retry after an exception
            if (consecutiveRetryCount >= WATCH_RETRIES) {
                logger.error("KubePodListener exiting - exceeded max consecutive retries", exception);
                return;
            }
            consecutiveRetryCount++;

            try {
                logger.info("KubePodListener retry wait seconds: " + retryWait);
                TimeUnit.SECONDS.sleep(retryWait);
                retryWait = Math.min(retryWait + retryWait, WATCH_MAX_WAIT);
            } catch (InterruptedException ex) {
                logger.info("KubePodListener exiting - interruped while retrying");
                return;
            }
            logger.info("KubePodListener consecutive retry: " + consecutiveRetryCount);
        }
    }

    public Exception getException() {
        return exception;
    }

    public Map<String, Boolean> getPodMap() {
        return podMap;
    }
}
