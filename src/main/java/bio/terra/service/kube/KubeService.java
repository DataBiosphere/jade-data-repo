package bio.terra.service.kube;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Component
@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
public class KubeService {
    private static final Logger logger = LoggerFactory.getLogger(KubeService.class);
    private static final String KUBE_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";


    /**
     *
     *
     * Only call after Stairway is initialized.
     * Starts the watch thread to detect changes to the deployment
     * Checks for orphan Stairways from killed pods
     */
    public void startup() {
        // launch watch thread
        // check for Stairway orhans
    }

/*
    public void listFilesForFolder(final File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                logger.info("dir: " + fileEntry.getName());
                listFilesForFolder(fileEntry);
            } else {
                logger.info("file:" + fileEntry.getName());
            }
        }
    }
*/
    private String readFileIntoString(String path) {
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(path));
            return new String(encoded, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Failed to read file: " + path + "; ", e);
            return null;
        }
    }

    public void ktest() {
        try {
            String namespace = readFileIntoString(KUBE_DIR + "/namespace");
            logger.info("Namespace: " + namespace);

            // TEST
            // loading the in-cluster config, including:
            //   1. service-account CA
            //   2. service-account bearer-token
            //   3. service-account namespace
            //   4. master endpoints(ip, port) from pre-set environment variables
            ApiClient client = ClientBuilder.cluster().build();

            // set the global default api-client to the in-cluster one from above
            Configuration.setDefaultApiClient(client);

            // the CoreV1Api loads default api-client from global configuration.
            CoreV1Api api = new CoreV1Api();

            // invokes the CoreV1Api client
            V1PodList list =
                api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null);
            for (V1Pod item : list.getItems()) {
                System.out.println(item.getMetadata().getName());
            }
        } catch (ApiException | IOException ex) {
            logger.error("Caught exception: " + ex.toString(), ex);
        }
    }

}
