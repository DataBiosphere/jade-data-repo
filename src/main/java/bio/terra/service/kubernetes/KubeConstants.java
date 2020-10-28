package bio.terra.service.kubernetes;

public final class KubeConstants {
    private KubeConstants() { }

    static final String API_POD_FILTER = "datarepo-api";
    static final String KUBE_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";
    static final String KUBE_NAMESPACE_FILE = KUBE_DIR + "/namespace";
}
