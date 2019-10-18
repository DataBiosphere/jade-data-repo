package bio.terra.service.filedata.google.gcs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class GcsProjectFactory {
    private static final ConcurrentHashMap<String, GcsProject> gcsProjectCache = new ConcurrentHashMap<>();

    private final GcsConfiguration gcsConfiguration;

    @Autowired
    public GcsProjectFactory(GcsConfiguration gcsConfiguration) {
        this.gcsConfiguration = gcsConfiguration;
    }

    public GcsProject get(String projectId) {
        gcsProjectCache.computeIfAbsent(projectId, p -> new GcsProject(
                p,
                gcsConfiguration.getConnectTimeoutSeconds(),
                gcsConfiguration.getReadTimeoutSeconds()));
        return gcsProjectCache.get(projectId);
    }
}
