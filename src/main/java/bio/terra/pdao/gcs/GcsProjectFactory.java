package bio.terra.pdao.gcs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class GcsProjectFactory {
    private static ConcurrentHashMap<String, GcsProject> gcsProjectCache = new ConcurrentHashMap<>();

    private final GcsConfiguration gcsConfiguration;

    @Autowired
    public GcsProjectFactory(GcsConfiguration gcsConfiguration) {
        this.gcsConfiguration = gcsConfiguration;
    }

    public GcsProject get(String projectId) {
        if (!gcsProjectCache.containsKey(projectId)) {
            GcsProject gcsProject = new GcsProject(
                projectId,
                gcsConfiguration.getConnectTimeoutSeconds(),
                gcsConfiguration.getReadTimeoutSeconds());
            gcsProjectCache.putIfAbsent(projectId, gcsProject);
        }
        return gcsProjectCache.get(projectId);
    }
}
