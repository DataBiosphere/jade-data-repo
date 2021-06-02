package bio.terra.service.tabulardata.google;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Given a projectId, returns a {@link BigQueryProject} object while caching
 */
@Component
public class BigQueryProjectProvider implements Function<String, BigQueryProject> {
    private static final ConcurrentHashMap<String, BigQueryProject> BQ_PROJECT_CACHE = new ConcurrentHashMap<>();

    @Override
    public BigQueryProject apply(String projectId) {
        BQ_PROJECT_CACHE.computeIfAbsent(projectId, BigQueryProject::new);
        return BQ_PROJECT_CACHE.get(projectId);
    }

}
