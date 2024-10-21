package bio.terra.service.filedata.google.gcs;

import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.google.cloud.storage.Storage;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GcsProjectFactory {

  record ProjectUserIdentifier(String projectId, String userToImpersonate) {}

  private static final Map<ProjectUserIdentifier, GcsProject> gcsProjectCache =
      Collections.synchronizedMap(
          new PassiveExpiringMap<>(GcsProject.TOKEN_LENGTH.toSeconds(), TimeUnit.SECONDS));

  private static final int PROJECT_RESOURCES_CACHE_SIZE = 500;
  private static final Map<String, Optional<GoogleProjectResource>> projectResources =
      Collections.synchronizedMap(new LRUMap<>(PROJECT_RESOURCES_CACHE_SIZE));

  private final GcsConfiguration gcsConfiguration;
  private final GoogleResourceDao googleResourceDao;

  @Autowired
  public GcsProjectFactory(GcsConfiguration gcsConfiguration, GoogleResourceDao googleResourceDao) {
    this.gcsConfiguration = gcsConfiguration;
    this.googleResourceDao = googleResourceDao;
  }

  /**
   * Get a GcsProject object for the specified project authorized with a project-specific service
   * account
   *
   * @param projectId The project ID to get a GcsProject object for
   * @return GcsProject metadata object
   */
  public GcsProject get(String projectId) {
    return get(projectId, false);
  }

  /**
   * Get a GcsProject object for the specified project authorized with either a project-specific
   * service account or the TDR service account
   *
   * @param projectId The project ID to get a GcsProject object for
   * @param asMainServiceAccount If true, authorize using the main TDR service account
   * @return GcsProject metadata object
   */
  public GcsProject get(String projectId, boolean asMainServiceAccount) {
    GoogleProjectResource projectResource;

    ProjectUserIdentifier projectUserIdentifier;
    if (asMainServiceAccount || (projectResource = retrieveProjectResource(projectId)) == null) {
      projectUserIdentifier = new ProjectUserIdentifier(projectId, null);
    } else {
      projectUserIdentifier =
          new ProjectUserIdentifier(projectId, projectResource.getServiceAccount());
    }

    return gcsProjectCache.computeIfAbsent(
        projectUserIdentifier,
        p ->
            new GcsProject(
                p,
                gcsConfiguration.connectTimeoutSeconds(),
                gcsConfiguration.readTimeoutSeconds()));
  }

  private GoogleProjectResource retrieveProjectResource(String projectId) {
    return projectResources
        .computeIfAbsent(projectId, googleResourceDao::retrieveProjectByGoogleProjectIdMaybe)
        .orElse(null);
  }

  /**
   * Get a Storage object for the specified project authorized with a project-specific service
   * account
   *
   * @param projectId The project ID to get a Storage object for
   * @return Storage client for the specified project
   */
  public Storage getStorage(String projectId) {
    return getStorage(projectId, false);
  }

  /**
   * Get a Storage object for the specified project authorized with either a project-specific
   * service account or the TDR service account
   *
   * @param projectId The project ID to get a Storage object for
   * @param asMainServiceAccount If true, authorize using the main TDR service account
   * @return Storage client for the specified project
   */
  public Storage getStorage(String projectId, boolean asMainServiceAccount) {
    return get(projectId, asMainServiceAccount).getStorage();
  }
}
