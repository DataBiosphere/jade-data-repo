package bio.terra.service.filedata.google.gcs;

import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.google.cloud.storage.Storage;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GcsProjectFactory {
  private static final Map<ProjectUserIdentifier, GcsProject> gcsProjectCache =
      Collections.synchronizedMap(
          new PassiveExpiringMap<>(GcsProject.TOKEN_LENGTH.toSeconds(), TimeUnit.SECONDS));

  // TODO: cache table lookup
  private final GcsConfiguration gcsConfiguration;
  private final GoogleResourceDao googleResourceDao;

  @Autowired
  public GcsProjectFactory(GcsConfiguration gcsConfiguration, GoogleResourceDao googleResourceDao) {
    this.gcsConfiguration = gcsConfiguration;
    this.googleResourceDao = googleResourceDao;
  }

  public GcsProject get(String projectId) {
    return get(projectId, false);
  }

  public GcsProject get(String projectId, boolean asMainServiceAccount) {
    GoogleProjectResource googleProjectResource;

    ProjectUserIdentifier projectUserIdentifier;
    if (asMainServiceAccount
        || (googleProjectResource = googleResourceDao.retrieveProjectByGoogleProjectId(projectId))
            == null) {
      projectUserIdentifier = new ProjectUserIdentifier(projectId, null);
    } else {
      projectUserIdentifier =
          new ProjectUserIdentifier(projectId, googleProjectResource.getServiceAccount());
    }

    gcsProjectCache.computeIfAbsent(
        projectUserIdentifier,
        p ->
            new GcsProject(
                p,
                gcsConfiguration.getConnectTimeoutSeconds(),
                gcsConfiguration.getReadTimeoutSeconds()));
    return gcsProjectCache.get(projectUserIdentifier);
  }

  public Storage getStorage(String projectId) {
    return getStorage(projectId, false);
  }

  public Storage getStorage(String projectId, boolean asMainServiceAccount) {
    return get(projectId, asMainServiceAccount).getStorage();
  }
  //  public Storage getStorage(ProjectUserIdentifier projectUserIdentifier) {
  //    return get(projectUserIdentifier).getStorage();
  //  }
}
