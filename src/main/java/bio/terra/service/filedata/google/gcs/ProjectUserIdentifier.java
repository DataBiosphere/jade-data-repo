package bio.terra.service.filedata.google.gcs;

import java.util.Objects;
import java.util.Optional;

/** Class used to identify how a connection to a project should be made */
public class ProjectUserIdentifier {

  private final String projectId;
  private final Optional<String> userToImpersonate;

  public ProjectUserIdentifier(String projectId, String userToImpersonate) {
    this.projectId = projectId;
    this.userToImpersonate = Optional.ofNullable(userToImpersonate);
  }

  public String getProjectId() {
    return projectId;
  }

  public Optional<String> getUserToImpersonate() {
    return userToImpersonate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectUserIdentifier that = (ProjectUserIdentifier) o;
    return Objects.equals(projectId, that.projectId)
        && Objects.equals(userToImpersonate, that.userToImpersonate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, userToImpersonate);
  }
}
