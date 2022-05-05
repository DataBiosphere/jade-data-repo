package bio.terra.service.resourcemanagement.google;

import bio.terra.common.AclUtils;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Binding;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Policy;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.SetIamPolicyRequest;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class GoogleResourceManagerService {
  private static final Logger logger = LoggerFactory.getLogger(GoogleResourceManagerService.class);

  private final Environment environment;
  private final GoogleResourceConfiguration resourceConfiguration;

  @Autowired
  public GoogleResourceManagerService(
      Environment environment, GoogleResourceConfiguration resourceConfiguration) {
    this.environment = environment;
    this.resourceConfiguration = resourceConfiguration;
  }

  // TODO: convert this to using the resource manager service interface instead of the api interface
  //  https://googleapis.dev/java/google-cloud-resourcemanager/latest/index.html
  //     ?com/google/cloud/resourcemanager/ResourceManager.html
  //  And use GoogleCredentials instead of the deprecated class. (DR-1459)
  public CloudResourceManager cloudResourceManager() throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(resourceConfiguration.getApplicationName())
        .build();
  }

  // package access for use in tests
  public Project getProject(String googleProjectId) {
    try {
      CloudResourceManager resourceManager = cloudResourceManager();
      CloudResourceManager.Projects.Get request = resourceManager.projects().get(googleProjectId);
      return request.execute();
    } catch (GoogleJsonResponseException e) {
      // if the project does not exist, the API will return a 403 unauth. to prevent people probing
      // for projects. We tolerate non-existent projects, because we want to be able to retry
      // failures on deleting other projects.
      if (e.getDetails().getCode() != 403) {
        throw new GoogleResourceException("Unexpected error while checking on project state", e);
      }
      return null;
    } catch (IOException | GeneralSecurityException e) {
      throw new GoogleResourceException("Could not check on project state", e);
    }
  }

  public void deleteProject(String googleProjectId) {
    try {
      CloudResourceManager resourceManager = cloudResourceManager();
      CloudResourceManager.Projects.Delete request =
          resourceManager.projects().delete(googleProjectId);
      // the response will be empty if the request is successful in the delete
      request.execute();
    } catch (IOException | GeneralSecurityException e) {
      if (e instanceof GoogleJsonResponseException
          && ((GoogleJsonResponseException) e)
              .getDetails()
              .getMessage()
              .equals("Cannot delete an inactive project.")) {
        logger.warn(
            String.format(
                "Project [%s] is already inactive and so will not be deleted", googleProjectId),
            e);
        return;
      }
      throw new GoogleResourceException("Could not delete project", e);
    }
  }

  // Set permissions on a project
  public void updateIamPermissions(
      Map<String, List<String>> userPermissions,
      String projectId,
      GoogleProjectService.PermissionOp permissionOp)
      throws InterruptedException {

    // Nothing to do if no permissions updates are requested
    if (userPermissions == null || userPermissions.size() == 0) {
      return;
    }

    GetIamPolicyRequest getIamPolicyRequest = new GetIamPolicyRequest();

    AclUtils.aclUpdateRetry(
        () -> {
          try {
            CloudResourceManager resourceManager = cloudResourceManager();
            Policy policy =
                resourceManager.projects().getIamPolicy(projectId, getIamPolicyRequest).execute();
            final List<Binding> bindingsList = policy.getBindings();

            switch (permissionOp) {
              case ENABLE_PERMISSIONS:
                for (var entry : userPermissions.entrySet()) {
                  Binding binding =
                      new Binding().setRole(entry.getKey()).setMembers(entry.getValue());
                  bindingsList.add(binding);
                }
                break;

              case REVOKE_PERMISSIONS:
                // Remove members from the current policies
                for (var entry : userPermissions.entrySet()) {
                  CollectionUtils.filter(
                      bindingsList,
                      b -> {
                        if (Objects.equals(b.getRole(), entry.getKey())) {
                          // Remove the members that were passed in
                          b.setMembers(ListUtils.subtract(b.getMembers(), entry.getValue()));
                          // Remove any entries from the bindings list with no members
                          return !b.getMembers().isEmpty();
                        }
                        return true;
                      });
                }
            }

            policy.setBindings(bindingsList);
            SetIamPolicyRequest setIamPolicyRequest = new SetIamPolicyRequest().setPolicy(policy);
            resourceManager.projects().setIamPolicy(projectId, setIamPolicyRequest).execute();
            return null;
          } catch (IOException | GeneralSecurityException ex) {
            throw new AclUtils.AclRetryException(
                "Encountered an error while updating IAM permissions", ex, ex.getMessage());
          }
        });
  }

  public void addLabelsToProject(String googleProjectId, Map<String, String> labels) {
    final Stream<Map.Entry<String, String>> additionalLabels;
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(env -> env.contains("test") || env.contains("int"))) {
      additionalLabels = Stream.of(Map.entry("project-for-test", "true"));
    } else {
      additionalLabels = Stream.empty();
    }

    try {
      CloudResourceManager resourceManager = cloudResourceManager();
      Project project = resourceManager.projects().get(googleProjectId).execute();
      Map<String, String> cleanedLabels =
          Stream.concat(
                  Stream.concat(project.getLabels().entrySet().stream(), additionalLabels),
                  labels.entrySet().stream()
                      .map(
                          e -> Map.entry(cleanForLabels(e.getKey()), cleanForLabels(e.getValue()))))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1));
      project.setLabels(cleanedLabels);
      logger.info("Adding labels to project {}", googleProjectId);
      resourceManager.projects().update(googleProjectId, project).execute();
    } catch (Exception ex) {
      // only a soft failure - we do not want to fail project create just on adding project labels
      logger.warn("Encountered error while updating project labels. ex: {}, stacktrace: {}", ex);
    }
  }

  /**
   * Google requires labels to consist of only lowercase, alphanumeric, "_", and "-" characters.
   * This value can be at most 63 characters long.
   *
   * @param string String to clean
   * @return The cleaned String
   */
  @VisibleForTesting
  static String cleanForLabels(String string) {
    return string
        .toLowerCase(Locale.ROOT)
        .trim()
        .replaceAll("[^a-z0-9_-]", "-")
        .substring(0, Math.min(string.length(), 63));
  }
}
