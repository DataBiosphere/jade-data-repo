package bio.terra.service.profile.google;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.exception.BillingServiceException;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.resourcenames.ResourceName;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.billing.v1.BillingAccountName;
import com.google.cloud.billing.v1.CloudBillingClient;
import com.google.cloud.billing.v1.CloudBillingSettings;
import com.google.cloud.billing.v1.ProjectBillingInfo;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("google")
public class GoogleBillingService {

  private static Logger logger = LoggerFactory.getLogger(GoogleBillingService.class);

  private static CloudBillingClient accessClient(AuthenticatedUserRequest user) {
    try {
      // Authentication is provided by the 'gcloud' tool when running locally
      // and by built-in service accounts when running on GAE, GCE, or GKE.
      GoogleCredentials serviceAccountCredentials =
          ServiceAccountCredentials.getApplicationDefault();

      // The createScopedRequired method returns true when running on GAE or a local developer
      // machine. In that case, the desired scopes must be passed in manually. When the code is
      // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
      // See https://developers.google.com/identity/protocols/application-default-credentials
      // for more information.
      if (serviceAccountCredentials.createScopedRequired()) {
        List<String> scopes = List.of("https://www.googleapis.com/auth/cloud-billing");
        serviceAccountCredentials = serviceAccountCredentials.createScoped(scopes);
      }

      //  If no user, use system credentials, otherwise use user credentials instead
      final String credentialName;
      final GoogleCredentials credentials;
      if (user == null || user.getToken().isEmpty()) {
        credentialName = "service account";
        credentials = serviceAccountCredentials;
      } else {
        credentialName = user.getEmail();
        credentials =
            GoogleCredentials.newBuilder()
                .setAccessToken(new AccessToken(user.getToken(), null))
                .build();
      }
      CloudBillingSettings cloudBillingSettings =
          CloudBillingSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      logger.info("Creating CloudBillingSettings with credentials: {}", credentialName);

      return CloudBillingClient.create(cloudBillingSettings);
    } catch (IOException e) {
      String message =
          String.format("Could not build Cloud Billing client instance: %s", e.getMessage());
      throw new BillingServiceException(message, e);
    }
  }

  private static CloudBillingClient accessClient() {
    return accessClient(null);
  }

  /**
   * Checks to see if a user has access to the billing account for this profile.
   *
   * <p>from: https://developers.google.com/apis-explorer/#p/cloudbilling/v1/
   *
   * <p>cloudbilling.billingAccounts.testIamPermissions Tests the access control policy for a
   * billing account. This method takes the resource and a set of permissions as input and returns
   * the subset of the input permissions that the caller is allowed for that resource.
   *
   * <p>from: https://cloud.google.com/billing/v1/how-tos/access-control from:
   * https://bit.ly/2TM2RPz (TestIamPermissionsRequest sample code)
   *
   * <p>In order to call projects.updateBillingInfo, the caller must have permissions
   * billing.resourceAssociations.create and resourcemanager.projects.createBillingAssignment on the
   * billing account.
   *
   * <p>The second permission is specific to projects, so we will check for the first permission
   * here.
   *
   * <p>The minimum scope required to verify permissions is:
   * https://www.googleapis.com/auth/cloud-billing.readonly
   *
   * @param user
   * @param billingAccountId
   * @return true if a user can act as a billing account *user* (viewer is not enough), false
   *     otherwise
   */
  public boolean canAccess(AuthenticatedUserRequest user, String billingAccountId) {
    ResourceName resource = BillingAccountName.of(billingAccountId);
    List<String> permissions = Collections.singletonList("billing.resourceAssociations.create");
    TestIamPermissionsRequest permissionsRequest =
        TestIamPermissionsRequest.newBuilder()
            .setResource(resource.toString())
            .addAllPermissions(permissions)
            .build();
    try (CloudBillingClient client = accessClient(user)) {
      logger.info("Testing IAM permission on billing account: {}", billingAccountId);
      TestIamPermissionsResponse response = client.testIamPermissions(permissionsRequest);
      List<String> actualPermissions = response.getPermissionsList();
      return actualPermissions != null && actualPermissions.equals(permissions);
    } catch (ApiException e) {
      int status = e.getStatusCode().getCode().getHttpStatusCode();
      if (status == 400 || status == 404) {
        // The billing account id is invalid or does not exist. This counts as inaccessible.
        return false;
      }
      String message =
          String.format("Could not check permissions on billing account '%s'", billingAccountId);
      throw new BillingServiceException(message, e);
    }
  }

  /**
   * Checks to see if the account the repository is running as has access to the billing account for
   * this profile.
   *
   * <p>See {@link GoogleBillingService#canAccess(AuthenticatedUserRequest, String) } for more
   * details.
   */
  public boolean repositoryCanAccess(String billingAccountId) {
    return canAccess(null, billingAccountId);
  }

  public boolean assignProjectBilling(
      BillingProfileModel billingProfile, GoogleProjectResource project) {
    String billingAccountId = billingProfile.getBillingAccountId();
    String projectId = project.getGoogleProjectId();
    ProjectBillingInfo content =
        ProjectBillingInfo.newBuilder()
            .setBillingAccountName("billingAccounts/" + billingAccountId)
            .build();
    try (CloudBillingClient cloudBillingClient = accessClient()) {
      ProjectBillingInfo billingResponse =
          cloudBillingClient.updateProjectBillingInfo("projects/" + projectId, content);
      return billingResponse.getBillingEnabled();
    } catch (ApiException e) {
      String message =
          String.format(
              "Could not assign billing account '%s' to project: %s", billingAccountId, projectId);
      throw new BillingServiceException(message, e);
    }
  }

  public ProjectBillingInfo getProjectBilling(String projectId) {
    try (CloudBillingClient cloudBillingClient = accessClient()) {
      return cloudBillingClient.getProjectBillingInfo("projects/" + projectId);
    } catch (ApiException e) {
      String message =
          String.format("Could not retrieve billing account to project: %s", projectId);
      throw new BillingServiceException(message, e);
    }
  }
}
