package bio.terra.service.resourcemanagement.google;

import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.BillingService;
import bio.terra.service.resourcemanagement.exception.BillingServiceException;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.resourcenames.ResourceName;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.billing.v1.BillingAccountName;
import com.google.cloud.billing.v1.CloudBillingClient;
import com.google.cloud.billing.v1.CloudBillingSettings;
import com.google.cloud.billing.v1.ProjectBillingInfo;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Service
@Profile("google")
public class GoogleBillingService implements BillingService {

    private static CloudBillingClient cloudBillingClient() {
        try {
            // Authentication is provided by the 'gcloud' tool when running locally
            // and by built-in service accounts when running on GAE, GCE, or GKE.
            GoogleCredentials credentials = ServiceAccountCredentials.getApplicationDefault();

            // The createScopedRequired method returns true when running on GAE or a local developer
            // machine. In that case, the desired scopes must be passed in manually. When the code is
            // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
            // See https://developers.google.com/identity/protocols/application-default-credentials
            // for more information.
            List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");
            if (credentials.createScopedRequired()) {
                credentials = credentials.createScoped(scopes);
            }

            // Use these credentials for the CloudBillingClient
            CloudBillingSettings cloudBillingSettings =
                CloudBillingSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();

            return CloudBillingClient.create(cloudBillingSettings);
        } catch (IOException e) {
            String message = String.format("Could not build Cloud Billing client instance: %s", e.getMessage());
            throw new BillingServiceException(message, e);
        }
    }

    /**
     * Checks to see if the account the repository is running as has access to the billing account for this profile.
     *
     * from: https://developers.google.com/apis-explorer/#p/cloudbilling/v1/
     *
     * cloudbilling.billingAccounts.testIamPermissions	Tests the access control policy for a billing account. This
     * method takes the resource and a set of permissions as input and returns the subset of the input permissions that
     * the caller is allowed for that resource.
     *
     * from: https://cloud.google.com/billing/v1/how-tos/access-control
     * from: https://bit.ly/2TM2RPz (TestIamPermissionsRequest sample code)
     *
     * In order to call projects.updateBillingInfo, the caller must have permissions billing.resourceAssociations.create
     * and resourcemanager.projects.createBillingAssignment on the billing account.
     *
     * The second permission is specific to projects, so we will check for the first permission here.
     *
     * @param billingProfile
     * @return true if the repository can act as a billing account *user* (viewer is not enough), false otherwise
     */
    @Override
    public boolean canAccess(BillingProfile billingProfile) {
        String accountId = billingProfile.getBillingAccountId();
        ResourceName resource = BillingAccountName.of(accountId);
        List<String> permissions = Collections.singletonList("billing.resourceAssociations.create");
        TestIamPermissionsRequest permissionsRequest = TestIamPermissionsRequest.newBuilder()
            .setResource(resource.toString())
            .addAllPermissions(permissions)
            .build();
        try {
            TestIamPermissionsResponse response = cloudBillingClient().testIamPermissions(permissionsRequest);
            List<String> actualPermissions = response.getPermissionsList();
            return actualPermissions != null && actualPermissions.equals(permissions);
        } catch (ApiException e) {
            int status = e.getStatusCode().getCode().getHttpStatusCode();
            if (status == 400 || status == 404) {
                // The billing account id is invalid or does not exist. This counts as inaccessible.
                return false;
            }
            String message = String.format("ERROR: Could not check permissions on `%s`: %s",
                resource.toString(),
                e.getMessage());
            throw new BillingServiceException(message, e);
        }
    }

    public boolean assignProjectBilling(BillingProfile billingProfile, GoogleProjectResource project) {
        String billingAccountId = billingProfile.getBillingAccountId();
        String projectId = project.getGoogleProjectId();
        ProjectBillingInfo content = ProjectBillingInfo.newBuilder()
            .setBillingAccountName("billingAccounts/" + billingAccountId)
            .build();
        try {
            ProjectBillingInfo billingResponse = cloudBillingClient()
                .updateProjectBillingInfo("projects/" + projectId, content);
            return billingResponse.getBillingEnabled();
        } catch (ApiException e) {
            String message = String.format("Could not assign billing account '%s' to project: %s", billingAccountId,
                projectId);
            throw new BillingServiceException(message, e);
        }
    }
}
