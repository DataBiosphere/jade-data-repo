package bio.terra.service.google;

import bio.terra.metadata.BillingProfile;
import bio.terra.service.BillingService;
import bio.terra.service.exception.BillingServiceException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudbilling.Cloudbilling;
import com.google.api.services.cloudbilling.model.TestIamPermissionsRequest;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

@Service
@Profile("google")
public class GoogleBillingService implements BillingService {

    private static Cloudbilling cloudbilling() {
        try {
            // Authentication is provided by the 'gcloud' tool when running locally
            // and by built-in service accounts when running on GAE, GCE, or GKE.
            GoogleCredential credential = GoogleCredential.getApplicationDefault();

            // The createScopedRequired method returns true when running on GAE or a local developer
            // machine. In that case, the desired scopes must be passed in manually. When the code is
            // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
            // See https://developers.google.com/identity/protocols/application-default-credentials
            // for more information.
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(
                    Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            return new Cloudbilling.Builder(httpTransport, jsonFactory, credential)
                .setApplicationName("jade-data-repository")
                .build();
        } catch (IOException | GeneralSecurityException e) {
            String message = String.format("Could not build Cloudbilling instance: %s", e.getMessage());
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
        String accountId = "billingAccounts/" + billingProfile.getBillingAccountId();
        List<String> permissions = Collections.singletonList("billing.resourceAssociations.create");
        TestIamPermissionsRequest permissionsRequest = new TestIamPermissionsRequest().setPermissions(permissions);
        try {
            Cloudbilling.BillingAccounts.TestIamPermissions testIamPermissions =
                cloudbilling().billingAccounts().testIamPermissions(accountId, permissionsRequest);
            List<String> actualPermissions = testIamPermissions.execute().getPermissions();
            return actualPermissions != null && actualPermissions.equals(permissions);
        } catch (GoogleJsonResponseException e) {
            int status = e.getStatusCode();
            if (status == 400 || status == 404) {
                // The billing account id is invalid or does not exist. This counts as inaccessible.
                return false;
            }
            String message = String.format("%s Error, Could not check permissions on: %s", status, accountId);
            throw new BillingServiceException(message, e);
        } catch (IOException e) {
            String message = String.format("Could not check permissions on: %s", accountId);
            throw new BillingServiceException(message, e);
        }
    }
}
