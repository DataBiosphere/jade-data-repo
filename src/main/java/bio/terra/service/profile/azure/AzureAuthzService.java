package bio.terra.service.profile.azure;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class AzureAuthzService {

    static final String AUTH_TAG_KEY = "terra-auth";

    private final AzureResourceConfiguration resourceConfiguration;

    @Autowired
    public AzureAuthzService(AzureResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    public boolean canAccess(AuthenticatedUserRequest user,
                             UUID subscriptionId,
                             String resourceGroupName,
                             String applicationDeploymentName) {
        AzureResourceManager client = resourceConfiguration.getClient(subscriptionId);
        String applicationResourceId = MetadataDataAccessUtils.getApplicationDeploymentId(
                subscriptionId,
                resourceGroupName,
                applicationDeploymentName);
        try {
            GenericResource applicationDeployment = client.genericResources()
                .getById(applicationResourceId);

            return applicationDeployment.tags()
                .getOrDefault(AUTH_TAG_KEY, "")
                .strip().equalsIgnoreCase(user.getEmail());
        } catch (Exception e) {
            return false;
        }
    }
}
