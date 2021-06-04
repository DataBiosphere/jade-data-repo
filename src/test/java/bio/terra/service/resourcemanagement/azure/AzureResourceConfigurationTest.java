package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import com.azure.core.management.Region;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.storage.models.StorageAccount;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests direct interaction with Azure.  Note: to run these tests, the following environment variables must be set:
 * <UL>
 * <LI>AZURE_CREDENTIALS_APPLICATIONID</LI>
 * <LI>AZURE_CREDENTIALS_HOMETENANTID</LI>
 * <LI>AZURE_CREDENTIALS_SECRET</LI>
 * </UL>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"connectedtest", "google"})
@Category(Connected.class)
public class AzureResourceConfigurationTest {

    private final Logger logger = LoggerFactory.getLogger(AzureResourceConfigurationTest.class);

    @Autowired
    private AzureResourceConfiguration azureResourceConfiguration;

    @Autowired
    private ConnectedTestConfiguration connectedTestConfiguration;

    @Test
    public void testAbilityToCreateAndDeleteStorageAccount() {

        BillingProfileModel profileModel = createProfileModel();

        AzureResourceManager client = azureResourceConfiguration
            .getClient(UUID.fromString(profileModel.getTenantId()), UUID.fromString(profileModel.getSubscriptionId()));


        logger.info("Creating storage account...");
        // Create the storage account
        StorageAccount storageAccount = client.storageAccounts()
            .define("ct" + Instant.now().toEpochMilli())
            .withRegion(Region.US_CENTRAL)
            .withExistingResourceGroup(profileModel.getResourceGroupName())
            .create();

        logger.info("Getting storage account...");
        // Ensure we can get the storage account by not failing
        client.storageAccounts().getById(storageAccount.id());

        logger.info("Deleting storage account...");
        // Delete the storage account
        client.storageAccounts().deleteById(storageAccount.id());

        logger.info("Making sure storage account was deleted");
        // Make sure the storage account is deleted.  Timing affects what errors is thrown so wait until the final state
        // error is thrown.
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try {
                client.storageAccounts().getById(storageAccount.id());
            } catch (ManagementException e) {
                logger.info("Expected error:", e);
                assertThat(e.getValue().getCode(), equalTo("ResourceNotFound"));
                assertThat(
                    e.getMessage(),
                    containsString(String.format("The Resource 'Microsoft.Storage/storageAccounts/%s' under resource " +
                        "group '%s' was not found.", storageAccount.name(), storageAccount.resourceGroupName())));
            }
        });
    }

    private BillingProfileModel createProfileModel() {
        return new BillingProfileModel()
            .profileName("somename")
            .biller("direct")
            .cloudPlatform(CloudPlatform.AZURE)
            .tenantId(connectedTestConfiguration.getTargetTenantId().toString())
            .subscriptionId(connectedTestConfiguration.getTargetSubscriptionId().toString())
            .resourceGroupName(connectedTestConfiguration.getTargetResourceGroupName());
    }
}
