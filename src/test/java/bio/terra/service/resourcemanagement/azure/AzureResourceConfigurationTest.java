package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.OnDemand;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.filedata.azure.tables.TableFileDao;
import bio.terra.service.resourcemanagement.AzureDataLocationSelector;
import bio.terra.stairway.ShortUUID;
import com.azure.core.management.Region;
import com.azure.core.management.exception.ManagementException;
import com.azure.core.util.Context;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.Deployment;
import com.azure.resourcemanager.resources.models.DeploymentMode;
import com.azure.resourcemanager.resources.models.ResourceReference;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.FileSystemSasPermission;
import io.micrometer.core.instrument.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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

/**
 * Tests direct interaction with Azure. Note: to run these tests, the following environment
 * variables must be set:
 *
 * <UL>
 *   <LI>AZURE_CREDENTIALS_APPLICATIONID
 *   <LI>AZURE_CREDENTIALS_HOMETENANTID
 *   <LI>AZURE_CREDENTIALS_SECRET
 * </UL>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"connectedtest", "google"})
@Category(OnDemand.class)
public class AzureResourceConfigurationTest {

  private final Logger logger = LoggerFactory.getLogger(AzureResourceConfigurationTest.class);

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  @Autowired private TableFileDao tableFileDao;

  @Test
  public void testAbilityToCreateAndDeleteStorageAccount() {
    BillingProfileModel profileModel = createProfileModel();

    AzureResourceManager client =
        azureResourceConfiguration.getClient(
            profileModel.getTenantId(), profileModel.getSubscriptionId());

    logger.info("Creating storage account...");
    // Create the storage account
    StorageAccount storageAccount =
        client
            .storageAccounts()
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
    // Make sure the storage account is deleted.  Timing affects what errors is thrown so wait until
    // the final state
    // error is thrown.
    Awaitility.waitAtMost(5, TimeUnit.MINUTES)
        .pollInterval(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try {
                client.storageAccounts().getById(storageAccount.id());
              } catch (ManagementException e) {
                logger.info("Expected error:", e);
                assertThat(
                    "Deleted storage account isn't found",
                    e.getMessage(),
                    containsString("Status code 404"));
              }
            });
  }

  @Test
  public void testAbilityToDeployApplication() {
    BillingProfileModel profileModel = createProfileModel();

    AzureResourceManager client =
        azureResourceConfiguration.getClient(
            profileModel.getTenantId(), profileModel.getSubscriptionId());

    logger.info("Deploying managed application...");
    ManagedApplicationDeployment applicationDeployment =
        createManagedApplication(client, profileModel);

    logger.info("Creating a storage account in the managed application...");
    String storageAccountName =
        "sa"
            + AzureDataLocationSelector.armUniqueString(
                applicationDeployment.applicationDeploymentName, 13);
    String fileSystemName =
        "f"
            + AzureDataLocationSelector.armUniqueString(
                applicationDeployment.applicationDeploymentName, 13);
    // Note that this fails (e.g. authenticating using the end user's tenant
    assertThat(
        "Get expected error",
        assertThrows(
                ManagementException.class,
                () -> {
                  client
                      .storageAccounts()
                      .define(storageAccountName)
                      .withRegion(Region.US_NORTH_CENTRAL)
                      .withExistingResourceGroup(applicationDeployment.applicationResourceGroup)
                      .withHnsEnabled(true)
                      .create();
                })
            .getMessage(),
        containsString("Status code 403"));

    // ...but authenticating with the home tenant does work
    assertDoesNotThrow(
        () -> {
          AzureResourceManager clientFromHome =
              azureResourceConfiguration.getClient(
                  azureResourceConfiguration.credentials().getHomeTenantId(),
                  profileModel.getSubscriptionId());

          clientFromHome
              .storageAccounts()
              .define(storageAccountName)
              .withRegion(Region.US_NORTH_CENTRAL)
              .withExistingResourceGroup(applicationDeployment.applicationResourceGroup)
              .withHnsEnabled(true)
              .create();
        });

    // Authentication from the tenant where the TDR is deployed...fails
    assertThat(
        "Get expected error",
        assertThrows(
                DataLakeStorageException.class,
                () -> {
                  DataLakeFileSystemClient fileSystemClient =
                      new DataLakeServiceClientBuilder()
                          .credential(azureResourceConfiguration.getAppToken())
                          .endpoint(applicationDeployment.storageEndpoint)
                          .buildClient()
                          .createFileSystem(fileSystemName);

                  // Perform file operations
                  createFileAndSign(fileSystemClient);
                })
            .getMessage(),
        containsString("Status code 401"));

    // Authentication from the target (user tenant)...fails
    assertThat(
        "Get expected error",
        assertThrows(
                DataLakeStorageException.class,
                () -> {
                  DataLakeFileSystemClient fileSystemClient =
                      new DataLakeServiceClientBuilder()
                          .credential(
                              azureResourceConfiguration.getAppToken(
                                  connectedTestConfiguration.getTargetTenantId()))
                          .endpoint(applicationDeployment.storageEndpoint)
                          .buildClient()
                          .getFileSystemClient(fileSystemName);

                  // Perform file operations
                  createFileAndSign(fileSystemClient);
                })
            .getMessage(),
        containsString("Status code 403"));

    // Using the resource management API to get a key then use that to auth does work
    assertDoesNotThrow(
        () -> {
          // Need to keep retrying for ACLs to propagate on newly created application
          String key =
              Awaitility.waitAtMost(5, TimeUnit.MINUTES)
                  .until(
                      () -> {
                        // Create a resource manager client using the home tenant and the target
                        // user's subscription
                        AzureResourceManager clientFromHome =
                            azureResourceConfiguration.getClient(
                                azureResourceConfiguration.credentials().getHomeTenantId(),
                                profileModel.getSubscriptionId());

                        try {
                          // Get a key from the newly created storage account
                          return clientFromHome
                              .storageAccounts()
                              .getByResourceGroup(
                                  applicationDeployment.applicationDeploymentName,
                                  storageAccountName)
                              .getKeys()
                              .get(0)
                              .value();
                        } catch (DataLakeStorageException | ManagementException e) {
                          // Changes have not propagated yet
                          logger.info("Waiting for ACLs to propagate");
                          return null;
                        }
                      },
                      k -> !Objects.isNull(k));

          // Create a data lake client by authenticating using the found key
          DataLakeFileSystemClient fileSystemClient =
              new DataLakeServiceClientBuilder()
                  .credential(new StorageSharedKeyCredential(storageAccountName, key))
                  .endpoint("https://" + storageAccountName + ".blob.core.windows.net")
                  .buildClient()
                  .createFileSystem(fileSystemName);

          // Perform file operations
          createFileAndSign(fileSystemClient);
        });

    deleteManagedApplication(client, applicationDeployment);
  }

  private void createFileAndSign(DataLakeFileSystemClient fileSystemClient) throws IOException {
    String filePath = "some/file.txt";
    logger.info("Creating file...");
    DataLakeFileClient file = fileSystemClient.createFile(filePath);
    String dataToUpload = "hello azure!";
    file.appendWithResponse(
        new ByteArrayInputStream(dataToUpload.getBytes(StandardCharsets.UTF_8)),
        0,
        dataToUpload.length(),
        DigestUtils.md5(dataToUpload),
        null,
        null,
        Context.NONE);
    file.flush(dataToUpload.length());

    DataLakeFileClient fileClient = fileSystemClient.getFileClient(filePath);
    assertThat("File exists", fileClient.exists(), equalTo(true));

    // Generate a Sas token
    logger.info("Generating Sas token...");
    String sasToken = generateSas(fileClient);
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      String fileUrl = String.format("%s?%s", fileClient.getFileUrl(), sasToken);
      logger.info("Validating File Url {}...", fileUrl);
      HttpUriRequest request = new HttpHead(fileUrl);
      try (CloseableHttpResponse response = client.execute(request)) {
        assertThat("Sas is valid", response.getStatusLine().getStatusCode(), equalTo(200));
      }
    }
  }

  private BillingProfileModel createProfileModel() {
    return new BillingProfileModel()
        .profileName("somename")
        .biller("direct")
        .cloudPlatform(CloudPlatform.AZURE)
        .tenantId(connectedTestConfiguration.getTargetTenantId())
        .subscriptionId(connectedTestConfiguration.getTargetSubscriptionId())
        .resourceGroupName(connectedTestConfiguration.getTargetResourceGroupName());
  }

  /**
   * Deploy a managed application in a subscription defined by the profileModel.
   *
   * <p>Note that this should be moved into a common lib
   */
  private ManagedApplicationDeployment createManagedApplication(
      AzureResourceManager client, BillingProfileModel profileModel) {
    String deploymentName = "tdr" + ShortUUID.get();
    String rgId =
        "/subscriptions/" + profileModel.getSubscriptionId() + "/resourceGroups/" + deploymentName;
    Map<String, Object> parameters =
        Map.of(
            "storageAccountNamePrefix",
            "tdr1",
            "storageAccountType",
            "Standard_LRS",
            "location",
            Region.US_CENTRAL,
            "applicationResourceName",
            deploymentName,
            "managedResourceGroupId",
            rgId);

    final String template;
    try (InputStream stream =
        getClass()
            .getClassLoader()
            .getResourceAsStream("azure/template/managedApplicationTemplate.json")) {
      template = IOUtils.toString(stream);
    } catch (IOException e) {
      throw new RuntimeException("Problem reading resource", e);
    }

    // TODO, right now this is a manual process but there is a rest endpoint to do programatically
    // accept terms.
    // This should likely be implemented in the Azure SDK (see:
    // https://github.com/Azure/azure-sdk-for-java/blob/master/sdk/marketplaceordering/  \
    // mgmt-v2015_06_01/src/main/java/com/microsoft/azure/management/marketplaceordering/v2015_06_01/ \
    // implementation/MarketplaceAgreementsInner.java)

    try {
      Deployment deployment =
          client
              .deployments()
              .define(deploymentName)
              .withExistingResourceGroup(profileModel.getResourceGroupName())
              .withTemplate(template)
              .withParameters(
                  parameters.entrySet().stream()
                      .collect(
                          Collectors.toMap(Map.Entry::getKey, e -> Map.of("value", e.getValue()))))
              .withMode(DeploymentMode.INCREMENTAL)
              .create();

      ResourceReference resourceReference = deployment.outputResources().get(0);

      Map<String, Map<String, String>> outputs =
          ((Map<String, Map<String, Map<String, String>>>)
                  client
                      .genericResources()
                      .getById(resourceReference.id(), azureResourceConfiguration.apiVersion())
                      .properties())
              .get("outputs");

      String storageEndpoint = outputs.get("storageEndpoint").get("value");

      String storageAccountName = outputs.get("storageAccountName").get("value");

      String fileSystemName = outputs.get("fileSystemName").get("value");

      logger.info(
          "Created application group {}, with container {}/{}",
          deploymentName,
          storageEndpoint,
          fileSystemName);

      return new ManagedApplicationDeployment(
          resourceReference.id(),
          deploymentName,
          deploymentName,
          storageEndpoint,
          storageAccountName,
          fileSystemName);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse template", e);
    }
  }

  private void deleteManagedApplication(
      AzureResourceManager client, ManagedApplicationDeployment applicationDeployment) {
    logger.info("Deleting the managed application deployment");
    client.deployments().deleteById(applicationDeployment.applicationDeploymentId);

    logger.info("Deleting the managed application");
    client.genericResources().deleteById(applicationDeployment.applicationDeploymentId);
  }

  /** Stores information relevant to an application's deployment */
  private static class ManagedApplicationDeployment {
    // The Azure resource Id
    private final String applicationDeploymentId;
    // The name given to the deployment
    private final String applicationDeploymentName;
    // The resource group where the application is deployed
    private final String applicationResourceGroup;
    // The endpoint to use to connect to the storage account created when deploying the application
    private final String storageEndpoint;
    // The name of the storage account
    private final String storageAccountName;
    // The name of the filesystem created when deploying the application
    private final String fileSystemName;

    ManagedApplicationDeployment(
        String applicationDeploymentId,
        String applicationDeploymentName,
        String applicationResourceGroup,
        String storageEndpoint,
        String storageAccountName,
        String fileSystemName) {
      this.applicationDeploymentId = applicationDeploymentId;
      this.applicationDeploymentName = applicationDeploymentName;
      this.applicationResourceGroup = applicationResourceGroup;
      this.storageEndpoint = storageEndpoint;
      this.storageAccountName = storageAccountName;
      this.fileSystemName = fileSystemName;
    }
  }

  private String generateSas(DataLakeFileClient client) {
    OffsetDateTime expiryTime = OffsetDateTime.now().plusHours(1);
    FileSystemSasPermission permission =
        new FileSystemSasPermission().setReadPermission(true).setWritePermission(false);

    // Note: can't use user delegation since that's not supported cross tenant
    // Note: the version is so that subdirectory sharing works...docs say it's a no-op, but it sure
    // isn't...
    return client.generateSas(
        new DataLakeServiceSasSignatureValues(expiryTime, permission)
            // Version is set to a version of the token signing API the supports keys that permit
            // listing files
            .setVersion("2020-04-08"));
  }
}
