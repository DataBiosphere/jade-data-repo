package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class SnapshotStorageAccountDaoTest {

  @MockBean private SnapshotStorageAccountDao snapshotStorageAccountDao;
  @MockBean private AzureStorageAccountService storageAccountService;
  @MockBean private AzureApplicationDeploymentService applicationDeploymentService;
  @Autowired private ResourceService resourceService;

  @Test
  void testCreateSnapshotAccountLink() throws Exception {
    String flightId = UUID.randomUUID().toString();
    UUID billingProfileId = UUID.randomUUID();
    UUID datasetId = UUID.randomUUID();
    UUID snapshotId = UUID.randomUUID();
    UUID azureStorageAccountResourceId = UUID.randomUUID();
    UUID azureApplicationDeploymentResourceId = UUID.randomUUID();
    BillingProfileModel billingProfile =
        new BillingProfileModel().profileName("profileName").id(billingProfileId);

    DatasetSummary summary =
        new DatasetSummary()
            .storage(
                List.of(
                    new AzureStorageResource(
                        datasetId,
                        AzureCloudResource.STORAGE_ACCOUNT,
                        AzureRegion.DEFAULT_AZURE_REGION)));
    Dataset dataset = new Dataset(summary);
    when(snapshotStorageAccountDao.getStorageAccountResourceIdForSnapshotId(snapshotId))
        .thenReturn(Optional.of(azureStorageAccountResourceId));
    when(storageAccountService.getStorageAccountResourceById(any(), anyBoolean()))
        .thenReturn(
            new AzureStorageAccountResource()
                .name("azureStorageAccount")
                .resourceId(azureStorageAccountResourceId)
                .profileId(billingProfileId)
                .region(AzureRegion.DEFAULT_AZURE_REGION));
    when(applicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile))
        .thenReturn(
            new AzureApplicationDeploymentResource()
                .id(azureApplicationDeploymentResourceId)
                .profileId(billingProfileId)
                .storageAccountPrefix("tdr"));
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource()
            .region(AzureRegion.DEFAULT_AZURE_REGION)
            .name("name")
            .profileId(billingProfileId)
            .resourceId(azureStorageAccountResourceId);
    when(storageAccountService.getOrCreateStorageAccount(any(), any(), any(), any(), any()))
        .thenReturn(storageAccountResource);

    AzureStorageAccountResource azureStorageAccountResource =
        resourceService.createSnapshotStorageAccount(
            snapshotId,
            dataset.getStorageAccountRegion(),
            billingProfile,
            flightId,
            dataset.isSecureMonitoringEnabled());

    assertThat(
        "Returns the new storage account resource",
        azureStorageAccountResource,
        is(storageAccountResource));
  }
}
