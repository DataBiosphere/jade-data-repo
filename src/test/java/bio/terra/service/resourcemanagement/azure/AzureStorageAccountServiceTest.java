package bio.terra.service.resourcemanagement.azure;

import static bio.terra.service.filedata.azure.util.AzureConstants.RESOURCE_NOT_FOUND_CODE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.AzureStorageAccountSkuType;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.exception.StorageAccountLockException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.stairway.ShortUUID;
import com.azure.core.management.exception.ManagementError;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.resourcemanager.storage.models.StorageAccounts;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureStorageAccountServiceTest {
  private static final String MANAGED_GROUP_NAME = "mgd-grp-1";
  private static final String STORAGE_ACCOUNT_NAME = "sa1";
  private static final String APPLICATION_DEPLOYMENT_NAME = "appdeployment";
  private static final String COLLECTION_ID = UUID.randomUUID().toString();
  private static final AzureRegion REGION = AzureRegion.CENTRAL_US;
  private static final AzureStorageAccountSkuType STORAGE_SKU_TYPE =
      AzureStorageAccountSkuType.STANDARD_LRS;

  @Mock private AzureResourceDao resourceDao;
  @Mock private AzureResourceConfiguration resourceConfiguration;
  @Mock private ProfileDao profileDao;
  @Mock private StorageAccounts storageAccounts;
  @Mock private StorageAccount storageAccount;

  private BillingProfileModel billingProfileModel;
  private AzureApplicationDeploymentResource applicationResource;

  private AzureStorageAccountService service;

  @BeforeEach
  void setUp() {
    service = new AzureStorageAccountService(resourceDao, resourceConfiguration, profileDao);

    billingProfileModel = ProfileFixtures.randomAzureBillingProfile();
    applicationResource =
        new AzureApplicationDeploymentResource()
            .profileId(billingProfileModel.getId())
            .azureResourceGroupName(MANAGED_GROUP_NAME)
            .azureApplicationDeploymentName(APPLICATION_DEPLOYMENT_NAME)
            .storageAccountSkuType(STORAGE_SKU_TYPE);
  }

  private void mockApis() {
    when(profileDao.getBillingProfileById(billingProfileModel.getId()))
        .thenReturn(billingProfileModel);
    // Mock Azure client calls
    AzureResourceManager client = mock(AzureResourceManager.class);
    when(client.storageAccounts()).thenReturn(storageAccounts);
    when(resourceConfiguration.getClient(billingProfileModel.getSubscriptionId()))
        .thenReturn(client);
  }

  @Test
  void testGetStorageAccountResourceById() {
    UUID storageAccountId = UUID.randomUUID();
    service.getStorageAccountResourceById(storageAccountId, false);
    verify(resourceDao, times(1)).retrieveStorageAccountById(storageAccountId);
  }

  @Test
  void testGetStorageAccountResourceByIdAndCheckCloudFound() {
    mockApis();
    UUID storageAccountId = UUID.randomUUID();
    when(resourceDao.retrieveStorageAccountById(storageAccountId))
        .thenReturn(
            new AzureStorageAccountResource()
                .profileId(billingProfileModel.getId())
                .applicationResource(applicationResource)
                .name(STORAGE_ACCOUNT_NAME));

    when(storageAccounts.getByResourceGroup(MANAGED_GROUP_NAME, STORAGE_ACCOUNT_NAME))
        .thenReturn(storageAccount);
    assertDoesNotThrow(() -> service.getStorageAccountResourceById(storageAccountId, true));
  }

  @Test
  void testGetStorageAccountResourceByIdAndCheckCloudNotFound() {
    mockApis();
    UUID storageAccountId = UUID.randomUUID();
    when(resourceDao.retrieveStorageAccountById(storageAccountId))
        .thenReturn(
            new AzureStorageAccountResource()
                .profileId(billingProfileModel.getId())
                .applicationResource(applicationResource)
                .name(STORAGE_ACCOUNT_NAME));

    mockStorageAccountNotFound();

    assertThrows(
        CorruptMetadataException.class,
        () -> service.getStorageAccountResourceById(storageAccountId, true));
  }

  @Test
  void tesGetOrCreateStorageAccountCase1() throws InterruptedException {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource));

    when(storageAccounts.getByResourceGroup(MANAGED_GROUP_NAME, STORAGE_ACCOUNT_NAME))
        .thenReturn(storageAccount);

    service.getOrCreateStorageAccount(
        STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);

    // Create finish doesn't get called
    verify(resourceDao, never()).unlockStorageAccount(any(), any(), any());
  }

  @Test
  void tesGetOrCreateStorageAccountCase2() {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource)
                // Locked by a different flight
                .flightId(ShortUUID.get()));

    when(storageAccounts.getByResourceGroup(MANAGED_GROUP_NAME, STORAGE_ACCOUNT_NAME))
        .thenReturn(storageAccount);

    assertThrows(
        StorageAccountLockException.class,
        () ->
            service.getOrCreateStorageAccount(
                STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight),
        "Storage account locked by flightId: " + flight);
  }

  @Test
  void tesGetOrCreateStorageAccountCase3() throws InterruptedException {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource)
                .flightId(flight));

    when(storageAccounts.getByResourceGroup(MANAGED_GROUP_NAME, STORAGE_ACCOUNT_NAME))
        .thenReturn(storageAccount);

    service.getOrCreateStorageAccount(
        STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);

    // Called by createFinish
    verify(resourceDao, times(1)).unlockStorageAccount(STORAGE_ACCOUNT_NAME, COLLECTION_ID, flight);
  }

  @Test
  void tesGetOrCreateStorageAccountCase5() {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource));

    mockStorageAccountNotFound();

    assertThrows(
        CorruptMetadataException.class,
        () ->
            service.getOrCreateStorageAccount(
                STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight),
        "Storage account does not exist, metadata out of sync with cloud state: "
            + STORAGE_ACCOUNT_NAME);
  }

  @Test
  void tesGetOrCreateStorageAccountCase6() {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource)
                // Locked by a different flight
                .flightId(ShortUUID.get()));

    mockStorageAccountNotFound();

    assertThrows(
        StorageAccountLockException.class,
        () ->
            service.getOrCreateStorageAccount(
                STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight),
        "Storage account locked by flightId: " + flight);
  }

  @Test
  void tesGetOrCreateStorageAccountCase7() throws InterruptedException {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource)
                .region(REGION)
                // Locked by the same flight (something went wrong and storage account needs to be
                // created for real)
                .flightId(flight));
    mockStorageAccountNotFound();
    mockStorageAccountCreation();

    service.getOrCreateStorageAccount(
        STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);

    // Called by createFinish
    verify(resourceDao, times(1)).unlockStorageAccount(STORAGE_ACCOUNT_NAME, COLLECTION_ID, flight);
  }

  @Test
  void tesGetOrCreateStorageAccountCase8() throws InterruptedException {
    mockApis();
    String flight = ShortUUID.get();
    when(resourceDao.getStorageAccount(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, APPLICATION_DEPLOYMENT_NAME))
        .thenReturn(null);
    mockStorageAccountNotFound();
    mockStorageAccountCreation();
    // Mock creating the metadata record
    when(resourceDao.createAndLockStorage(
            STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight))
        .thenReturn(
            new AzureStorageAccountResource()
                .name(STORAGE_ACCOUNT_NAME)
                .applicationResource(applicationResource)
                .region(REGION)
                // Locked by this flight flight
                .flightId(flight));
    service.getOrCreateStorageAccount(
        STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);

    // Called by createFinish
    verify(resourceDao, times(1)).unlockStorageAccount(STORAGE_ACCOUNT_NAME, COLLECTION_ID, flight);
  }

  private void mockStorageAccountNotFound() {
    when(storageAccounts.getByResourceGroup(MANAGED_GROUP_NAME, STORAGE_ACCOUNT_NAME))
        .thenThrow(
            new ManagementException(
                "Could not find storage account",
                null,
                new ManagementError(RESOURCE_NOT_FOUND_CODE, "Couldn't find resource")));
  }

  /** Mocks the fluent interface needed to create a storage account with the Azure client */
  private void mockStorageAccountCreation() {
    StorageAccount.DefinitionStages.Blank withRegion =
        mock(StorageAccount.DefinitionStages.Blank.class);
    StorageAccount.DefinitionStages.WithGroup withGroup =
        mock(StorageAccount.DefinitionStages.WithGroup.class);
    StorageAccount.DefinitionStages.WithCreate withSku =
        mock(StorageAccount.DefinitionStages.WithCreate.class);
    StorageAccount.DefinitionStages.WithCreate withHns =
        mock(StorageAccount.DefinitionStages.WithCreate.class);
    StorageAccount.DefinitionStages.WithCreate withCreate =
        mock(StorageAccount.DefinitionStages.WithCreate.class);
    doReturn(storageAccount).when(withCreate).create();
    doReturn(withCreate).when(withHns).withHnsEnabled(true);
    doReturn(withHns).when(withSku).withSku(STORAGE_SKU_TYPE.getValue());
    doReturn(withSku).when(withGroup).withExistingResourceGroup(MANAGED_GROUP_NAME);
    doReturn(withGroup).when(withRegion).withRegion(REGION.getValue());
    doReturn(withRegion).when(storageAccounts).define(STORAGE_ACCOUNT_NAME);
  }

  @Test
  void listStorageAccountPerAppDeployment() {
    UUID appId = UUID.randomUUID();
    List<AzureStorageAccountResource> storageAccounts =
        List.of(
            new AzureStorageAccountResource().name("StorageAccount1"),
            new AzureStorageAccountResource().name("StorageAccount2"));
    when(resourceDao.retrieveStorageAccountsByApplicationResource(appId, true))
        .thenReturn(storageAccounts);
    var returnedStorageAccounts = service.listStorageAccountPerAppDeployment(List.of(appId), true);
    assertThat(
        "Correct storage accounts are returned", returnedStorageAccounts, equalTo(storageAccounts));
  }
}
