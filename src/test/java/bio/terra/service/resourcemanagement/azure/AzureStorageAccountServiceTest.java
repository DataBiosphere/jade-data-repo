package bio.terra.service.resourcemanagement.azure;

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
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class AzureStorageAccountServiceTest {
  private final Logger logger = LoggerFactory.getLogger(AzureStorageAccountServiceTest.class);
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
  @Mock private AzureResourceManager client;

  private BillingProfileModel billingProfileModel;
  private AzureApplicationDeploymentResource applicationResource;

  private AzureStorageAccountService service;

  @Before
  public void setUp() throws Exception {
    service = new AzureStorageAccountService(resourceDao, resourceConfiguration, profileDao);

    billingProfileModel = ProfileFixtures.randomAzureBillingProfile();
    when(profileDao.getBillingProfileById(billingProfileModel.getId()))
        .thenReturn(billingProfileModel);
    applicationResource =
        new AzureApplicationDeploymentResource()
            .profileId(billingProfileModel.getId())
            .azureResourceGroupName(MANAGED_GROUP_NAME)
            .azureApplicationDeploymentName(APPLICATION_DEPLOYMENT_NAME)
            .storageAccountSkuType(STORAGE_SKU_TYPE);

    // Mock Azure client calls
    when(client.storageAccounts()).thenReturn(storageAccounts);
    when(resourceConfiguration.getClient(billingProfileModel.getSubscriptionId()))
        .thenReturn(client);
  }

  @Test
  public void testGetStorageAccountResourceById() {
    UUID storageAccountId = UUID.randomUUID();
    service.getStorageAccountResourceById(storageAccountId, false);
    verify(resourceDao, times(1)).retrieveStorageAccountById(storageAccountId);
  }

  @Test
  public void testGetStorageAccountResourceByIdAndCheckCloudFound() {
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
  public void testGetStorageAccountResourceByIdAndCheckCloudNotFound() {
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
  public void tesGetOrCreateStorageAccountCase1() throws InterruptedException {
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
  public void tesGetOrCreateStorageAccountCase2() {
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
        () -> {
          try {
            service.getOrCreateStorageAccount(
                STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        "Storage account locked by flightId: " + flight);
  }

  @Test
  public void tesGetOrCreateStorageAccountCase3() throws InterruptedException {
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
  public void tesGetOrCreateStorageAccountCase4() {
    logger.info("Case 4 is not reachable");
  }

  @Test
  public void tesGetOrCreateStorageAccountCase5() {
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
        () -> {
          try {
            service.getOrCreateStorageAccount(
                STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        "Storage account does not exist, metadata out of sync with cloud state: "
            + STORAGE_ACCOUNT_NAME);
  }

  @Test
  public void tesGetOrCreateStorageAccountCase6() {
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
        () -> {
          try {
            service.getOrCreateStorageAccount(
                STORAGE_ACCOUNT_NAME, COLLECTION_ID, applicationResource, REGION, flight);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        "Storage account locked by flightId: " + flight);
  }

  @Test
  public void tesGetOrCreateStorageAccountCase7() throws InterruptedException {
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
  public void tesGetOrCreateStorageAccountCase8() throws InterruptedException {
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
                new ManagementError("ResourceNotFound", "Couldn't find resource")));
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
}
