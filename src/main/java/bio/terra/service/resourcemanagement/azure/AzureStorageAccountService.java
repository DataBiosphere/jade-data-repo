package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.model.AzureRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.resourcemanagement.exception.AzureResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.StorageAccountLockException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.storage.models.StorageAccount;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class AzureStorageAccountService {

  private static final Logger logger = LoggerFactory.getLogger(AzureStorageAccountService.class);

  private final AzureResourceDao resourceDao;
  private final AzureResourceConfiguration resourceConfiguration;
  private final ProfileDao profileDao;

  @Autowired
  public AzureStorageAccountService(
      AzureResourceDao resourceDao,
      AzureResourceConfiguration resourceConfiguration,
      ProfileDao profileDao) {
    this.resourceDao = resourceDao;
    this.resourceConfiguration = resourceConfiguration;
    this.profileDao = profileDao;
  }

  /**
   * Fetch metadata for an existing storage_account_resource Note this method checks for the
   * existence of the underlying cloud resource, if checkCloudResourceExists=true.
   *
   * @param storageAccountResourceId id of the storage account resource
   * @param checkCloudResourceExists true to do the existence check, false to skip it
   * @return a reference to the storage account as a POJO AzureStorageAccountResource
   * @throws AzureResourceNotFoundException if no storage_account_resource metadata row is found
   * @throws CorruptMetadataException if the storage_account_resource metadata row exists but the
   *     cloud resource does not
   */
  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public AzureStorageAccountResource getStorageAccountResourceById(
      UUID storageAccountResourceId, boolean checkCloudResourceExists) {
    // fetch the storage_account_resource metadata row
    AzureStorageAccountResource storageAccountResource =
        resourceDao.retrieveStorageAccountById(storageAccountResourceId);

    if (checkCloudResourceExists) {
      BillingProfileModel billingProfile =
          profileDao.getBillingProfileById(storageAccountResource.getProfileId());
      // throw an exception if the storage account does not already exist
      StorageAccount storageAccount =
          getCloudStorageAccount(billingProfile, storageAccountResource);
      if (storageAccount == null) {
        throw new CorruptMetadataException(
            "Storage account metadata exists, but actual storage account not found: "
                + storageAccountResource.getName());
      }
    }

    return storageAccountResource;
  }

  /**
   * Fetch/create a storage account cloud resource and the associated metadata in the
   * storage_account_resource table.
   *
   * <p>On entry to this method, there are 9 states along 3 main dimensions: Azure storage account -
   * exists or not DR Metadata record - exists or not DR Metadata lock state (only if record
   * exists): - not locked - locked by this flight - locked by another flight In addition, there is
   * one case where it matters if we are reusing storage accounts or not.
   *
   * <p>Itemizing the 9 cases: CASE 1: storage account exists, record exists, record is unlocked The
   * predominant case. We return the storage account resource
   *
   * <p>CASE 2: storage account exists, record exists, locked by another flight We have to wait
   * until the other flight finishes creating the storage account. Throw
   * StorageLockFailureException. We expect the calling Step to retry on that exception.
   *
   * <p>CASE 3: storage account exists, record exists, locked by us This flight created the storage
   * account, but failed before we could unlock it. So, we unlock and return the storage account
   * resource.
   *
   * <p>CASE 4: storage account exists, no record exists, we are not reusing storage account This is
   * the production mode and should not happen. It means we our metadata does not reflect the actual
   * cloud resources. Throw CorruptMetadataException
   *
   * <p>CASE 5: no storage account exists, record exists, not locked This should not happen. Throw
   * CorruptMetadataException
   *
   * <p>CASE 6: no storage account exists, record exists, locked by another flight We have to wait
   * until the other flight finishes creating the storage account. Throw
   * StorageAccountLockFailureException. We expect the calling Step to retry on that exception.
   *
   * <p>CASE 7: no storage account exists, record exists, locked by this flight We must have failed
   * after creating and locking the record, but before creating the storage account. Proceed with
   * the finish-trying-to-create-storage-account algorithm
   *
   * <p>CASE 8: no storage account exists, no record exists Proceed with
   * try-to-create-storage-account algorithm
   *
   * <p>The algorithm to create a storage-account is like a miniature flight and we implement it as
   * a set of methods that chain to make the whole algorithm: 1. createMetadataRecord: create and
   * lock the metadata record; then 2. createCloudStorageAccount: if the storage account does not
   * exist, create it; then 3. createFinish: unlock the metadata record The algorithm may fail
   * between any of those steps, so we may arrive in this method needing to do some or all of those
   * steps.
   *
   * @param storageAccountName name for a new or existing storage account
   * @param applicationResource application deployment in which the storage account should be
   *     retrieved or created
   * @param region location of the storage account
   * @param flightId flight making the request
   * @return a reference to the storage account as a POJO {@link AzureStorageAccountResource}
   * @throws CorruptMetadataException in CASE 5 and CASE 6
   * @throws StorageAccountLockException in CASE 2 and CASE 7, and sometimes case 9
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public AzureStorageAccountResource getOrCreateStorageAccount(
      String storageAccountName,
      String containerId,
      AzureApplicationDeploymentResource applicationResource,
      AzureRegion region,
      String flightId)
      throws InterruptedException {
    logger.info("Creating storage account {}", storageAccountName);
    // Try to get the storage account record and the storage account object
    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(applicationResource.getProfileId());
    AzureStorageAccountResource storageAccountResource =
        resourceDao.getStorageAccount(
            storageAccountName,
            containerId,
            applicationResource.getAzureApplicationDeploymentName());
    StorageAccount storageAccount = getCloudStorageAccount(profileModel, storageAccountResource);

    // Test all of the cases
    if (storageAccount != null) {
      if (storageAccountResource != null) {
        String lockingFlightId = storageAccountResource.getFlightId();
        if (lockingFlightId == null) {
          // CASE 1: everything exists and is unlocked
          return storageAccountResource;
        }
        if (!StringUtils.equals(lockingFlightId, flightId)) {
          // CASE 2: another flight is creating the storage account
          throw storageAccountLockException(flightId);
        }
        // CASE 3: we have the flight locked, but we did all of the creating.
        return createFinish(flightId, storageAccountResource, containerId);
      } else {
        // CASE 4: This code as currently implemented is unreachable since an empty resource make it
        // impossible
        // to have a cloud storage account value
        throw new CorruptMetadataException(
            "Storage account already exists, metadata out of sync with cloud state: "
                + storageAccountName);
      }
    } else {
      // storage account does not exist
      if (storageAccountResource != null) {
        String lockingFlightId = storageAccountResource.getFlightId();
        if (lockingFlightId == null) {
          // CASE 5: no storage account, but the metadata record exists unlocked
          throw new CorruptMetadataException(
              "Storage account does not exist, metadata out of sync with cloud state: "
                  + storageAccountResource);
        }
        if (!StringUtils.equals(lockingFlightId, flightId)) {
          // CASE 6: another flight is creating the storage account
          throw storageAccountLockException(flightId);
        }
        // CASE 7: this flight has the metadata locked, but didn't finish creating the storage
        // account
        return createCloudStorageAccount(
            profileModel, storageAccountResource, containerId, flightId);
      } else {
        // CASE 8: no storage account and no record
        return createMetadataRecord(
            profileModel, storageAccountName, containerId, applicationResource, region, flightId);
      }
    }
  }

  /** Retrieve a storage account metadata object by the specified UUID ID */
  public AzureStorageAccountResource retrieveStorageAccountById(UUID storageAccountId) {
    return resourceDao.retrieveStorageAccountById(storageAccountId);
  }

  public void deleteCloudStorageAccountMetadata(
      String storageAccountResourceName, String topLevelContainer, String flightId) {
    logger.info(
        "Deleting Azure storage account metadata named {} with top level container {}",
        storageAccountResourceName,
        topLevelContainer);
    boolean deleted =
        resourceDao.deleteStorageAccountMetadata(
            storageAccountResourceName, topLevelContainer, flightId);
    logger.info("Metadata removed: {}", deleted);
  }

  private StorageAccountLockException storageAccountLockException(String flightId) {
    return new StorageAccountLockException("Storage account locked by flightId: " + flightId);
  }

  // Step 1 of creating a new storage account - create and lock the metadata record
  private AzureStorageAccountResource createMetadataRecord(
      BillingProfileModel profileModel,
      String storageAccountName,
      String containerId,
      AzureApplicationDeploymentResource applicationResource,
      AzureRegion region,
      String flightId)
      throws InterruptedException {

    // insert a new storage_account_resource row and lock it
    AzureRegion regionToUse =
        Optional.ofNullable(region).orElse(applicationResource.getDefaultRegion());

    AzureStorageAccountResource storageAccountResource =
        resourceDao.createAndLockStorage(
            storageAccountName, containerId, applicationResource, regionToUse, flightId);
    if (storageAccountResource == null) {
      // We tried and failed to get the lock. So we ended up in CASE 2 after all.
      throw storageAccountLockException(flightId);
    }

    return createCloudStorageAccount(profileModel, storageAccountResource, containerId, flightId);
  }

  // Step 2 of creating a new storage account
  private AzureStorageAccountResource createCloudStorageAccount(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      String containerId,
      String flightId) {
    // If the storage account doesn't exist, create it
    StorageAccount storageAccount = getCloudStorageAccount(profileModel, storageAccountResource);
    if (storageAccount == null) {
      storageAccount = newCloudStorageAccount(profileModel, storageAccountResource);
    }

    return createFinish(flightId, storageAccountResource, containerId);
  }

  // Step 3 (last) of creating a new storage account
  private AzureStorageAccountResource createFinish(
      String flightId, AzureStorageAccountResource storageAccountResource, String containerId) {
    resourceDao.unlockStorageAccount(storageAccountResource.getName(), containerId, flightId);

    return storageAccountResource;
  }

  /**
   * Create a new storage account cloud resource. Note this method does not create any associated
   * metadata in the storage_account_resource table.
   *
   * @param storageAccountResource description of the storage account resource to be created
   * @return a reference to the storage account as an Azure Storage account object
   */
  private StorageAccount newCloudStorageAccount(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    logger.info(
        "Creating storage account '{}' in application deployment '{}'",
        storageAccountResource.getName(),
        storageAccountResource.getApplicationResource().getAzureApplicationDeploymentName());

    return resourceConfiguration
        .getClient(profileModel.getSubscriptionId())
        .storageAccounts()
        .define(storageAccountResource.getName())
        .withRegion(storageAccountResource.getRegion().getValue())
        .withExistingResourceGroup(
            storageAccountResource.getApplicationResource().getAzureResourceGroupName())
        .withSku(
            storageAccountResource.getApplicationResource().getStorageAccountSkuType().getValue())
        .withHnsEnabled(true)
        .create();
  }

  /**
   * Fetch an existing storage account cloud resource. Note this method does not check any
   * associated metadata in the storage_account_resource table.
   *
   * @param profileModel the TDR billing profile associated with this storage account
   * @param storageAccountResource storage account resource to look up storage account. If it is
   *     null, return null
   * @return a reference to the storage account as an Azure storage account object, null if not
   *     found
   */
  StorageAccount getCloudStorageAccount(
      BillingProfileModel profileModel, AzureStorageAccountResource storageAccountResource) {
    if (storageAccountResource == null) {
      return null;
    }
    try {
      return resourceConfiguration
          .getClient(profileModel.getSubscriptionId())
          .storageAccounts()
          .getByResourceGroup(
              storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
              storageAccountResource.getName());
    } catch (ManagementException e) {
      if (e.getValue().getCode().equals("ResourceNotFound")) {
        return null;
      }
      throw new AzureResourceException("Could not check storage account existence", e);
    } catch (Exception e) {
      throw new AzureResourceException("Could not check storage account existence", e);
    }
  }
}
