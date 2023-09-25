package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.AzureStorageAccountSkuType;
import bio.terra.common.DaoKeyHolder;
import bio.terra.service.profile.exception.ProfileInUseException;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.resourcemanagement.exception.AzureResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.google.api.client.util.Objects;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class AzureResourceDao {
  private static final Logger logger = LoggerFactory.getLogger(AzureResourceDao.class);

  private final NamedParameterJdbcTemplate jdbcTemplate;

  private static final String sqlApplicationRetrieve =
      "SELECT id, azure_application_deployment_id, "
          + "azure_application_deployment_name, azure_resource_group_name, azure_synapse_workspace, default_region,"
          + "storage_account_prefix, storage_account_sku_type, profile_id"
          + " FROM application_deployment_resource";
  private static final String sqlApplicationRetrieveById =
      sqlApplicationRetrieve + " WHERE marked_for_delete = false AND id = :id";
  private static final String sqlApplicationRetrieveByDepName =
      sqlApplicationRetrieve
          + " WHERE marked_for_delete = false AND azure_application_deployment_name = :azure_application_deployment_name";
  private static final String sqlApplicationRetrieveByIdForDelete =
      sqlApplicationRetrieve + " WHERE marked_for_delete = true AND id = :id";
  private static final String sqlApplicationRetrieveByBillingProfileId =
      sqlApplicationRetrieve + " WHERE marked_for_delete = false AND profile_id = :profile_id";

  private static final String sqlStorageAccountRetrieve =
      "SELECT distinct da.id AS application_resource_id, azure_application_deployment_id, "
          + " azure_application_deployment_name, azure_resource_group_name, azure_synapse_workspace, "
          + " default_region, storage_account_prefix, storage_account_sku_type, "
          + " profile_id, sa.id AS storage_account_resource_id, name, datacontainer, metadatacontainer, dbname, "
          + " sr.region as region, flightid, toplevelcontainer "
          + "FROM storage_account_resource sa "
          + "JOIN application_deployment_resource da ON sa.application_resource_id = da.id "
          + "LEFT JOIN dataset_storage_account dsa on sa.id = dsa.storage_account_resource_id "
          + "LEFT JOIN storage_resource sr on dsa.dataset_id = sr.dataset_id AND sr.cloud_resource='STORAGE_ACCOUNT' ";

  private static final String sqlStorageAccountRetrievedByApplicationResource =
      sqlStorageAccountRetrieve
          + "WHERE sa.marked_for_delete = :marked_for_delete AND application_resource_id = :application_resource_id";
  private static final String sqlStorageAccountRetrievedById =
      sqlStorageAccountRetrieve + "WHERE sa.marked_for_delete = false AND sa.id = :id";
  private static final String sqlStorageAccountRetrievedByName =
      sqlStorageAccountRetrieve
          + "WHERE sa.marked_for_delete = false AND sa.name = :name AND sa.toplevelcontainer = :toplevelcontainer";

  // Given a profile id, compute the count of all references to projects associated with the profile
  private static final String sqlProfileProjectRefs =
      "SELECT aid, dscnt + sncnt + sacnt AS refcnt FROM "
          + " (SELECT"
          + "  app.id AS aid,"
          + "  (SELECT COUNT(*) FROM dataset WHERE dataset.application_resource_id = app.id) AS dscnt,"
          + "  (SELECT COUNT(*) FROM snapshot WHERE snapshot.application_resource_id = app.id) AS sncnt,"
          + "  (SELECT count(*) FROM storage_account_resource, dataset_storage_account"
          + "    WHERE storage_account_resource.application_resource_id = app.id"
          + "    AND storage_account_resource.id = dataset_storage_account.storage_account_resource_id"
          + "    AND dataset_storage_account.successful_ingests > 0) AS sacnt"
          + " FROM application_deployment_resource AS app "
          + " WHERE app.profile_id = :profile_id) AS X";

  // Class for collecting results from the above query
  private static class AppRefs {
    private UUID appId;
    private long refCount;

    public UUID getAppId() {
      return appId;
    }

    public AppRefs projectId(UUID projectId) {
      this.appId = projectId;
      return this;
    }

    public long getRefCount() {
      return refCount;
    }

    public AppRefs refCount(long refCount) {
      this.refCount = refCount;
      return this;
    }
  }

  @Autowired
  public AzureResourceDao(NamedParameterJdbcTemplate jdbcTemplate) throws SQLException {
    this.jdbcTemplate = jdbcTemplate;
  }

  // -- deployed application resource methods --

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public UUID createApplicationDeployment(AzureApplicationDeploymentResource application) {
    String sql =
        "INSERT INTO application_deployment_resource "
            + "(azure_application_deployment_id,azure_application_deployment_name,azure_resource_group_name,"
            + "azure_synapse_workspace,default_region,storage_account_prefix,storage_account_sku_type,profile_id) "
            + " VALUES "
            + "(:azure_application_deployment_id,:azure_application_deployment_name,:azure_resource_group_name,"
            + ":azure_synapse_workspace,:default_region,:storage_account_prefix,:storage_account_sku_type,:profile_id)";
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue(
                "azure_application_deployment_id", application.getAzureApplicationDeploymentId())
            .addValue(
                "azure_application_deployment_name",
                application.getAzureApplicationDeploymentName())
            .addValue("azure_resource_group_name", application.getAzureResourceGroupName())
            .addValue("azure_synapse_workspace", application.getAzureSynapseWorkspaceName())
            .addValue("default_region", application.getDefaultRegion().name())
            .addValue("storage_account_prefix", application.getStorageAccountPrefix())
            .addValue(
                "storage_account_sku_type",
                application.getStorageAccountSkuType().name().toString())
            .addValue("profile_id", application.getProfileId());
    DaoKeyHolder keyHolder = new DaoKeyHolder();
    jdbcTemplate.update(sql, params, keyHolder);
    return keyHolder.getId();
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public AzureApplicationDeploymentResource retrieveApplicationDeploymentById(UUID id) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
    return retrieveApplicationDeploymentBy(sqlApplicationRetrieveById, params);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public AzureApplicationDeploymentResource retrieveApplicationDeploymentByName(
      String applicationName) {
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("azure_application_deployment_name", applicationName);
    return retrieveApplicationDeploymentBy(sqlApplicationRetrieveByDepName, params);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public List<AzureStorageAccountResource> retrieveStorageAccountsByApplicationResource(
      UUID applicationResourceId, boolean markedForDelete) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("application_resource_id", applicationResourceId)
            .addValue("marked_for_delete", markedForDelete);
    return retrieveStorageAccountsBy(sqlStorageAccountRetrievedByApplicationResource, params);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public AzureApplicationDeploymentResource retrieveApplicationDeploymentByIdForDelete(UUID id) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", id);
    return retrieveApplicationDeploymentBy(sqlApplicationRetrieveByIdForDelete, params);
  }

  @Transactional(
      propagation = Propagation.REQUIRED,
      isolation = Isolation.SERIALIZABLE,
      readOnly = true)
  public List<AzureApplicationDeploymentResource> retrieveApplicationDeploymentsByBillingProfileId(
      UUID billingProfileId) {
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("profile_id", billingProfileId);
    return retrieveApplicationDeploymentListBy(sqlApplicationRetrieveByBillingProfileId, params);
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public List<UUID> markUnusedApplicationDeploymentsForDelete(UUID profileId) {
    // Note: this logic is copied over from the project logic in GCP and may be overkill for the
    // azure usecase but
    // this will make it less likely to accidentally delete storage accounts
    // Collect all application deployments related to the incoming profile and compute the number of
    // references
    // on those apps.
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("profile_id", profileId);
    List<AppRefs> appRefs =
        jdbcTemplate.query(
            sqlProfileProjectRefs,
            params,
            (rs, rowNum) ->
                new AppRefs()
                    .projectId(rs.getObject("aid", UUID.class))
                    .refCount(rs.getLong("refcnt")));

    // If the profile is in use by any project, we bail here.
    long totalRefs = 0;
    for (AppRefs ref : appRefs) {
      logger.info(
          "Profile app reference appId: {} refCount: {}", ref.getAppId(), ref.getRefCount());
      totalRefs += ref.getRefCount();
    }

    logger.info(
        "Profile {} has {} app deployments with the total of {} references",
        profileId,
        appRefs.size(),
        totalRefs);

    if (totalRefs > 0) {
      throw new ProfileInUseException("Profile is in use and cannot be deleted");
    }

    // Common variables for marking application references and storage accounts for delete.
    List<UUID> appIds = appRefs.stream().map(AppRefs::getAppId).collect(Collectors.toList());
    if (appIds.size() > 0) {
      MapSqlParameterSource markParams = new MapSqlParameterSource().addValue("app_ids", appIds);

      final String sqlMarkProjects =
          "UPDATE application_deployment_resource SET marked_for_delete = true"
              + " WHERE id IN (:app_ids)";
      jdbcTemplate.update(sqlMarkProjects, markParams);

      final String sqlMarkBuckets =
          "UPDATE storage_account_resource SET marked_for_delete = true"
              + " WHERE application_resource_id IN (:app_ids)";
      jdbcTemplate.update(sqlMarkBuckets, markParams);
    }

    return appIds;
  }

  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void deleteApplicationDeploymentMetadata(List<UUID> applicationIds) {
    if (applicationIds.size() > 0) {
      MapSqlParameterSource markParams =
          new MapSqlParameterSource().addValue("application_ids", applicationIds);

      // Delete the storage accounts
      final String sqlMarkStorageAccounts =
          "DELETE FROM storage_account_resource WHERE marked_for_delete = true "
              + "AND application_resource_id IN (:application_ids)";
      jdbcTemplate.update(sqlMarkStorageAccounts, markParams);

      // Delete the application deployments
      final String sqlMarkApplications =
          "DELETE FROM application_deployment_resource WHERE marked_for_delete = true "
              + "AND id IN (:application_ids)";
      jdbcTemplate.update(sqlMarkApplications, markParams);
    }
  }

  private AzureApplicationDeploymentResource retrieveApplicationDeploymentBy(
      String sql, MapSqlParameterSource params) {
    List<AzureApplicationDeploymentResource> applicationList =
        retrieveApplicationDeploymentListBy(sql, params);
    if (applicationList.size() == 0) {
      throw new AzureResourceNotFoundException("Application deployment not found");
    }
    if (applicationList.size() > 1) {
      throw new CorruptMetadataException(
          "Found more than one result for deployed application resource: "
              + applicationList.get(0).getAzureApplicationDeploymentName());
    }
    return applicationList.get(0);
  }

  private List<AzureApplicationDeploymentResource> retrieveApplicationDeploymentListBy(
      String sql, MapSqlParameterSource params) {
    return jdbcTemplate.query(
        sql,
        params,
        (rs, rowNum) ->
            new AzureApplicationDeploymentResource()
                .id(rs.getObject("id", UUID.class))
                .profileId(rs.getObject("profile_id", UUID.class))
                .azureApplicationDeploymentId(rs.getString("azure_application_deployment_id"))
                .azureApplicationDeploymentName(rs.getString("azure_application_deployment_name"))
                .azureResourceGroupName(rs.getString("azure_resource_group_name"))
                .azureSynapseWorkspaceName(rs.getString("azure_synapse_workspace"))
                .defaultRegion(AzureRegion.fromValue(rs.getString("default_region")))
                .storageAccountPrefix(rs.getString("storage_account_prefix"))
                .storageAccountSkuType(
                    AzureStorageAccountSkuType.valueOf(rs.getString("storage_account_sku_type"))));
  }

  // -- storage account resource methods --

  /**
   * Insert a new row into the storage_account_resource metadata table and give the provided flight
   * the lock by setting the flightid column. If there already exists a row with this storage
   * account name and top level container, return null instead of throwing an exception.
   *
   * @return an AzureStorageAccountResource if the insert succeeded, null otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public AzureStorageAccountResource createAndLockStorage(
      String storageAccountName,
      String collectionId,
      AzureApplicationDeploymentResource applicationResource,
      AzureRegion region,
      String flightId) {
    // Put an end to serialization errors here. We only come through here if we really need to
    // create the storage, so this is not on the path of most storage account lookups.
    jdbcTemplate.getJdbcTemplate().execute("LOCK TABLE storage_account_resource IN EXCLUSIVE MODE");

    String sql =
        """
      INSERT INTO storage_account_resource (application_resource_id, name, toplevelcontainer,
        datacontainer, metadatacontainer, dbname, flightid) VALUES
        (:application_resource_id, :name, :toplevelcontainer, :datacontainer, :metadatacontainer,
        :dbname, :flightid)
      ON CONFLICT ON CONSTRAINT storage_account_resource_name_toplevelcontainer_key DO NOTHING
    """;

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("application_resource_id", applicationResource.getId())
            .addValue("name", storageAccountName)
            .addValue("toplevelcontainer", collectionId)
            .addValue("datacontainer", "data")
            .addValue("metadatacontainer", "metadata")
            .addValue("dbname", storageAccountName)
            .addValue("flightid", flightId);
    DaoKeyHolder keyHolder = new DaoKeyHolder();

    int numRowsUpdated = jdbcTemplate.update(sql, params, keyHolder);
    if (numRowsUpdated == 1) {
      return new AzureStorageAccountResource()
          .resourceId(keyHolder.getId())
          .flightId(flightId)
          .profileId(applicationResource.getProfileId())
          .applicationResource(applicationResource)
          .name(storageAccountName)
          .topLevelContainer(collectionId)
          .dataContainer("data")
          .metadataContainer("metadata")
          .dbName(storageAccountName)
          .region(region);
    } else {
      return null;
    }
  }

  /**
   * Unlock an existing storage_account_resource metadata row, by setting flightid = NULL. Only the
   * flight that currently holds the lock can unlock the row. The lock may not be held - that is not
   * an error
   *
   * @param storageAccountName storage account to unlock
   * @param collectionId the id of the dataset or snapshot to unlock the storage account for
   * @param flightId flight trying to unlock it
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public void unlockStorageAccount(
      String storageAccountName, String collectionId, String flightId) {
    String sql =
        """
        UPDATE storage_account_resource SET flightid = NULL
        WHERE name = :name
        AND toplevelcontainer = :toplevelcontainer
        AND flightid = :flightid
        """;
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("name", storageAccountName)
            .addValue("toplevelcontainer", collectionId)
            .addValue("flightid", flightId);
    int numRowsUpdated = jdbcTemplate.update(sql, params);
    logger.info(
        "Storage account {} with container {} was {}",
        storageAccountName,
        collectionId,
        (numRowsUpdated > 0 ? "unlocked" : "not locked"));
  }

  /**
   * Fetch an existing storage_account_resource metadata row using the name amd application
   * deployment name. This method expects that there is exactly one row matching the provided name
   * and application name.
   *
   * @param storageAccountName name of the storage account
   * @param collectionId id of the collection (e.g. dataset or snapshot)
   * @param applicationName application name in which we are searching for the storage account
   * @return a reference to the storage account as a POJO {@link AzureStorageAccountResource} or
   *     null if not found
   * @throws AzureResourceException if the storage account matches, but is in the wrong application
   *     deployment
   * @throws CorruptMetadataException if multiple storage accounts have the same name
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public AzureStorageAccountResource getStorageAccount(
      String storageAccountName, String collectionId, String applicationName) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("name", storageAccountName)
            .addValue("toplevelcontainer", collectionId);
    AzureStorageAccountResource storageAccountResource =
        retrieveStorageAccountBy(sqlStorageAccountRetrievedByName, params);
    if (storageAccountResource == null) {
      return null;
    }

    String foundApplicationName =
        storageAccountResource.getApplicationResource().getAzureApplicationDeploymentName();
    if (!Objects.equal(foundApplicationName, applicationName)) {
      // there is a storage account with this name in our metadata, but it's for a different app
      // deployment
      throw new AzureResourceException(
          String.format(
              "A storage account with this name already exists for a different application: %s, %s",
              storageAccountName, applicationName));
    }

    return storageAccountResource;
  }

  /**
   * Fetch an existing storage_account_resource metadata row using the id. This method expects that
   * there is exactly one row matching the provided resource id.
   *
   * @param storageAccountId unique id of a storage account resource
   * @return a reference to the storage account as a POJO StorageAccountResource
   * @throws AzureResourceNotFoundException if no storage_account_resource metadata row is found
   */
  @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
  public AzureStorageAccountResource retrieveStorageAccountById(UUID storageAccountId) {
    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", storageAccountId);
    AzureStorageAccountResource storageAccountResource =
        retrieveStorageAccountBy(sqlStorageAccountRetrievedById, params);
    if (storageAccountResource == null) {
      throw new AzureResourceNotFoundException(
          "Storage account not found for id:" + storageAccountId);
    }
    return storageAccountResource;
  }

  /**
   * Mark the storage_account_resource metadata row associated with the storage account for delete,
   * provided the row is either unlocked or locked by the provided flight.
   *
   * <p>Actual delete is performed when the associated application deployment is deleted
   *
   * @param storageAccountName name of storage account to delete
   * @param flightId flight trying to delete storage account
   * @return true if a row is deleted, false otherwise
   */
  @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.SERIALIZABLE)
  public boolean markForDeleteStorageAccountMetadata(
      String storageAccountName, String topLevelContainer, String flightId) {
    String sql =
        """
      UPDATE storage_account_resource SET marked_for_delete = true
      WHERE name = :name
      AND toplevelcontainer = :topLevelContainer
      AND (flightid = :flightid OR flightid IS NULL)
      """;

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("name", storageAccountName)
            .addValue("topLevelContainer", topLevelContainer)
            .addValue("flightid", flightId);
    int numRowsUpdated = jdbcTemplate.update(sql, params);
    return (numRowsUpdated == 1);
  }

  private AzureStorageAccountResource retrieveStorageAccountBy(
      String sql, MapSqlParameterSource params) {
    List<AzureStorageAccountResource> storageAccountResources =
        retrieveStorageAccountsBy(sql, params);
    if (storageAccountResources.size() > 1) {
      throw new CorruptMetadataException(
          "Found more than one result for storage account resource: "
              + storageAccountResources.get(0).getName());
    }

    return (storageAccountResources.size() == 0 ? null : storageAccountResources.get(0));
  }

  private List<AzureStorageAccountResource> retrieveStorageAccountsBy(
      String sql, MapSqlParameterSource params) {
    List<AzureStorageAccountResource> storageAccountResources =
        jdbcTemplate.query(
            sql,
            params,
            (rs, rowNum) -> {
              // Make deployed application resource and a storage account resource from the query
              // result
              AzureApplicationDeploymentResource applicationResource =
                  new AzureApplicationDeploymentResource()
                      .id(rs.getObject("application_resource_id", UUID.class))
                      .profileId(rs.getObject("profile_id", UUID.class))
                      .azureApplicationDeploymentId(rs.getString("azure_application_deployment_id"))
                      .azureApplicationDeploymentName(
                          rs.getString("azure_application_deployment_name"))
                      .azureResourceGroupName(rs.getString("azure_resource_group_name"))
                      .azureSynapseWorkspaceName(rs.getString("azure_synapse_workspace"))
                      .defaultRegion(AzureRegion.fromName(rs.getString("default_region")))
                      .storageAccountPrefix(rs.getString("storage_account_prefix"))
                      .storageAccountSkuType(
                          AzureStorageAccountSkuType.valueOf(
                              rs.getString("storage_account_sku_type")));

              AzureRegion region =
                  Optional.ofNullable(rs.getString("region"))
                      .map(AzureRegion::valueOf)
                      .orElse(applicationResource.getDefaultRegion());

              // Since storing the region was not in the original data, we supply the
              // default if a value is not present.
              return new AzureStorageAccountResource()
                  .applicationResource(applicationResource)
                  .profileId(rs.getObject("profile_id", UUID.class))
                  .resourceId(rs.getObject("storage_account_resource_id", UUID.class))
                  .name(rs.getString("name"))
                  .topLevelContainer(rs.getString("toplevelcontainer"))
                  .dataContainer(rs.getString("datacontainer"))
                  .metadataContainer(rs.getString("metadatacontainer"))
                  .dbName(rs.getString("dbname"))
                  .flightId(rs.getString("flightid"))
                  .region(region);
            });

    return storageAccountResources;
  }
}
