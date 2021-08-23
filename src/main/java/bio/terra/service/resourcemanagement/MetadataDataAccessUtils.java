package bio.terra.service.resourcemanagement;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.Table;
import bio.terra.model.AccessInfoAzureModel;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import com.azure.storage.blob.sas.BlobSasPermission;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

/**
 * Utilities for building strings to access metadata
 */
@Component
public final class MetadataDataAccessUtils {

  private static final String BIGQUERY_DATASET_LINK =
      "https://console.cloud.google.com/bigquery?project=<project>&"
          + "ws=!<dataset>&d=<dataset>&p=<project>&page=<page>";
  private static final String BIGQUERY_TABLE_LINK = BIGQUERY_DATASET_LINK + "&t=<table>";
  private static final String BIGQUERY_TABLE_ADDRESS = "<project>.<dataset>.<table>";
  private static final String BIGQUERY_DATASET_ID = "<project>:<dataset>";
  private static final String BIGQUERY_TABLE_ID = "<dataset_id>.<table>";
  private static final String BIGQUERY_BASE_QUERY = "SELECT * FROM `<table_address>` LIMIT 1000";

  private static final String DEPLOYED_APPLICATION_RESOURCE_ID =
      "/subscriptions/<subscription>/resourceGroups"
          + "/<resource_group>/providers/Microsoft.Solutions/applications/<application_name>";

  private static ResourceService resourceService;
  private static final AzureBlobStorePdao azureBlobStorePdao;

  public MetadataDataAccessUtils(ResourceService resourceService, AzureBlobStorePdao azureBlobStorePdao) {
    this.resourceService = resourceService;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  /**
   * Nature of the page to link to in the BigQuery UI
   */
  enum LinkPage {
    DATASET("dataset"),
    TABLE("table");
    private final String value;

    LinkPage(final String value) {
      this.value = value;
    }
  }

  /**
   * Generate an {@link AccessInfoModel} from a Snapshot
   */
  public static AccessInfoModel accessInfoFromSnapshot(final Snapshot snapshot) {
    return makeAccessInfoBigQuery(
        snapshot.getName(),
        snapshot.getProjectResource().getGoogleProjectId(),
        snapshot.getTables());
  }

  /**
   * Generate an {@link AccessInfoModel} from a Dataset
   */
  public static AccessInfoModel accessInfoFromDataset(final Dataset dataset) {
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper
        .of(dataset.getDatasetSummary().getStorageCloudPlatform());

    if (cloudPlatformWrapper.isGcp()) {
      return makeAccessInfoBigQuery(
          BigQueryPdao.prefixName(dataset.getName()),
          dataset.getProjectResource().getGoogleProjectId(),
          dataset.getTables());
    } else if (cloudPlatformWrapper.isAzure()) {
      AzureStorageAccountResource storageAccountResource = resourceService
          .getStorageAccount(dataset, dataset.getDatasetSummary().getDefaultBillingProfile());
      return makeAccessInfoAzure(dataset.getName(), storageAccountResource, dataset.getTables());
    } else {
      throw new IllegalArgumentException("Unrecognized cloud platform");
    }
  }

  private static AccessInfoModel makeAccessInfoAzure(
      final String datasetName,
      final AzureStorageAccountResource storageAccountResource,
      final List<? extends Table> tables) {
    AccessInfoModel accessInfoModel = new AccessInfoModel();

//    BlobContainerClientFactory targetDataClientFactory =
//        azureBlobStorePdao.getTargetDataClientFactory(
//            profileModel, storageAccount, ContainerType.METADATA, false);
//
//    // Given the sas token, rebuild a signed url
//    BlobSasTokenOptions options =
//        new BlobSasTokenOptions(
//            DEFAULT_SAS_TOKEN_EXPIRATION,
//            new BlobSasPermission()
//                .setReadPermission(true)
//                .setListPermission(true)
//            AzureSynapsePdao.class.getName());
//    String signedURL =
//        targetDataClientFactory.getBlobSasUrlFactory().createSasUrlForBlob(blobName, options);

    accessInfoModel.azure(
        new AccessInfoAzureModel()
            .datasetName(datasetName)
            .storageAccountId(storageAccountResource.getResourceId().toString())

    );

    return accessInfoModel;
  }

  private static AccessInfoModel makeAccessInfoBigQuery(
      final String bqDatasetName,
      final String googleProjectId,
      final List<? extends Table> tables) {
    AccessInfoModel accessInfoModel = new AccessInfoModel();
    String datasetId =
        new ST(BIGQUERY_DATASET_ID)
            .add("project", googleProjectId)
            .add("dataset", bqDatasetName)
            .render();

    // Currently, only BigQuery is supported.  Parquet specific information will be added here
    accessInfoModel.bigQuery(
        new AccessInfoBigQueryModel()
            .datasetName(bqDatasetName)
            .projectId(googleProjectId)
            .link(
                new ST(BIGQUERY_DATASET_LINK)
                    .add("project", googleProjectId)
                    .add("dataset", bqDatasetName)
                    .add("page", LinkPage.DATASET.value)
                    .render())
            .datasetId(datasetId)
            .tables(
                tables.stream()
                    .map(
                        t ->
                            new AccessInfoBigQueryModelTable()
                                .name(t.getName())
                                .link(
                                    new ST(BIGQUERY_TABLE_LINK)
                                        .add("project", googleProjectId)
                                        .add("dataset", bqDatasetName)
                                        .add("table", t.getName())
                                        .add("page", LinkPage.TABLE.value)
                                        .render())
                                .qualifiedName(
                                    new ST(BIGQUERY_TABLE_ADDRESS)
                                        .add("project", googleProjectId)
                                        .add("dataset", bqDatasetName)
                                        .add("table", t.getName())
                                        .render())
                                .id(
                                    new ST(BIGQUERY_TABLE_ID)
                                        .add("dataset_id", datasetId)
                                        .add("table", t.getName())
                                        .render()))
                    // Use the address that was already rendered
                    .map(
                        st ->
                            st.sampleQuery(
                                new ST(BIGQUERY_BASE_QUERY)
                                    .add("table_address", st.getQualifiedName())
                                    .render()))
                    .collect(Collectors.toList())));

    return accessInfoModel;
  }

  /**
   * Return the Azure resource ID for the application deployment associated with the specified
   * {@link BillingProfileModel}
   *
   * @param profileModel The billing profile to get the application deployment for
   * @return Azure resource identifier
   */
  public static String getApplicationDeploymentId(BillingProfileModel profileModel) {
    return getApplicationDeploymentId(
        profileModel.getSubscriptionId(),
        profileModel.getResourceGroupName(),
        profileModel.getApplicationDeploymentName());
  }

  /**
   * Return the Azure resource ID for the application deployment associated with the specified
   * parameters
   *
   * @param subscriptionId            The ID of the subscription into which the application is
   *                                  deployed
   * @param resourceGroupName         The name of the resource group into which the application is
   *                                  deployed
   * @param applicationDeploymentName The name of the application deployment
   * @return Azure resource identifier
   */
  public static String getApplicationDeploymentId(
      UUID subscriptionId, String resourceGroupName, String applicationDeploymentName) {
    return new ST(DEPLOYED_APPLICATION_RESOURCE_ID)
        .add("subscription", subscriptionId)
        .add("resource_group", resourceGroupName)
        .add("application_name", applicationDeploymentName)
        .render();
  }
}
