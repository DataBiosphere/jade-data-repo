package bio.terra.service.resourcemanagement;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.CollectionType;
import bio.terra.common.Table;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.AccessInfoParquetModelTable;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import com.azure.storage.blob.sas.BlobSasPermission;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

/** Utilities for building strings to access metadata */
@Component
public final class MetadataDataAccessUtils {

  private static final Duration DEFAULT_SAS_TOKEN_EXPIRATION = Duration.ofMinutes(15);
  private static final String BIGQUERY_DATASET_LINK =
      "https://console.cloud.google.com/bigquery?project=<project>&"
          + "ws=!<dataset>&d=<dataset>&p=<project>&page=<page>";
  private static final String BIGQUERY_TABLE_LINK = BIGQUERY_DATASET_LINK + "&t=<table>";
  private static final String BIGQUERY_TABLE_ADDRESS = "<project>.<dataset>.<table>";
  private static final String BIGQUERY_DATASET_ID = "<project>:<dataset>";
  private static final String BIGQUERY_TABLE_ID = "<dataset_id>.<table>";
  private static final String BIGQUERY_BASE_QUERY = "SELECT * FROM `<table_address>`";

  private static final String AZURE_PARQUET_LINK =
      "https://<storageAccount>.blob.core.windows.net/metadata/<blob>";
  private static final String AZURE_BLOB_TEMPLATE_DATASET = "parquet/<table>";
  private static final String AZURE_BLOB_TEMPLATE_SNAPSHOT =
      "parquet/<collectionId>/<table>/*.parquet/*";
  private static final String AZURE_DATASET_ID = "<storageAccount>.<dataset>";

  private static final String DEPLOYED_APPLICATION_RESOURCE_ID =
      "/subscriptions/<subscription>/resourceGroups"
          + "/<resource_group>/providers/Microsoft.Solutions/applications/<application_name>";

  private final ResourceService resourceService;
  private final ProfileService profileService;

  private final AzureBlobStorePdao azureBlobStorePdao;

  @Autowired
  public MetadataDataAccessUtils(
      ResourceService resourceService,
      AzureBlobStorePdao azureBlobStorePdao,
      ProfileService profileService) {
    this.resourceService = resourceService;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.profileService = profileService;
  }

  /** Nature of the page to link to in the BigQuery UI */
  enum LinkPage {
    DATASET("dataset"),
    TABLE("table");
    private final String value;

    LinkPage(final String value) {
      this.value = value;
    }
  }

  /** Generate an {@link AccessInfoModel} from a Snapshot */
  public AccessInfoModel accessInfoFromSnapshot(
      final Snapshot snapshot, final AuthenticatedUserRequest userRequest) {
    return accessInfoFromSnapshot(snapshot, userRequest, null);
  }
  /** Generate an {@link AccessInfoModel} from a Snapshot */
  public AccessInfoModel accessInfoFromSnapshot(
      final Snapshot snapshot, final AuthenticatedUserRequest userRequest, String forTable) {
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(
            snapshot
                .getFirstSnapshotSource()
                .getDataset()
                .getDatasetSummary()
                .getStorageCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      return makeAccessInfoBigQuery(
          snapshot.getName(),
          snapshot.getProjectResource().getGoogleProjectId(),
          snapshot.getTables());
    } else if (cloudPlatformWrapper.isAzure()) {
      BillingProfileModel profileModel =
          profileService.getProfileByIdNoCheck(snapshot.getProfileId());
      AzureStorageAccountResource storageAccountResource = snapshot.getStorageAccountResource();
      List<SnapshotTable> tables;
      if (forTable == null) {
        tables = snapshot.getTables();
      } else {
        tables =
            snapshot.getTables().stream()
                .filter(t -> t.getName().equalsIgnoreCase(forTable))
                .collect(Collectors.toList());
      }
      return makeAccessInfoAzure(
          snapshot, storageAccountResource, tables, profileModel, userRequest);
    } else {
      throw new InvalidCloudPlatformException();
    }
  }

  /** Generate an {@link AccessInfoModel} from a Dataset */
  public AccessInfoModel accessInfoFromDataset(
      final Dataset dataset, final AuthenticatedUserRequest userRequest) {
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    if (cloudPlatformWrapper.isGcp()) {
      return makeAccessInfoBigQuery(
          BigQueryPdao.prefixName(dataset.getName()),
          dataset.getProjectResource().getGoogleProjectId(),
          dataset.getTables());
    } else if (cloudPlatformWrapper.isAzure()) {
      BillingProfileModel profileModel = dataset.getDatasetSummary().getDefaultBillingProfile();
      AzureStorageAccountResource storageAccountResource =
          resourceService.getDatasetStorageAccount(dataset, profileModel);
      return makeAccessInfoAzure(
          dataset, storageAccountResource, dataset.getTables(), profileModel, userRequest);
    } else {
      throw new InvalidCloudPlatformException();
    }
  }

  private AccessInfoModel makeAccessInfoAzure(
      final FSContainerInterface collection,
      final AzureStorageAccountResource storageAccountResource,
      final List<? extends Table> tables,
      final BillingProfileModel profileModel,
      final AuthenticatedUserRequest userRequest) {
    AccessInfoModel accessInfoModel = new AccessInfoModel();

    BlobSasTokenOptions blobSasTokenOptions =
        new BlobSasTokenOptions(
            DEFAULT_SAS_TOKEN_EXPIRATION,
            new BlobSasPermission().setReadPermission(true).setListPermission(true),
            userRequest.getEmail());

    String blobName;
    BiFunction<FSContainerInterface, Table, String> tableBlobGenerator;
    if (collection.getCollectionType() == CollectionType.DATASET) {
      blobName = "parquet";
      tableBlobGenerator =
          (c, t) -> new ST(AZURE_BLOB_TEMPLATE_DATASET).add("table", t.getName()).render();
    } else if (collection.getCollectionType() == CollectionType.SNAPSHOT) {
      blobName = "parquet/" + collection.getId();
      tableBlobGenerator =
          (c, t) ->
              new ST(AZURE_BLOB_TEMPLATE_SNAPSHOT)
                  .add("collectionId", c.getId())
                  .add("table", t.getName())
                  .render();
    } else {
      throw new IllegalArgumentException(
          String.format("Invalid collection type: %s", collection.getClass().getName()));
    }

    String unsignedUrl =
        new ST(AZURE_PARQUET_LINK)
            .add("storageAccount", storageAccountResource.getName())
            .add("blob", blobName)
            .render();
    String signedURL =
        azureBlobStorePdao.signFile(
            profileModel,
            storageAccountResource,
            unsignedUrl,
            ContainerType.METADATA,
            blobSasTokenOptions);

    UrlParts urlParts = UrlParts.fromUrl(signedURL);
    accessInfoModel.parquet(
        new AccessInfoParquetModel()
            .datasetName(collection.getName())
            .datasetId(
                new ST(AZURE_DATASET_ID)
                    .add("storageAccount", storageAccountResource.getName())
                    .add("dataset", collection.getName())
                    .render())
            .storageAccountId(storageAccountResource.getResourceId().toString())
            .url(urlParts.url)
            .sasToken(urlParts.sasToken)
            .tables(
                tables.stream()
                    .map(
                        t -> {
                          String tableBlob = tableBlobGenerator.apply(collection, t);
                          String unsignedTableUrl =
                              new ST(AZURE_PARQUET_LINK)
                                  .add("storageAccount", storageAccountResource.getName())
                                  .add("blob", tableBlob)
                                  .render();
                          String tableUrl =
                              azureBlobStorePdao.signFile(
                                  profileModel,
                                  storageAccountResource,
                                  unsignedTableUrl,
                                  ContainerType.METADATA,
                                  blobSasTokenOptions);
                          UrlParts tableUrlParts = UrlParts.fromUrl(tableUrl);
                          return new AccessInfoParquetModelTable()
                              .name(t.getName())
                              .url(tableUrlParts.url)
                              .sasToken(tableUrlParts.sasToken);
                        })
                    .collect(Collectors.toList())));

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
   * @param subscriptionId The ID of the subscription into which the application is deployed
   * @param resourceGroupName The name of the resource group into which the application is deployed
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

  private static class UrlParts {
    private final String url;
    private final String sasToken;

    public UrlParts(final String url, final String sasToken) {
      this.url = url;
      this.sasToken = sasToken;
    }

    public static UrlParts fromUrl(final String signedURL) {
      String[] urlParts = signedURL.split("\\?");
      if (urlParts.length != 2) {
        throw new IllegalArgumentException(
            String.format("Url %s does not appear to be properly formatted", signedURL));
      }
      return new UrlParts(urlParts[0], urlParts[1]);
    }
  }
}
