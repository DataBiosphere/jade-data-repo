package bio.terra.service.resourcemanagement;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.Table;
import bio.terra.common.exception.CommonExceptions;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.AccessInfoModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

/** Utilities for building strings to access metadata */
@Component
public final class MetadataDataAccessUtils {

  private static final String BIGQUERY_DATASET_LINK =
      "https://console.cloud.google.com/bigquery?project=<project>&"
          + "ws=!<dataset>&d=<dataset>&p=<project>&page=<page>";
  private static final String BIGQUERY_TABLE_LINK = BIGQUERY_DATASET_LINK + "&t=<table>";
  private static final String BIGQUERY_TABLE_ADDRESS = "<project>.<dataset>.<table>";
  private static final String BIGQUERY_DATASET_ID = "<project>:<dataset>";
  private static final String BIGQUERY_TABLE_ID = "<dataset_id>.<table>";
  private static final String BIGQUERY_BASE_QUERY = "SELECT * FROM `<table_address>`";

  private final ResourceService resourceService;
  private final ProfileService profileService;

  @Autowired
  public MetadataDataAccessUtils(ResourceService resourceService, ProfileService profileService) {
    this.resourceService = resourceService;
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
      throw CommonExceptions.AZURE_NOT_SUPPORTED;
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
      throw CommonExceptions.AZURE_NOT_SUPPORTED;
    } else {
      throw new InvalidCloudPlatformException();
    }
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
}
