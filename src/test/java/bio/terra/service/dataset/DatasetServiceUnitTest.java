package bio.terra.service.dataset;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.usermetrics.UserLoggingMetrics;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.SqlSortDirection;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnStatisticsDoubleModel;
import bio.terra.model.ColumnStatisticsIntModel;
import bio.terra.model.ColumnStatisticsTextModel;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.TableDataType;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.SynapseDataResultModel;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderSettingsDao;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DatasetServiceUnitTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final String DATASET_TABLE_NAME = "Table1";

  @Mock private DatasetDao datasetDao;

  private DatasetService datasetService;
  @Mock private DatasetJsonConversion datasetJsonConversion;
  @Mock private JobService jobService;
  @Mock private LoadService loadService;
  @Mock private ProfileDao profileDao;
  @Mock private StorageTableService storageTableService;
  @Mock private BigQueryTransactionPdao bigQueryTransactionPdao;
  @Mock private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Mock private MetadataDataAccessUtils metadataDataAccessUtils;
  @Mock private ResourceService resourceService;
  @Mock private GcsPdao gcsPdao;
  @Mock private ObjectMapper objectMapper;
  @Mock private AzureBlobStorePdao azureBlobStorePdao;
  @Mock private ProfileService profileService;
  @Mock private UserLoggingMetrics loggingMetrics;
  @Mock private IamService iamService;
  @Mock private DatasetTableDao datasetTableDao;
  @Mock private AzureSynapsePdao azureSynapsePdao;

  @BeforeEach
  void setup() {
    datasetService =
        new DatasetService(
            datasetJsonConversion,
            datasetDao,
            jobService,
            loadService,
            profileDao,
            storageTableService,
            bigQueryTransactionPdao,
            bigQueryDatasetPdao,
            resourceService,
            gcsPdao,
            objectMapper,
            azureBlobStorePdao,
            profileService,
            loggingMetrics,
            iamService,
            datasetTableDao,
            azureSynapsePdao,
            metadataDataAccessUtils);
  }

  @Test
  void enumerate() {
    UUID uuid = UUID.randomUUID();
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(uuid, Set.of(role));
    MetadataEnumeration<DatasetSummary> metadataEnumeration = new MetadataEnumeration<>();
    DatasetSummary summary =
        new DatasetSummary().id(uuid).createdDate(Instant.now()).storage(List.of());
    metadataEnumeration.items(List.of(summary));
    when(datasetDao.enumerate(
            anyInt(), anyInt(), any(), any(), any(), any(), eq(resourcesAndRoles.keySet()), any()))
        .thenReturn(metadataEnumeration);
    var datasets = datasetService.enumerate(0, 10, null, null, null, null, resourcesAndRoles, null);
    assertThat(datasets.getItems().get(0).getId(), equalTo(uuid));
    assertThat(datasets.getRoleMap(), hasEntry(uuid.toString(), List.of(role.toString())));
  }

  @Test
  void patchDatasetIamActions() {
    assertThat(
        "Patch without PHS ID update does not require passport identifier update permissions",
        datasetService.patchDatasetIamActions(new DatasetPatchRequestModel()),
        containsInAnyOrder(IamAction.MANAGE_SCHEMA));

    assertThat(
        "Patch with PHS ID update to empty string requires passport identifier update permissions",
        datasetService.patchDatasetIamActions(new DatasetPatchRequestModel().phsId("")),
        containsInAnyOrder(IamAction.MANAGE_SCHEMA, IamAction.UPDATE_PASSPORT_IDENTIFIER));

    assertThat(
        "Patch with PHS ID update requires passport identifier update permissions",
        datasetService.patchDatasetIamActions(new DatasetPatchRequestModel().phsId("phs123456")),
        containsInAnyOrder(IamAction.MANAGE_SCHEMA, IamAction.UPDATE_PASSPORT_IDENTIFIER));

    assertThat(
        "Patch with description update requires manage schema update permissions",
        datasetService.patchDatasetIamActions(
            new DatasetPatchRequestModel().description("an updated description")),
        containsInAnyOrder(IamAction.MANAGE_SCHEMA));
  }

  @Test
  void updatePredictableIdsFlag() {
    UUID datasetId = UUID.randomUUID();
    DatasetSummary summary = mock(DatasetSummary.class);
    when(summary.toModel()).thenReturn(new DatasetSummaryModel().id(datasetId));
    when(datasetDao.retrieveSummaryById(datasetId)).thenReturn(summary);
    datasetService.setPredictableFileIds(datasetId, true);
    verify(datasetDao).setPredictableFileId(datasetId, true);
    verify(datasetDao).retrieveSummaryById(datasetId);
  }

  @Test
  void testTranslateData() {
    testRetrieveDataGCP(12, 0);
    testRetrieveDataGCP(0, 0);
    testRetrieveDataGCP(8, 4);
    testRetrieveDataAzure(12, 0);
    testRetrieveDataAzure(0, 0);
    testRetrieveDataAzure(8, 4);
  }

  private void testRetrieveDataGCP(int totalRowCount, int filteredRowCount) {
    mockDataset(CloudPlatform.GCP, TableDataType.STRING);
    when(datasetTableDao.retrieveColumnNames(any(), anyBoolean())).thenReturn(List.of("column1"));
    List<BigQueryDataResultModel> values = new ArrayList<>();
    if (filteredRowCount > 0) {
      values.add(
          new BigQueryDataResultModel()
              .filteredCount(filteredRowCount)
              .totalCount(totalRowCount)
              .rowResult(new HashMap<>()));
    }
    try (MockedStatic<BigQueryPdao> utilities = Mockito.mockStatic(BigQueryPdao.class)) {
      utilities
          .when(
              () ->
                  BigQueryPdao.getTable(
                      any(), any(), any(), anyInt(), anyInt(), any(), any(), any()))
          .thenReturn(values);
      utilities
          .when(() -> BigQueryPdao.getTableTotalRowCount(any(), any()))
          .thenReturn(totalRowCount);
      retrieveDataAndValidate(totalRowCount, filteredRowCount);
    }
  }

  private void testRetrieveDataAzure(int totalRowCount, int filteredRowCount) {
    mockDataset(CloudPlatform.AZURE, TableDataType.STRING);
    List<SynapseDataResultModel> values = new ArrayList<>();
    if (filteredRowCount != 0) {
      values.add(
          new SynapseDataResultModel()
              .filteredCount(filteredRowCount)
              .totalCount(totalRowCount)
              .rowResult(new HashMap<>()));
    } else {
      when(azureSynapsePdao.getTableTotalRowCount(any(), any(), any())).thenReturn(totalRowCount);
    }
    when(azureSynapsePdao.getTableData(
            any(), any(), any(), any(), anyInt(), anyInt(), any(), any(), any(), any()))
        .thenReturn(values);
    when(metadataDataAccessUtils.accessInfoFromDataset(any(), any()))
        .thenReturn(
            new AccessInfoModel()
                .parquet(new AccessInfoParquetModel().url("fake.url").sasToken("fake.sas.token")));
    retrieveDataAndValidate(totalRowCount, filteredRowCount);
  }

  private void retrieveDataAndValidate(int totalRowCount, int filteredRowCount) {
    DatasetDataModel datasetDataModel =
        datasetService.retrieveData(
            TEST_USER,
            UUID.randomUUID(),
            DATASET_TABLE_NAME,
            100,
            0,
            PDAO_ROW_ID_COLUMN,
            SqlSortDirection.ASC,
            "");
    assertThat(
        "Correct total row count", datasetDataModel.getTotalRowCount(), equalTo(totalRowCount));
    assertThat(
        "Correct filtered row count",
        datasetDataModel.getFilteredRowCount(),
        equalTo(filteredRowCount));
  }

  @Test
  void testRetrieveColumnStatistics_GCP_TextColumn() {
    mockDataset(CloudPlatform.GCP, TableDataType.STRING);
    ColumnStatisticsTextValue expectedValue =
        new ColumnStatisticsTextValue().value("val1").count(2);
    try (MockedStatic<BigQueryPdao> utilities = Mockito.mockStatic(BigQueryPdao.class)) {
      utilities
          .when(() -> BigQueryPdao.getStatsForTextColumn(any(), any(), any(), any()))
          .thenReturn(new ColumnStatisticsTextModel().values(List.of(expectedValue)));
      ColumnStatisticsTextModel statsModel =
          (ColumnStatisticsTextModel)
              datasetService.retrieveColumnStatistics(
                  TEST_USER, UUID.randomUUID(), DATASET_TABLE_NAME, "column1", "");
      assertThat("Correct stats value", statsModel.getValues(), containsInAnyOrder(expectedValue));
    }
  }

  @Test
  void testRetrieveColumnStatistics_Azure_TextColumn() {
    mockDataset(CloudPlatform.AZURE, TableDataType.STRING);
    ColumnStatisticsTextValue expectedValue =
        new ColumnStatisticsTextValue().value("val1").count(2);
    ColumnStatisticsTextModel expectedModel =
        new ColumnStatisticsTextModel().values(List.of(expectedValue));
    when(azureSynapsePdao.getStatsForTextColumn(any(), any(), any(), any()))
        .thenReturn(expectedModel);
    when(metadataDataAccessUtils.accessInfoFromDataset(any(), any()))
        .thenReturn(
            new AccessInfoModel()
                .parquet(new AccessInfoParquetModel().url("fake.url").sasToken("fake.sas.token")));
    ColumnStatisticsTextModel statsModel =
        (ColumnStatisticsTextModel)
            datasetService.retrieveColumnStatistics(
                TEST_USER, UUID.randomUUID(), DATASET_TABLE_NAME, "column1", "");
    assertThat("Correct stats value", statsModel.getValues(), containsInAnyOrder(expectedValue));
  }

  @Test
  void testRetrieveColumnStatistics_GCP_DoubleColumn() {
    mockDataset(CloudPlatform.GCP, TableDataType.FLOAT);
    ColumnStatisticsDoubleModel expectedValue =
        new ColumnStatisticsDoubleModel().maxValue(2.0).minValue(1.0);
    try (MockedStatic<BigQueryPdao> utilities = Mockito.mockStatic(BigQueryPdao.class)) {
      utilities
          .when(() -> BigQueryPdao.getStatsForDoubleColumn(any(), any(), any(), any()))
          .thenReturn(expectedValue);
      ColumnStatisticsDoubleModel statsModel =
          (ColumnStatisticsDoubleModel)
              datasetService.retrieveColumnStatistics(
                  TEST_USER, UUID.randomUUID(), DATASET_TABLE_NAME, "column1", "");
      assertThat(
          "Correct max value", statsModel.getMaxValue(), equalTo(expectedValue.getMaxValue()));
      assertThat(
          "Correct min value", statsModel.getMinValue(), equalTo(expectedValue.getMinValue()));
    }
  }

  @Test
  void testRetrieveColumnStatistics_Azure_DoubleColumn() {
    mockDataset(CloudPlatform.AZURE, TableDataType.FLOAT);
    ColumnStatisticsDoubleModel expectedValue =
        new ColumnStatisticsDoubleModel().maxValue(2.0).minValue(1.0);
    when(azureSynapsePdao.getStatsForDoubleColumn(any(), any(), any(), any()))
        .thenReturn(expectedValue);
    when(metadataDataAccessUtils.accessInfoFromDataset(any(), any()))
        .thenReturn(
            new AccessInfoModel()
                .parquet(new AccessInfoParquetModel().url("fake.url").sasToken("fake.sas.token")));
    ColumnStatisticsDoubleModel statsModel =
        (ColumnStatisticsDoubleModel)
            datasetService.retrieveColumnStatistics(
                TEST_USER, UUID.randomUUID(), DATASET_TABLE_NAME, "column1", "");
    assertThat("Correct max value", statsModel.getMaxValue(), equalTo(expectedValue.getMaxValue()));
    assertThat("Correct min value", statsModel.getMinValue(), equalTo(expectedValue.getMinValue()));
  }

  @Test
  void testRetrieveColumnStatistics_GCP_IntColumn() {
    mockDataset(CloudPlatform.GCP, TableDataType.INTEGER);
    ColumnStatisticsIntModel expectedValue = new ColumnStatisticsIntModel().maxValue(2).minValue(1);
    try (MockedStatic<BigQueryPdao> utilities = Mockito.mockStatic(BigQueryPdao.class)) {
      utilities
          .when(() -> BigQueryPdao.getStatsForIntColumn(any(), any(), any(), any()))
          .thenReturn(expectedValue);
      ColumnStatisticsIntModel statsModel =
          (ColumnStatisticsIntModel)
              datasetService.retrieveColumnStatistics(
                  TEST_USER, UUID.randomUUID(), DATASET_TABLE_NAME, "column1", "");
      assertThat(
          "Correct max value", statsModel.getMaxValue(), equalTo(expectedValue.getMaxValue()));
      assertThat(
          "Correct min value", statsModel.getMinValue(), equalTo(expectedValue.getMinValue()));
    }
  }

  @Test
  void testRetrieveColumnStatistics_Azure_IntColumn() {
    mockDataset(CloudPlatform.AZURE, TableDataType.INTEGER);
    ColumnStatisticsIntModel expectedValue = new ColumnStatisticsIntModel().maxValue(3).minValue(1);
    when(azureSynapsePdao.getStatsForIntColumn(any(), any(), any(), any()))
        .thenReturn(expectedValue);
    when(metadataDataAccessUtils.accessInfoFromDataset(any(), any()))
        .thenReturn(
            new AccessInfoModel()
                .parquet(new AccessInfoParquetModel().url("fake.url").sasToken("fake.sas.token")));
    ColumnStatisticsIntModel statsModel =
        (ColumnStatisticsIntModel)
            datasetService.retrieveColumnStatistics(
                TEST_USER, UUID.randomUUID(), DATASET_TABLE_NAME, "column1", "");
    assertThat("Correct max value", statsModel.getMaxValue(), equalTo(expectedValue.getMaxValue()));
    assertThat("Correct min value", statsModel.getMinValue(), equalTo(expectedValue.getMinValue()));
  }

  private void mockDataset(CloudPlatform cloudPlatform, TableDataType columnDataType) {
    List<DatasetTable> tables = List.of(new DatasetTable().name(DATASET_TABLE_NAME));
    UUID datasetId = UUID.randomUUID();
    Dataset mockDataset =
        new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform))
            .id(datasetId)
            .tables(tables)
            .tables(
                List.of(
                    new DatasetTable()
                        .name(DATASET_TABLE_NAME)
                        .columns(List.of(new Column().name("column1").type(columnDataType)))));
    when(datasetDao.retrieve(any())).thenReturn(mockDataset);
  }
}
