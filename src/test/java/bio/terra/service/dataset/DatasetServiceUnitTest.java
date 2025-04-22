package bio.terra.service.dataset;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
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
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.TableDataType;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.service.dataset.flight.inheritsteward.SetInheritStewardFlight;
import bio.terra.service.dataset.flight.unlock.DatasetUnlockFlight;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.SynapseDataResultModel;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobBuilder;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DatasetServiceUnitTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID DATASET_ID = UUID.randomUUID();
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
  void retrieve() {
    var dataset = new Dataset().id(DATASET_ID);
    when(datasetDao.retrieve(DATASET_ID)).thenReturn(dataset);
    assertThat(datasetService.retrieve(DATASET_ID), equalTo(dataset));
  }

  @Test
  void retrieveForIngest() {
    var dataset = new Dataset().id(DATASET_ID);
    when(datasetDao.retrieve(DATASET_ID, false, false)).thenReturn(dataset);
    assertThat(datasetService.retrieveForIngest(DATASET_ID), equalTo(dataset));
  }

  @Test
  void enumerate() {
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(DATASET_ID, Set.of(role));
    MetadataEnumeration<DatasetSummary> metadataEnumeration = new MetadataEnumeration<>();
    DatasetSummary summary =
        new DatasetSummary().id(DATASET_ID).createdDate(Instant.now()).storage(List.of());
    metadataEnumeration.items(List.of(summary));
    when(datasetDao.enumerate(
            anyInt(), anyInt(), any(), any(), any(), any(), eq(resourcesAndRoles.keySet()), any()))
        .thenReturn(metadataEnumeration);
    var datasets = datasetService.enumerate(0, 10, null, null, null, null, resourcesAndRoles, null);
    assertThat(datasets.getItems().get(0).getId(), equalTo(DATASET_ID));
    assertThat(datasets.getRoleMap(), hasEntry(DATASET_ID.toString(), List.of(role.toString())));
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
    DatasetSummary summary = mock(DatasetSummary.class);
    when(summary.toModel()).thenReturn(new DatasetSummaryModel().id(DATASET_ID));
    when(datasetDao.retrieveSummaryById(DATASET_ID)).thenReturn(summary);
    datasetService.setPredictableFileIds(DATASET_ID, true);
    verify(datasetDao).setPredictableFileId(DATASET_ID, true);
    verify(datasetDao).retrieveSummaryById(DATASET_ID);
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
            DATASET_ID,
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
                  TEST_USER, DATASET_ID, DATASET_TABLE_NAME, "column1", "");
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
                TEST_USER, DATASET_ID, DATASET_TABLE_NAME, "column1", "");
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
                  TEST_USER, DATASET_ID, DATASET_TABLE_NAME, "column1", "");
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
                TEST_USER, DATASET_ID, DATASET_TABLE_NAME, "column1", "");
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
                  TEST_USER, DATASET_ID, DATASET_TABLE_NAME, "column1", "");
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
                TEST_USER, DATASET_ID, DATASET_TABLE_NAME, "column1", "");
    assertThat("Correct max value", statsModel.getMaxValue(), equalTo(expectedValue.getMaxValue()));
    assertThat("Correct min value", statsModel.getMinValue(), equalTo(expectedValue.getMinValue()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setInheritSteward(boolean inheritSteward) {
    JobBuilder jobBuilder =
        new JobBuilder("", SetInheritStewardFlight.class, null, TEST_USER, jobService);
    when(jobService.newJob(
            String.format("Set inherit steward to %s for dataset %s", inheritSteward, DATASET_ID),
            SetInheritStewardFlight.class,
            null,
            TEST_USER))
        .thenReturn(jobBuilder);
    var custodianEmail = "custodianEmail";
    var stewardEmail = "stewardEmail";
    var members = Arrays.asList("member");
    var stewardMembers = Arrays.asList("member2");
    when(iamService.retrievePolicies(TEST_USER, IamResourceType.DATASET, DATASET_ID))
        .thenReturn(
            List.of(
                new SamPolicyModel()
                    .name(IamRole.CUSTODIAN.toString())
                    .email(custodianEmail)
                    .members(members),
                new SamPolicyModel()
                    .name(IamRole.STEWARD.toString())
                    .email(stewardEmail)
                    .members(stewardMembers)));
    ArgumentCaptor<FlightMap> captor = ArgumentCaptor.forClass(FlightMap.class);
    when(jobService.submit(eq(SetInheritStewardFlight.class), captor.capture()))
        .thenReturn("JobId");
    assertThat(
        "Job is submitted and JobId is returned",
        datasetService.setInheritSteward(DATASET_ID, inheritSteward, TEST_USER),
        equalTo("JobId"));
    FlightMap flightMap = captor.getValue();
    assertThat(
        flightMap.get(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.class),
        equalTo(IamResourceType.DATASET));
    assertThat(flightMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class), equalTo(DATASET_ID));
    assertThat(
        flightMap.get(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.class),
        equalTo(IamAction.SET_INHERIT_STEWARD));
    assertThat(
        flightMap.get(JobMapKeys.DATASET_POLICY_EMAILS.getKeyName(), List.class),
        equalTo(Arrays.asList(custodianEmail, stewardEmail)));
    assertThat(
        flightMap.get(JobMapKeys.DATASET_POLICY_USERS.getKeyName(), List.class),
        equalTo(Stream.of(members, stewardMembers).flatMap(List::stream).toList()));
    assertThat(
        flightMap.get(JobMapKeys.INHERIT_STEWARD.getKeyName(), Boolean.class),
        equalTo(inheritSteward));
  }

  private void mockDataset(CloudPlatform cloudPlatform, TableDataType columnDataType) {
    List<DatasetTable> tables = List.of(new DatasetTable().name(DATASET_TABLE_NAME));
    Dataset mockDataset =
        new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform))
            .id(DATASET_ID)
            .tables(tables)
            .tables(
                List.of(
                    new DatasetTable()
                        .name(DATASET_TABLE_NAME)
                        .columns(List.of(new Column().name("column1").type(columnDataType)))));
    when(datasetDao.retrieve(any())).thenReturn(mockDataset);
  }

  private ResourceLocks mockSubmitAndWait(UnlockResourceRequest request, JobBuilder jobBuilder) {
    when(jobService.newJob(anyString(), eq(DatasetUnlockFlight.class), eq(request), eq(TEST_USER)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(any(), any())).thenReturn(jobBuilder);
    ResourceLocks resourceLocks = new ResourceLocks().exclusive("an-exclusive-lock");
    when(jobBuilder.submitAndWait(ResourceLocks.class)).thenReturn(resourceLocks);
    return resourceLocks;
  }

  @Test
  void manualUnlock() {
    UnlockResourceRequest request = new UnlockResourceRequest().lockName("flightId");
    JobBuilder jobBuilder = mock(JobBuilder.class);
    ResourceLocks expected = mockSubmitAndWait(request, jobBuilder);

    ResourceLocks actual = datasetService.manualUnlock(TEST_USER, DATASET_ID, request);
    assertThat("Job is submitted and ResourceLocks returned", actual, equalTo(expected));
    // Dataset ID is supplied as an input parameter
    verify(jobBuilder).addParameter(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void createDataset(boolean isTDRBillingProfile) {
    var defaultBillingProfile = UUID.randomUUID();
    var datasetName = "datasetName";
    DatasetRequestModel datasetRequestModel =
        new DatasetRequestModel().name(datasetName).defaultProfileId(defaultBillingProfile);
    JobBuilder jobBuilder =
        new JobBuilder("", DatasetCreateFlight.class, datasetRequestModel, TEST_USER, jobService);
    when(jobService.newJob(
            String.format("Create dataset %s", datasetName),
            DatasetCreateFlight.class,
            datasetRequestModel,
            TEST_USER))
        .thenReturn(jobBuilder);

    if (!isTDRBillingProfile) {
      when(profileService.getProfileByIdNoCheck(defaultBillingProfile))
          .thenThrow(new ProfileNotFoundException("Profile not found"));
    }

    ArgumentCaptor<FlightMap> captor = ArgumentCaptor.forClass(FlightMap.class);
    when(jobService.submit(eq(DatasetCreateFlight.class), captor.capture())).thenReturn("JobId");

    datasetService.createDataset(datasetRequestModel, TEST_USER);
    verify(profileService).getProfileByIdNoCheck(defaultBillingProfile);

    FlightMap flightMap = captor.getValue();
    assertThat(
        flightMap.get(JobMapKeys.TDR_BILLING_PROFILE_FALLBACK.getKeyName(), Boolean.class),
        equalTo(isTDRBillingProfile));
  }

  @ParameterizedTest
  @MethodSource("provideIamRoleName")
  void testIsInherited(String role, boolean isInherited) {
    assertThat(DatasetService.isInheritedRole(role), is(isInherited));
  }

  private static Stream<Arguments> provideIamRoleName() {
    return Stream.of(
        Arguments.of("custodian", true),
        Arguments.of("steward", true),
        Arguments.of("reader", false),
        Arguments.of("CUSTODIAN", true),
        Arguments.of("STEWARD", true),
        Arguments.of("READER", false),
        Arguments.of("12345", false));
  }

  @ParameterizedTest
  @MethodSource("provideIamRoles")
  void testIsInherited(IamRole role, boolean isInherited) {
    assertThat(DatasetService.isInheritedRole(role), is(isInherited));
  }

  private static Stream<Arguments> provideIamRoles() {
    return Stream.of(
        Arguments.of(IamRole.CUSTODIAN, true),
        Arguments.of(IamRole.STEWARD, true),
        Arguments.of(IamRole.READER, false));
  }
}
