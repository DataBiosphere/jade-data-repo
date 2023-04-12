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
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SqlSortDirection;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DatasetService.class})
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetServiceUnitTest {
  private AuthenticatedUserRequest testUser;
  private String datasetTableName;
  @MockBean private DatasetDao datasetDao;

  @Autowired private DatasetService datasetService;

  @MockBean private JobService jobService;
  @MockBean private LoadService loadService;
  @MockBean private ProfileDao profileDao;
  @MockBean private StorageTableService storageTableService;
  @MockBean private BigQueryTransactionPdao bigQueryTransactionPdao;
  @MockBean private BigQueryDatasetPdao bigQueryDatasetPdao;
  @MockBean private MetadataDataAccessUtils metadataDataAccessUtils;
  @MockBean private ResourceService resourceService;
  @MockBean private GcsPdao gcsPdao;
  @MockBean private ObjectMapper objectMapper;
  @MockBean private AzureBlobStorePdao azureBlobStorePdao;
  @MockBean private ProfileService profileService;
  @MockBean private UserLoggingMetrics loggingMetrics;
  @MockBean private IamService iamService;
  @MockBean private DatasetTableDao datasetTableDao;
  @MockBean private AzureSynapsePdao azureSynapsePdao;

  @Before
  public void setup() {
    testUser =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DatasetUnit")
            .setEmail("dataset@unit.com")
            .setToken("token")
            .build();
    datasetTableName = "Table1";
  }

  @Test
  public void enumerate() {
    UUID uuid = UUID.randomUUID();
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(uuid, Set.of(role));
    MetadataEnumeration<DatasetSummary> metadataEnumeration = new MetadataEnumeration<>();
    DatasetSummary summary =
        new DatasetSummary().id(uuid).createdDate(Instant.now()).storage(List.of());
    metadataEnumeration.items(List.of(summary));
    when(datasetDao.enumerate(
            anyInt(), anyInt(), any(), any(), any(), any(), eq(resourcesAndRoles.keySet())))
        .thenReturn(metadataEnumeration);
    var datasets = datasetService.enumerate(0, 10, null, null, null, null, resourcesAndRoles);
    assertThat(datasets.getItems().get(0).getId(), equalTo(uuid));
    assertThat(datasets.getRoleMap(), hasEntry(uuid.toString(), List.of(role.toString())));
  }

  @Test
  public void patchDatasetIamActions() {
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
  public void updatePredictableIdsFlag() {
    UUID datasetId = UUID.randomUUID();
    DatasetSummary summary = mock(DatasetSummary.class);
    when(summary.toModel()).thenReturn(new DatasetSummaryModel().id(datasetId));
    when(datasetDao.retrieveSummaryById(datasetId)).thenReturn(summary);
    datasetService.setPredictableFileIds(datasetId, true);
    verify(datasetDao).setPredictableFileId(datasetId, true);
    verify(datasetDao).retrieveSummaryById(datasetId);
  }

  @Test
  public void testTranslateData() {
    testRetrieveDataGCP(12, 0);
    testRetrieveDataGCP(0, 0);
    testRetrieveDataGCP(8, 4);
    testRetrieveDataAzure(12, 0);
    testRetrieveDataAzure(0, 0);
    testRetrieveDataAzure(8, 4);
  }

  private void testRetrieveDataGCP(int totalRowCount, int filteredRowCount) {
    mockDataset(CloudPlatform.GCP);
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
    mockDataset(CloudPlatform.AZURE);
    List<SynapseDataResultModel> values = new ArrayList<>();
    if (filteredRowCount > 0) {
      values.add(
          new SynapseDataResultModel()
              .filteredCount(filteredRowCount)
              .totalCount(totalRowCount)
              .rowResult(new HashMap<>()));
    }
    when(azureSynapsePdao.getTableData(
            any(), any(), any(), any(), anyInt(), anyInt(), any(), any(), any(), any()))
        .thenReturn(values);
    when(azureSynapsePdao.getTableTotalRowCount(any(), any(), any())).thenReturn(totalRowCount);
    when(metadataDataAccessUtils.accessInfoFromDataset(any(), any()))
        .thenReturn(
            new AccessInfoModel()
                .parquet(new AccessInfoParquetModel().url("fake.url").sasToken("fake.sas.token")));
    retrieveDataAndValidate(totalRowCount, filteredRowCount);
  }

  private void retrieveDataAndValidate(int totalRowCount, int filteredRowCount) {
    DatasetDataModel datasetDataModel =
        datasetService.retrieveData(
            testUser,
            UUID.randomUUID(),
            datasetTableName,
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

  private void mockDataset(CloudPlatform cloudPlatform) {
    List<DatasetTable> tables = List.of(new DatasetTable().name(datasetTableName));
    UUID datasetId = UUID.randomUUID();
    Dataset mockDataset =
        new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform)).id(datasetId).tables(tables);
    when(datasetDao.retrieve(any())).thenReturn(mockDataset);
    when(datasetTableDao.retrieveColumnNames(any(), anyBoolean())).thenReturn(List.of("column1"));
  }
}
