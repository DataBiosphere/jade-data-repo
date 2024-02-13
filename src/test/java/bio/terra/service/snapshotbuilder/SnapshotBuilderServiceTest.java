package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderFactory;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotBuilderServiceTest {
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private SnapshotBuilderService snapshotBuilderService;
  @Mock private DatasetService datasetService;
  @Mock private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private CriteriaQueryBuilderFactory criteriaQueryBuilderFactory;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  public void beforeEach() {
    snapshotBuilderService =
        new SnapshotBuilderService(
            snapshotRequestDao,
            snapshotBuilderSettingsDao,
            datasetService,
            bigQueryDatasetPdao,
            azureSynapsePdao,
            criteriaQueryBuilderFactory);
  }

  @Test
  void createSnapshotRequest() {
    UUID datasetId = UUID.randomUUID();
    String email = "user@gmail.com";
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse();
    when(snapshotRequestDao.create(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email))
        .thenReturn(response);

    assertThat(
        "createSnapshotRequest returns the expected response",
        snapshotBuilderService.createSnapshotRequest(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email),
        equalTo(response));
  }

  @Test
  void enumerateSnapshotRequestsByDatasetId() {
    UUID datasetId = UUID.randomUUID();
    SnapshotAccessRequestResponse responseItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse();
    List<SnapshotAccessRequestResponse> response = List.of(responseItem);
    when(snapshotRequestDao.enumerateByDatasetId(datasetId)).thenReturn(response);

    EnumerateSnapshotAccessRequestItem expectedItem =
        new EnumerateSnapshotAccessRequestItem()
            .id(responseItem.getId())
            .status(responseItem.getStatus())
            .createdDate(responseItem.getCreatedDate())
            .name(responseItem.getSnapshotName())
            .researchPurpose(responseItem.getSnapshotResearchPurpose())
            .createdBy(responseItem.getCreatedBy());
    EnumerateSnapshotAccessRequest expected =
        new EnumerateSnapshotAccessRequest().addItemsItem(expectedItem);

    assertThat(
        "EnumerateByDatasetId returns the expected response",
        snapshotBuilderService.enumerateByDatasetId(datasetId),
        equalTo(expected));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  public void getConceptChildren(CloudPlatform cloudPlatform) {
    Dataset dataset = makeDataset(cloudPlatform);
    when(datasetService.retrieve(dataset.getId())).thenReturn(dataset);
    var concepts = List.of(new SnapshotBuilderConcept().name("concept1").id(1));
    mockRunQueryForConcepts(cloudPlatform, concepts, dataset);
    var response = snapshotBuilderService.getConceptChildren(dataset.getId(), 1, TEST_USER);
    assertThat(
        "getConceptChildren returns the expected response",
        response.getResult(),
        equalTo(concepts));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void searchConcepts(CloudPlatform cloudPlatform) {
    Dataset dataset = makeDataset(cloudPlatform);
    when(datasetService.retrieve(dataset.getId())).thenReturn(dataset);
    var concepts = List.of(new SnapshotBuilderConcept().name("concept1").id(1));
    mockRunQueryForConcepts(cloudPlatform, concepts, dataset);
    var response =
        snapshotBuilderService.searchConcepts(dataset.getId(), "condition", "cancer", TEST_USER);
    assertThat(
        "searchConcepts returns the expected response", response.getResult(), equalTo(concepts));
  }

  private DatasetModel makeDatasetModel() {
    return new DatasetModel().name("name").dataProject("project");
  }

  private static Dataset makeDataset(CloudPlatform cloudPlatform) {
    return new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform))
        .projectResource(new GoogleProjectResource().googleProjectId("project123"))
        .name("dataset123")
        .id(UUID.randomUUID());
  }

  private void mockRunQueryForConcepts(
      CloudPlatform cloudPlatform, List<SnapshotBuilderConcept> concepts, Dataset dataset) {
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper.of(cloudPlatform);
    if (cloudPlatformWrapper.isGcp()) {
      when(datasetService.retrieveModel(dataset, TEST_USER)).thenReturn(makeDatasetModel());
      when(bigQueryDatasetPdao.<SnapshotBuilderConcept>runQuery(any(), any(), any()))
          .thenReturn(concepts);
    } else {
      when(datasetService.getOrCreateExternalAzureDataSource(dataset, TEST_USER))
          .thenReturn("dataSource");
      when(azureSynapsePdao.<SnapshotBuilderConcept>runQuery(any(), any())).thenReturn(concepts);
    }
  }

  @Test
  void getTableNameGeneratorHandlesGCPCorrectly() {
    Dataset dataset = new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP));
    when(datasetService.retrieveModel(dataset, TEST_USER))
        .thenReturn(new DatasetModel().name("name").dataProject("data-project"));
    var tableNameGenerator = snapshotBuilderService.getTableNameGenerator(dataset, TEST_USER);
    assertThat(
        "The generated name is the same as the BQVisitor generated name",
        tableNameGenerator.generate("table"),
        equalTo(
            BigQueryVisitor.bqTableName(datasetService.retrieveModel(dataset, TEST_USER))
                .generate("table")));
  }

  @Test
  void getTableNameGeneratorHandlesAzureCorrectly() {
    String dataSourceName = "data-source";
    String tableName = "azure-table";
    Dataset dataset = new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.AZURE));
    when(datasetService.getOrCreateExternalAzureDataSource(dataset, TEST_USER))
        .thenReturn(dataSourceName);
    var tableNameGenerator = snapshotBuilderService.getTableNameGenerator(dataset, TEST_USER);
    assertThat(
        "The generated name is the same as the SynapseVisitor generated name",
        tableNameGenerator.generate(tableName),
        equalTo(SynapseVisitor.azureTableName(dataSourceName).generate(tableName)));
  }

  @Test
  void getRollupCountForCriteriaGroupsGeneratesAndRunsAQuery() {
    Dataset dataset =
        new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.AZURE))
            .projectResource(new GoogleProjectResource().googleProjectId("project123"))
            .name("dataset123")
            .id(UUID.randomUUID());
    TablePointer tablePointer = TablePointer.fromRawSql("table-name");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    Query query =
        new Query(
            List.of(
                new FieldVariable(new FieldPointer(tablePointer, "column-name"), tableVariable)),
            List.of(tableVariable));
    var criteriaQueryBuilderMock = mock(CriteriaQueryBuilder.class);
    when(datasetService.retrieve(dataset.getId())).thenReturn(dataset);
    when(criteriaQueryBuilderFactory.createCriteriaQueryBuilder(any(), any(), any()))
        .thenReturn(criteriaQueryBuilderMock);
    when(criteriaQueryBuilderMock.generateRollupCountsQueryForCriteriaGroupsList(any()))
        .thenReturn(query);
    when(azureSynapsePdao.runQuery(eq(query.renderSQL(any())), any())).thenReturn(List.of(5));
    int rollupCount =
        snapshotBuilderService.getRollupCountForCriteriaGroups(
            dataset.getId(), List.of(List.of()), TEST_USER);
    assertThat(
        "rollup count should be response from stubbed query runner", rollupCount, equalTo(5));
  }
}
