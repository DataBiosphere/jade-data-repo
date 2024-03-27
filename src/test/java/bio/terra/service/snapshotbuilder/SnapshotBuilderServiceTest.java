package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderGetConceptHierarchyResponse;
import bio.terra.model.SnapshotBuilderParentConcept;
import bio.terra.model.SnapshotBuilderSettings;
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
import bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
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
  @Mock private QueryBuilderFactory queryBuilderFactory;

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
            queryBuilderFactory);
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
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption
        .name("condition")
        .id(19)
        .tableName("condition_occurrence")
        .columnName("condition_concept_id");
    SnapshotBuilderSettings snapshotBuilderSettings =
        new SnapshotBuilderSettings().domainOptions(List.of(domainOption));
    when(snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataset.getId()))
        .thenReturn(snapshotBuilderSettings);
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
    when(queryBuilderFactory.criteriaQueryBuilder(any(), any(), any()))
        .thenReturn(criteriaQueryBuilderMock);
    when(criteriaQueryBuilderMock.generateRollupCountsQueryForCriteriaGroupsList(any()))
        .thenReturn(query);
    when(azureSynapsePdao.runQuery(
            eq(query.renderSQL(CloudPlatformWrapper.of(CloudPlatform.AZURE))), any()))
        .thenReturn(List.of(5));
    int rollupCount =
        snapshotBuilderService.getRollupCountForCriteriaGroups(
            dataset.getId(), List.of(List.of()), TEST_USER);
    assertThat(
        "rollup count should be response from stubbed query runner", rollupCount, equalTo(5));
  }

  @ParameterizedTest
  @MethodSource
  void fuzzyLowCount(int rollupCount, int expectedFuzzyLowCount) {
    assertThat(
        "fuzzyLowCount should match rollup count unless rollup count is between 1 and 19, inclusive. Then, it should return 19.",
        snapshotBuilderService.fuzzyLowCount(rollupCount),
        equalTo(expectedFuzzyLowCount));
  }

  private static Stream<Arguments> fuzzyLowCount() {
    return Stream.of(
        arguments(0, 0),
        arguments(1, 19),
        arguments(3, 19),
        arguments(18, 19),
        arguments(19, 19),
        arguments(20, 20),
        arguments(21, 21));
  }

  private <T> void mockRunQueryForHierarchy(
      CloudPlatform cloudPlatform, List<T> results, Dataset dataset) {
    CloudPlatformWrapper.of(cloudPlatform)
        .choose(
            () ->
                when(bigQueryDatasetPdao.runQuery(
                        any(),
                        any(),
                        argThat(
                            (ArgumentMatcher<BigQueryDatasetPdao.Converter<T>>) Objects::nonNull)))
                    .thenReturn(results),
            () ->
                when(azureSynapsePdao.runQuery(
                        any(),
                        argThat((ArgumentMatcher<AzureSynapsePdao.Converter<T>>) Objects::nonNull)))
                    .thenReturn(results));
  }

  private static void assertParentQueryResult(SnapshotBuilderService.ParentQueryResult result) {
    assertThat(result.parentId(), is(1));
    assertThat(result.childId(), is(2));
    assertThat(result.childName(), is("name"));
  }

  @Test
  void testParentQueryResult() throws Exception {
    var resultSet = mock(ResultSet.class);
    when(resultSet.getInt(HierarchyQueryBuilder.PARENT_ID)).thenReturn(1);
    when(resultSet.getInt(HierarchyQueryBuilder.CONCEPT_ID)).thenReturn(2);
    when(resultSet.getString(HierarchyQueryBuilder.CONCEPT_NAME)).thenReturn("name");
    assertParentQueryResult(new SnapshotBuilderService.ParentQueryResult(resultSet));

    var fieldValueList =
        FieldValueList.of(
            List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "name")),
            Field.of(HierarchyQueryBuilder.PARENT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(HierarchyQueryBuilder.CONCEPT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(HierarchyQueryBuilder.CONCEPT_NAME, StandardSQLTypeName.STRING));

    assertParentQueryResult(new SnapshotBuilderService.ParentQueryResult(fieldValueList));
  }

  static SnapshotBuilderConcept concept(String name, int id) {
    return new SnapshotBuilderConcept().name(name).id(id).count(1).hasChildren(true);
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void getConceptHierarchy(CloudPlatform platform) {
    Dataset dataset = makeDataset(platform);
    var conceptId = 1;
    when(datasetService.retrieve(dataset.getId())).thenReturn(dataset);
    var queryBuilder = mock(HierarchyQueryBuilder.class);
    when(queryBuilderFactory.hierarchyQueryBuilder(any())).thenReturn(queryBuilder);
    when(queryBuilder.generateQuery(conceptId)).thenReturn(mock(Query.class));
    var concept1 = concept("concept1", 1);
    var concept2 = concept("concept2", 2);
    var concept3 = concept("concept3", 3);
    var results =
        List.of(
            new SnapshotBuilderService.ParentQueryResult(0, concept1.getId(), concept1.getName()),
            new SnapshotBuilderService.ParentQueryResult(0, concept2.getId(), concept2.getName()),
            new SnapshotBuilderService.ParentQueryResult(
                concept1.getId(), concept3.getId(), concept3.getName()));
    mockRunQueryForHierarchy(platform, results, dataset);
    assertThat(
        snapshotBuilderService.getConceptHierarchy(dataset.getId(), conceptId, TEST_USER),
        equalTo(
            new SnapshotBuilderGetConceptHierarchyResponse()
                .result(
                    List.of(
                        new SnapshotBuilderParentConcept()
                            .parentId(0)
                            .children(List.of(concept1, concept2)),
                        new SnapshotBuilderParentConcept()
                            .parentId(concept1.getId())
                            .children(List.of(concept3))))));
  }

  @Test
  void moveRootToFirst() {
    // Use a TreeMap to ensure that the root concept is last. With HashMap, the order is not fixed.
    Map<Integer, SnapshotBuilderParentConcept> parents = new TreeMap<>();
    parents.put(2, new SnapshotBuilderParentConcept().parentId(2).children(List.of()));
    parents.put(
        1, new SnapshotBuilderParentConcept().parentId(1).children(List.of(concept("child", 2))));

    var result = SnapshotBuilderService.moveRootToFirst(parents);

    assertThat(result.get(0).getParentId(), is(1));
  }
}
