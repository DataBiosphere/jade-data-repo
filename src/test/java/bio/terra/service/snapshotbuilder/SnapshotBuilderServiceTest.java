package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.ApiException;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderGetConceptHierarchyResponse;
import bio.terra.model.SnapshotBuilderParentConcept;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SnapshotModel;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.utils.ConceptChildrenQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.constants.Concept;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.sql.ResultSet;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotBuilderServiceTest {
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private SnapshotBuilderService snapshotBuilderService;
  @Mock private IamService iamService;
  @Mock private SnapshotService snapshotService;
  @Mock private DatasetService datasetService;
  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
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
            iamService,
            snapshotService,
            bigQuerySnapshotPdao,
            azureSynapsePdao,
            queryBuilderFactory);
  }

  @Test
  void createRequest() {
    UUID snapshotId = UUID.randomUUID();
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse();
    when(snapshotRequestDao.create(
            SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId), TEST_USER.getEmail()))
        .thenReturn(response);
    when(iamService.createSnapshotBuilderRequestResource(eq(TEST_USER), any(), any()))
        .thenReturn(Map.of(IamRole.OWNER, List.of(TEST_USER.getEmail())));
    assertThat(
        "createSnapshotRequest returns the expected response",
        snapshotBuilderService.createRequest(
            TEST_USER, SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId)),
        equalTo(response));
  }

  @Test
  void createRequestRollsBackIfSamFails() {
    UUID snapshotId = UUID.randomUUID();
    UUID snapshotRequestId = UUID.randomUUID();
    SnapshotAccessRequestResponse response =
        new SnapshotAccessRequestResponse().id(snapshotRequestId);
    when(snapshotRequestDao.create(
            SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId), TEST_USER.getEmail()))
        .thenReturn(response);
    when(iamService.createSnapshotBuilderRequestResource(eq(TEST_USER), any(), any()))
        .thenThrow(new ApiException("Error"));
    doNothing().when(snapshotRequestDao).delete(snapshotRequestId);
    assertThrows(
        InternalServerErrorException.class,
        () ->
            snapshotBuilderService.createRequest(
                TEST_USER, SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId)));
  }

  @Test
  void enumerateRequests() {
    SnapshotAccessRequestResponse responseItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(UUID.randomUUID());
    List<SnapshotAccessRequestResponse> response = List.of(responseItem);
    when(snapshotRequestDao.enumerate(Set.of(responseItem.getId()))).thenReturn(response);

    EnumerateSnapshotAccessRequest expected =
        new EnumerateSnapshotAccessRequest().addItemsItem(responseItem);

    assertThat(
        "EnumerateByDatasetId returns the expected response",
        snapshotBuilderService.enumerateRequests(Set.of(responseItem.getId())),
        equalTo(expected));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void getConceptChildren(CloudPlatform cloudPlatform) {
    Snapshot snapshot = makeSnapshot(cloudPlatform);
    when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);

    var queryBuilder = mock(ConceptChildrenQueryBuilder.class);
    when(queryBuilderFactory.conceptChildrenQueryBuilder()).thenReturn(queryBuilder);

    when(queryBuilder.retrieveDomainId(1)).thenReturn(mock(Query.class));
    when(queryBuilder.buildConceptChildrenQuery(any(), eq(1))).thenReturn(mock(Query.class));

    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption.name("domainId").tableName("domainTable").columnName("domain_concept_id");
    SnapshotBuilderSettings settings =
        new SnapshotBuilderSettings().domainOptions(List.of(domainOption));
    when(snapshotBuilderSettingsDao.getBySnapshotId(any())).thenReturn(settings);

    var concept =
        new SnapshotBuilderConcept()
            .name("childConcept")
            .id(2)
            .count(1)
            .hasChildren(true)
            .code("100")
            .count(99);
    mockRunQueryForGetConcepts(concept("childConcept", 2, true), snapshot, "domainId");

    var response = snapshotBuilderService.getConceptChildren(snapshot.getId(), 1, TEST_USER);
    assertThat(
        "getConceptChildren returns the expected response",
        response.getResult(),
        equalTo(List.of(concept)));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void searchConcepts(CloudPlatform cloudPlatform) {
    Snapshot snapshot = makeSnapshot(cloudPlatform);
    when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption
        .name("condition")
        .id(19)
        .tableName("condition_occurrence")
        .columnName("condition_concept_id");
    SnapshotBuilderSettings snapshotBuilderSettings =
        new SnapshotBuilderSettings().domainOptions(List.of(domainOption));
    when(snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId()))
        .thenReturn(snapshotBuilderSettings);

    var queryBuilder = mock(SearchConceptsQueryBuilder.class);
    when(queryBuilderFactory.searchConceptsQueryBuilder()).thenReturn(queryBuilder);
    when(queryBuilder.buildSearchConceptsQuery(any(), any())).thenReturn(mock(Query.class));

    var concept = new SnapshotBuilderConcept().name("concept1").id(1);
    mockRunQueryForSearchConcepts(concept, snapshot);
    var response =
        snapshotBuilderService.enumerateConcepts(
            snapshot.getId(), domainOption.getId(), "cancer", TEST_USER);
    assertThat(
        "searchConcepts returns the expected response",
        response.getResult(),
        equalTo(List.of(concept)));
  }

  @Test
  void searchConceptsUnknownDomain() {
    Snapshot snapshot = makeSnapshot(CloudPlatform.GCP);
    UUID snapshotId = snapshot.getId();
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    var domainOption = new SnapshotBuilderDomainOption();
    domainOption.setId(19);
    when(snapshotBuilderSettingsDao.getBySnapshotId(snapshotId))
        .thenReturn(new SnapshotBuilderSettings().domainOptions(List.of(domainOption)));
    assertThrows(
        BadRequestException.class,
        () -> snapshotBuilderService.enumerateConcepts(snapshotId, 20, "cancer", TEST_USER));
  }

  private static Snapshot makeSnapshot(CloudPlatform cloudPlatform) {
    return new Snapshot()
        .snapshotSources(List.of(new SnapshotSource().dataset(makeDataset(cloudPlatform))))
        .name("snapshot123")
        .id(UUID.randomUUID());
  }

  private static Dataset makeDataset(CloudPlatform cloudPlatform) {
    return new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform))
        .projectResource(new GoogleProjectResource().googleProjectId("project123"))
        .name("dataset123")
        .id(UUID.randomUUID());
  }

  private void mockRunQueryForSearchConcepts(SnapshotBuilderConcept concept, Snapshot snapshot) {
    mockRunQuery(snapshot).thenReturn(List.of(concept));
  }

  private void mockRunQueryForGetConcepts(
      SnapshotBuilderConcept concept, Snapshot snapshot, String domainId) {
    mockRunQuery(snapshot).thenReturn(List.of(domainId)).thenReturn(List.of(concept));
  }

  private void mockRunQueryForHierarchy(
      Snapshot snapshot, String domainId, List<SnapshotBuilderService.ParentQueryResult> results) {
    mockRunQuery(snapshot).thenReturn(List.of(domainId)).thenReturn(List.copyOf(results));
  }

  private <T> org.mockito.stubbing.OngoingStubbing<List<T>> mockRunQuery(Snapshot snapshot) {
    return CloudPlatformWrapper.of(snapshot.getCloudPlatform())
        .choose(
            () -> {
              // Not sure if we need this mock?
              // when(snapshotService.retrieve(snapshot, TEST_USER)).thenReturn(makeDatasetModel());
              return when(bigQuerySnapshotPdao.runQuery(any(), any(), any()));
            },
            () -> {
              when(snapshotService.getOrCreateExternalAzureDataSource(snapshot, TEST_USER))
                  .thenReturn("dataSource");
              return when(azureSynapsePdao.runQuery(any(), any()));
            });
  }

  @Test
  void getTableNameGeneratorHandlesGCPCorrectly() {
    Dataset dataset = new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP));
    Snapshot snapshot =
        new Snapshot()
            .id(UUID.randomUUID())
            .snapshotSources(List.of(new SnapshotSource().dataset(dataset)));
    when(snapshotService.retrieveSnapshotModel(snapshot.getId(), TEST_USER))
        .thenReturn(new SnapshotModel().name("name").dataProject("data-project"));
    var renderContext = snapshotBuilderService.createContext(snapshot, TEST_USER);
    assertThat(
        "The generated name is the same as the BQVisitor generated name",
        renderContext.getTableName("table"),
        equalTo(
            BigQueryVisitor.bqSnapshotTableName(
                    snapshotService.retrieveSnapshotModel(snapshot.getId(), TEST_USER))
                .generate("table")));
  }

  @Test
  void getTableNameGeneratorHandlesAzureCorrectly() {
    String dataSourceName = "data-source";
    String tableName = "azure-table";
    Dataset dataset = new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.AZURE));
    Snapshot snapshot =
        new Snapshot().snapshotSources(List.of(new SnapshotSource().dataset(dataset)));
    when(snapshotService.getOrCreateExternalAzureDataSource(snapshot, TEST_USER))
        .thenReturn(dataSourceName);
    var renderContext = snapshotBuilderService.createContext(snapshot, TEST_USER);
    assertThat(
        "The generated name is the same as the SynapseVisitor generated name",
        renderContext.getTableName(tableName),
        equalTo(SynapseVisitor.azureTableName(dataSourceName).generate(tableName)));
  }

  @Test
  void getRollupCountForCriteriaGroupsGeneratesAndRunsAQuery() {
    Snapshot snapshot = makeSnapshot(CloudPlatform.AZURE);
    var settings = new SnapshotBuilderSettings();
    when(snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId())).thenReturn(settings);
    Query query = mock(Query.class);
    var criteriaQueryBuilderMock = mock(CriteriaQueryBuilder.class);
    when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);
    when(queryBuilderFactory.criteriaQueryBuilder("person", settings))
        .thenReturn(criteriaQueryBuilderMock);
    var cohorts = List.of(new SnapshotBuilderCohort());
    when(criteriaQueryBuilderMock.generateRollupCountsQueryForCohorts(cohorts)).thenReturn(query);
    String sql = "sql";
    // Use a captor to verify that the context was created using the dataset's cloud platform.
    var contextArgument = ArgumentCaptor.forClass(SqlRenderContext.class);
    when(query.renderSQL(contextArgument.capture())).thenReturn(sql);
    var count = 5;
    when(azureSynapsePdao.runQuery(eq(sql), any())).thenReturn(List.of(count));
    int rollupCount =
        snapshotBuilderService.getRollupCountForCohorts(snapshot.getId(), cohorts, TEST_USER);
    assertThat(
        "rollup count should be response from stubbed query runner", rollupCount, equalTo(count));
    assertThat(
        contextArgument.getValue().getPlatform().getCloudPlatform(),
        is(snapshot.getCloudPlatform()));
  }

  @ParameterizedTest
  @MethodSource
  void fuzzyLowCount(int rollupCount, int expectedFuzzyLowCount) {
    assertThat(
        "fuzzyLowCount should match rollup count unless rollup count is between 1 and 19, inclusive. Then, it should return 19.",
        SnapshotBuilderService.fuzzyLowCount(rollupCount),
        equalTo(expectedFuzzyLowCount));
  }

  @Test
  void generateRowIdQuery() {
    UUID snapshotId = UUID.randomUUID();
    SnapshotAccessRequestResponse accessRequest =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(snapshotId);

    Dataset dataset = makeDataset(CloudPlatform.GCP);
    Snapshot snapshot =
        makeSnapshot(CloudPlatform.GCP)
            .snapshotSources(List.of(new SnapshotSource().dataset(dataset)));

    when(snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId()))
        .thenReturn(SnapshotBuilderTestData.SETTINGS);

    Query query = mock(Query.class);
    var criteriaQueryBuilderMock = mock(CriteriaQueryBuilder.class);
    when(queryBuilderFactory.criteriaQueryBuilder("person", SnapshotBuilderTestData.SETTINGS))
        .thenReturn(criteriaQueryBuilderMock);
    when(criteriaQueryBuilderMock.generateRowIdQueryForCohorts(
            accessRequest.getSnapshotSpecification().getCohorts()))
        .thenReturn(query);
    var contextArgument = ArgumentCaptor.forClass(SqlRenderContext.class);
    when(query.renderSQL(contextArgument.capture())).thenReturn("sql");

    assertEquals(
        "sql", snapshotBuilderService.generateRowIdQuery(accessRequest, snapshot, TEST_USER));
    assertThat(
        contextArgument.getValue().getPlatform().getCloudPlatform(),
        is(dataset.getCloudPlatform()));
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

  private static void assertParentQueryResult(SnapshotBuilderService.ParentQueryResult result) {
    assertThat(result.parentId(), is(1));
    assertThat(result.childId(), is(2));
    assertThat(result.childName(), is("name"));
  }

  @Test
  void testParentQueryResult() throws Exception {
    var resultSet = mock(ResultSet.class);
    when(resultSet.getInt(QueryBuilderFactory.PARENT_ID)).thenReturn(1);
    when(resultSet.getInt(Concept.CONCEPT_ID)).thenReturn(2);
    when(resultSet.getString(Concept.CONCEPT_NAME)).thenReturn("name");
    when(resultSet.getBoolean(QueryBuilderFactory.HAS_CHILDREN)).thenReturn(true);
    assertParentQueryResult(new SnapshotBuilderService.ParentQueryResult(resultSet));

    var fieldValueList =
        FieldValueList.of(
            List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "name"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "99"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
            Field.of(QueryBuilderFactory.PARENT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(Concept.CONCEPT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(Concept.CONCEPT_NAME, StandardSQLTypeName.STRING),
            Field.of(Concept.CONCEPT_CODE, StandardSQLTypeName.STRING),
            Field.of(QueryBuilderFactory.COUNT, StandardSQLTypeName.NUMERIC),
            Field.of(QueryBuilderFactory.HAS_CHILDREN, StandardSQLTypeName.BOOL));

    assertParentQueryResult(new SnapshotBuilderService.ParentQueryResult(fieldValueList));
  }

  @Test
  void testRejectRequest() {
    UUID id = UUID.randomUUID();
    snapshotBuilderService.rejectRequest(id);
    verify(snapshotRequestDao).update(id, SnapshotAccessRequestStatus.REJECTED);
  }

  @Test
  void testApproveRequest() {
    UUID id = UUID.randomUUID();
    snapshotBuilderService.approveRequest(id);
    verify(snapshotRequestDao).update(id, SnapshotAccessRequestStatus.APPROVED);
  }

  static SnapshotBuilderConcept concept(String name, int id, boolean hasChildren) {
    return new SnapshotBuilderConcept()
        .name(name)
        .id(id)
        .count(99)
        .hasChildren(hasChildren)
        .code("100");
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void getConceptHierarchy(CloudPlatform platform) {
    Snapshot snapshot = makeSnapshot(platform);
    var conceptId = 1;
    when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);
    var queryBuilder = mock(HierarchyQueryBuilder.class);
    when(queryBuilderFactory.hierarchyQueryBuilder()).thenReturn(queryBuilder);
    when(queryBuilderFactory.conceptChildrenQueryBuilder())
        .thenReturn(mock(ConceptChildrenQueryBuilder.class, Mockito.RETURNS_DEEP_STUBS));
    var domain = new SnapshotBuilderDomainOption();
    domain.setName("domain");
    var settings = new SnapshotBuilderSettings().domainOptions(List.of(domain));
    when(snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId())).thenReturn(settings);
    when(queryBuilder.generateQuery(domain, conceptId)).thenReturn(mock(Query.class));
    var concept1 = concept("concept1", 1, true);
    var concept2 = concept("concept2", 2, false);
    var concept3 = concept("concept3", 3, false);
    var results =
        List.of(
            createResult(0, concept1),
            createResult(0, concept2),
            createResult(concept1.getId(), concept3));
    mockRunQueryForHierarchy(snapshot, domain.getName(), results);
    assertThat(
        snapshotBuilderService.getConceptHierarchy(snapshot.getId(), conceptId, TEST_USER),
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

  private static SnapshotBuilderService.ParentQueryResult createResult(
      int parentId, SnapshotBuilderConcept concept) {
    return new SnapshotBuilderService.ParentQueryResult(
        parentId,
        concept.getId(),
        concept.getName(),
        concept.getCode(),
        concept.getCount(),
        concept.isHasChildren());
  }
}
