package bio.terra.service.snapshotbuilder;

import static bio.terra.service.snapshotbuilder.SnapshotBuilderService.validateGroupParams;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.TerraConfiguration;
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
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestMembersResponse;
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
import bio.terra.service.notification.NotificationService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import bio.terra.service.snapshotbuilder.utils.ConceptChildrenQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.EnumerateConceptsQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
  @Mock private NotificationService notificationService;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private QueryBuilderFactory queryBuilderFactory;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final TerraConfiguration terraConfiguration = new TerraConfiguration("basepath");

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
            notificationService,
            azureSynapsePdao,
            queryBuilderFactory,
            terraConfiguration);
  }

  @Test
  void createRequest() {
    UUID snapshotId = UUID.randomUUID();
    SnapshotAccessRequestModel model = SnapshotBuilderTestData.createAccessRequestModelApproved();
    when(snapshotRequestDao.create(
            SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId), TEST_USER.getEmail()))
        .thenReturn(model);
    when(iamService.createSnapshotBuilderRequestResource(eq(TEST_USER), any(), any()))
        .thenReturn(Map.of(IamRole.OWNER, List.of(TEST_USER.getEmail())));
    assertThat(
        "createSnapshotRequest returns the expected response",
        snapshotBuilderService.createRequest(
            TEST_USER, SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId)),
        equalTo(model.toApiResponse()));
  }

  @Test
  void createRequestRollsBackIfSamFails() {
    UUID snapshotId = UUID.randomUUID();
    SnapshotAccessRequestModel model = SnapshotBuilderTestData.createAccessRequestModelApproved();
    SnapshotAccessRequest request = SnapshotBuilderTestData.createSnapshotAccessRequest(snapshotId);
    when(snapshotRequestDao.create(request, TEST_USER.getEmail())).thenReturn(model);
    when(iamService.createSnapshotBuilderRequestResource(eq(TEST_USER), any(), any()))
        .thenThrow(new ApiException("Error"));
    doNothing().when(snapshotRequestDao).delete(model.id());
    assertThrows(
        InternalServerErrorException.class,
        () -> snapshotBuilderService.createRequest(TEST_USER, request));
  }

  @Test
  void enumerateRequests() {
    SnapshotAccessRequestModel responseItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestModel(UUID.randomUUID());
    List<SnapshotAccessRequestModel> response = List.of(responseItem);
    when(snapshotRequestDao.enumerate(Set.of(responseItem.id()))).thenReturn(response);
    EnumerateSnapshotAccessRequest expected =
        new EnumerateSnapshotAccessRequest().addItemsItem(responseItem.toApiResponse());

    assertThat(
        "EnumerateByDatasetId returns the expected response",
        snapshotBuilderService.enumerateRequests(Set.of(responseItem.id())),
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
  void enumerateConcepts(CloudPlatform cloudPlatform) {
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

    var queryBuilder = mock(EnumerateConceptsQueryBuilder.class);
    when(queryBuilderFactory.enumerateConceptsQueryBuilder()).thenReturn(queryBuilder);
    when(queryBuilder.buildEnumerateConceptsQuery(any(), eq(true))).thenReturn(mock(Query.class));

    var concept = new SnapshotBuilderConcept().name("concept1").id(1);
    mockRunQueryForEnumerateConcepts(concept, snapshot);
    var response =
        snapshotBuilderService.enumerateConcepts(
            snapshot.getId(), domainOption.getId(), "cancer", TEST_USER);
    assertThat(
        "enumerateConcepts returns the expected response",
        response.getResult(),
        equalTo(List.of(concept)));
  }

  @Test
  void enumerateConceptsUnknownDomain() {
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

  private void mockRunQueryForEnumerateConcepts(SnapshotBuilderConcept concept, Snapshot snapshot) {
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
            () -> when(bigQuerySnapshotPdao.runQuery(any(), any(), any(), any())),
            () -> {
              when(snapshotService.getOrCreateExternalAzureDataSource(snapshot, TEST_USER))
                  .thenReturn("dataSource");
              return when(azureSynapsePdao.runQuery(any(), any(), any()));
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
    when(queryBuilderFactory.criteriaQueryBuilder(settings)).thenReturn(criteriaQueryBuilderMock);
    var cohorts = List.of(new SnapshotBuilderCohort());
    when(criteriaQueryBuilderMock.generateRollupCountsQueryForCohorts(cohorts)).thenReturn(query);
    String sql = "sql";
    // Use a captor to verify that the context was created using the dataset's cloud platform.
    var contextArgument = ArgumentCaptor.forClass(SqlRenderContext.class);
    when(query.renderSQL(contextArgument.capture())).thenReturn(sql);
    var count = 5;
    when(azureSynapsePdao.runQuery(eq(sql), any(), any())).thenReturn(List.of(count));
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
    SnapshotAccessRequestModel snapshotAccessRequestModel =
        SnapshotBuilderTestData.createSnapshotAccessRequestModel(snapshotId);

    Dataset dataset = makeDataset(CloudPlatform.GCP);
    Snapshot snapshot =
        makeSnapshot(CloudPlatform.GCP)
            .snapshotSources(List.of(new SnapshotSource().dataset(dataset)));

    when(snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId()))
        .thenReturn(SnapshotBuilderTestData.SETTINGS);

    Query query = mock(Query.class);
    var criteriaQueryBuilderMock = mock(CriteriaQueryBuilder.class);
    when(queryBuilderFactory.criteriaQueryBuilder(SnapshotBuilderTestData.SETTINGS))
        .thenReturn(criteriaQueryBuilderMock);
    when(criteriaQueryBuilderMock.generateRowIdQueryForCohorts(
            snapshotAccessRequestModel.snapshotSpecification().getCohorts()))
        .thenReturn(query);
    var contextArgument = ArgumentCaptor.forClass(SqlRenderContext.class);
    when(query.renderSQL(contextArgument.capture())).thenReturn("sql");

    assertEquals(
        "sql",
        snapshotBuilderService.generateRowIdQuery(snapshotAccessRequestModel, snapshot, TEST_USER));
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
    var expected = new SnapshotBuilderService.ParentQueryResult(1, 2, "name", "100", 99, true);

    var resultSet = mock(ResultSet.class);
    when(resultSet.getInt(QueryBuilderFactory.PARENT_ID)).thenReturn(expected.parentId());
    when(resultSet.getInt(Concept.CONCEPT_ID)).thenReturn(expected.childId());
    when(resultSet.getString(Concept.CONCEPT_NAME)).thenReturn(expected.childName());
    when(resultSet.getString(Concept.CONCEPT_CODE)).thenReturn(expected.code());
    when(resultSet.getInt(QueryBuilderFactory.COUNT)).thenReturn(expected.count());
    when(resultSet.getInt(QueryBuilderFactory.HAS_CHILDREN))
        .thenReturn(expected.hasChildren() ? 1 : 0);
    assertParentQueryResult(new SnapshotBuilderService.ParentQueryResult(resultSet));

    var fieldValueList =
        FieldValueList.of(
            List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.parentId())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.childId())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, expected.childName()),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, expected.code()),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.count())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, expected.hasChildren() ? "1" : "0")),
            Field.of(QueryBuilderFactory.PARENT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(Concept.CONCEPT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(Concept.CONCEPT_NAME, StandardSQLTypeName.STRING),
            Field.of(Concept.CONCEPT_CODE, StandardSQLTypeName.STRING),
            Field.of(QueryBuilderFactory.COUNT, StandardSQLTypeName.NUMERIC),
            Field.of(QueryBuilderFactory.HAS_CHILDREN, StandardSQLTypeName.NUMERIC));

    assertParentQueryResult(new SnapshotBuilderService.ParentQueryResult(fieldValueList));
  }

  @Test
  void testRejectRequest() {
    UUID id = UUID.randomUUID();
    var response = SnapshotBuilderTestData.createAccessRequestModelApproved();
    when(snapshotRequestDao.getById(id)).thenReturn(response);
    assertThat(snapshotBuilderService.rejectRequest(id), is(response.toApiResponse()));
    verify(snapshotRequestDao).updateStatus(id, SnapshotAccessRequestStatus.REJECTED);
  }

  @Test
  void testApproveRequest() {
    var response = SnapshotBuilderTestData.createAccessRequestModelApproved();
    UUID id = response.id();
    when(snapshotRequestDao.getById(id)).thenReturn(response);
    assertThat(snapshotBuilderService.approveRequest(id), is(response.toApiResponse()));
    verify(snapshotRequestDao).updateStatus(id, SnapshotAccessRequestStatus.APPROVED);
  }

  @Test
  void testGetRequest() {
    var daoResponse = SnapshotBuilderTestData.createAccessRequestModelApproved();
    UUID id = daoResponse.id();
    when(snapshotRequestDao.getById(id)).thenReturn(daoResponse);
    assertThat(snapshotBuilderService.getRequest(id), is(daoResponse.toApiResponse()));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void testGetRequestDetails(CloudPlatform platform) {
    Snapshot snapshot = makeSnapshot(platform);
    var daoResponse = SnapshotBuilderTestData.createSnapshotAccessRequestModel(snapshot.getId());
    List<Integer> conceptIds = daoResponse.generateConceptIds();
    UUID id = daoResponse.id();
    var queryBuilder = mock(EnumerateConceptsQueryBuilder.class);
    when(queryBuilderFactory.enumerateConceptsQueryBuilder()).thenReturn(queryBuilder);
    when(queryBuilder.getConceptsFromConceptIds(conceptIds)).thenReturn(mock(Query.class));
    when(snapshotRequestDao.getById(id)).thenReturn(daoResponse);
    when(snapshotBuilderSettingsDao.getBySnapshotId(daoResponse.sourceSnapshotId()))
        .thenReturn(SnapshotBuilderTestData.SETTINGS);
    when(snapshotService.retrieve(daoResponse.sourceSnapshotId())).thenReturn(snapshot);
    List<Map.Entry<Integer, String>> conceptIdsAndNames =
        conceptIds.stream()
            .map(conceptId -> Map.entry(conceptId, String.format("Concept name %d", conceptId)))
            .toList();
    mockRunQuery(snapshot).thenReturn(List.copyOf(conceptIdsAndNames));
    assertThat(
        snapshotBuilderService.getRequestDetails(TEST_USER, id),
        is(
            daoResponse.generateModelDetails(
                SnapshotBuilderTestData.SETTINGS,
                conceptIdsAndNames.stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))));
  }

  @Test
  void testDeleteRequest() {
    UUID id = UUID.randomUUID();
    snapshotBuilderService.deleteRequest(TEST_USER, id);
    verify(snapshotRequestDao).updateStatus(id, SnapshotAccessRequestStatus.DELETED);
    verify(iamService).deleteSnapshotBuilderRequest(TEST_USER, id);
  }

  @Test
  void testEnumerateRequestsBySnapshot() {
    UUID id = UUID.randomUUID();
    List<SnapshotAccessRequestModel> daoResponse =
        List.of(SnapshotBuilderTestData.createAccessRequestModelApproved());
    when(snapshotRequestDao.enumerateBySnapshot(id)).thenReturn(daoResponse);
    assertThat(
        snapshotBuilderService.enumerateRequestsBySnapshot(id),
        is(
            new EnumerateSnapshotAccessRequest()
                .items(
                    daoResponse.stream().map(SnapshotAccessRequestModel::toApiResponse).toList())));
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

  @Test
  void createExportSnapshotLink() {
    UUID snapshotId = UUID.randomUUID();
    var link = snapshotBuilderService.createExportSnapshotLink(snapshotId);
    assertThat(
        link,
        allOf(
            containsString(terraConfiguration.basePath()), containsString(snapshotId.toString())));
  }

  @Test
  void notifySnapshotReady() {
    var request = SnapshotBuilderTestData.createAccessRequestModelApproved();
    when(snapshotRequestDao.getById(request.id())).thenReturn(request);
    var snapshot = new Snapshot().name("name");
    when(snapshotService.retrieve(request.createdSnapshotId())).thenReturn(snapshot);
    when(snapshotBuilderSettingsDao.getBySnapshotId(request.sourceSnapshotId()))
        .thenReturn(SnapshotBuilderTestData.SETTINGS);
    String id = "id";
    snapshotBuilderService.notifySnapshotReady(TEST_USER, id, request.id());
    verify(notificationService)
        .snapshotReady(
            eq(id), anyString(), eq(snapshot.getName()), eq("No snapshot specification found"));
  }

  @Nested
  class GroupMemberApis {

    SnapshotAccessRequestModel requestModel =
        SnapshotBuilderTestData.createAccessRequestModelSnapshotCreated();
    UUID requestId = new SnapshotAccessRequestResponse().getId();

    @BeforeEach
    void beforeEach() {
      when(snapshotRequestDao.getById(requestId)).thenReturn(requestModel);
    }

    @Test
    void testGetRequestGroupMembers() {
      SnapshotAccessRequestMembersResponse expected =
          new SnapshotAccessRequestMembersResponse().members(List.of());
      when(iamService.getGroupPolicyEmails(requestModel.samGroupName(), IamRole.MEMBER.toString()))
          .thenReturn(expected.getMembers());
      assertThat(snapshotBuilderService.getGroupMembers(requestId), is(expected));
    }

    @Test
    void testAddRequestGroupMember() {
      String memberEmail = "user@gmail.com";
      SnapshotAccessRequestMembersResponse expected =
          new SnapshotAccessRequestMembersResponse().members(List.of(memberEmail));
      when(iamService.addEmailToGroup(
              requestModel.samGroupName(), IamRole.MEMBER.toString(), memberEmail))
          .thenReturn(expected.getMembers());
      assertThat(snapshotBuilderService.addGroupMember(requestId, memberEmail), is(expected));
    }

    @Test
    void testDeleteRequestGroupMembers() {
      String memberEmail = "user@gmail.com";
      SnapshotAccessRequestMembersResponse expected =
          new SnapshotAccessRequestMembersResponse().members(List.of());
      when(iamService.removeEmailFromGroup(
              requestModel.samGroupName(), IamRole.MEMBER.toString(), memberEmail))
          .thenReturn(expected.getMembers());
      assertThat(snapshotBuilderService.deleteGroupMember(requestId, memberEmail), is(expected));
    }
  }

  @Nested
  class ValidateParameters {
    String badEmail = "badEmail";
    String validEmail = "user@gmail.com";
    SnapshotAccessRequestModel requestModelBlankGroup =
        SnapshotBuilderTestData.createAccessRequestModelApproved();
    SnapshotAccessRequestModel requestModel =
        SnapshotBuilderTestData.createAccessRequestModelSnapshotCreated();

    @Test
    void testInvalidEmail() {
      assertThrows(
          IllegalArgumentException.class, () -> validateGroupParams(requestModel, badEmail));
    }

    @Test
    void testBlankSamGroup() {
      assertThrows(
          IllegalArgumentException.class,
          () -> validateGroupParams(requestModelBlankGroup, validEmail));
    }

    @Test
    void testInvalidEmailAndGroup() {
      assertThrows(
          IllegalArgumentException.class,
          () -> validateGroupParams(requestModelBlankGroup, badEmail));
    }

    @Test
    void testValidEmailAndGroup() {
      assertDoesNotThrow(() -> validateGroupParams(requestModel, validEmail));
    }

    @Test
    void testNoEmailAndValidGroup() {
      assertDoesNotThrow(() -> validateGroupParams(requestModel, null));
    }

    @Test
    void testNoEmailAndBlankGroup() {
      assertThrows(
          IllegalArgumentException.class, () -> validateGroupParams(requestModelBlankGroup, null));
    }
  }
}
