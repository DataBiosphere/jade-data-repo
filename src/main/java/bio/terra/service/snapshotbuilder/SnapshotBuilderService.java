package bio.terra.service.snapshotbuilder;

import static bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory.FILTER_TEXT;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.ApiException;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderConceptsResponse;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderGetConceptHierarchyResponse;
import bio.terra.model.SnapshotBuilderParentConcept;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import com.google.cloud.bigquery.FieldValueList;
import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.thymeleaf.util.StringUtils;

@Component
public class SnapshotBuilderService {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotBuilderService.class);

  private final SnapshotRequestDao snapshotRequestDao;
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private final DatasetService datasetService;
  private final IamService iamService;
  private final SnapshotService snapshotService;
  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;

  private final AzureSynapsePdao azureSynapsePdao;
  private final QueryBuilderFactory queryBuilderFactory;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao,
      DatasetService datasetService,
      IamService iamService,
      SnapshotService snapshotService,
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      AzureSynapsePdao azureSynapsePdao,
      QueryBuilderFactory queryBuilderFactory) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
    this.datasetService = datasetService;
    this.iamService = iamService;
    this.snapshotService = snapshotService;
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.azureSynapsePdao = azureSynapsePdao;
    this.queryBuilderFactory = queryBuilderFactory;
  }

  public SnapshotAccessRequestResponse createRequest(
      AuthenticatedUserRequest userRequest, SnapshotAccessRequest snapshotAccessRequest) {
    SnapshotAccessRequestResponse snapshotAccessRequestResponse =
        snapshotRequestDao.create(snapshotAccessRequest, userRequest.getEmail());
    try {
      iamService.createSnapshotBuilderRequestResource(
          userRequest,
          snapshotAccessRequest.getSourceSnapshotId(),
          snapshotAccessRequestResponse.getId());
    } catch (ApiException e) {
      snapshotRequestDao.delete(snapshotAccessRequestResponse.getId());
      throw e;
    }
    return snapshotAccessRequestResponse;
  }

  private <T> List<T> runSnapshotBuilderQuery(
      Query query,
      Snapshot snapshot,
      AuthenticatedUserRequest userRequest,
      Map<String, String> userEnteredData,
      BigQuerySnapshotPdao.Converter<T> bqConverter,
      AzureSynapsePdao.Converter<T> synapseConverter) {
    String sql = query.renderSQL(createContext(snapshot, userRequest));
    Instant start = Instant.now();
    List<T> result =
        CloudPlatformWrapper.of(snapshot.getCloudPlatform())
            .choose(
                () -> bigQuerySnapshotPdao.runQuery(sql, userEnteredData, snapshot, bqConverter),
                () -> azureSynapsePdao.runQuery(sql, userEnteredData, synapseConverter));
    logger.info(
        "{} seconds to run query \"{}\"", Duration.between(start, Instant.now()).toSeconds(), sql);
    return result;
  }

  public SnapshotBuilderConceptsResponse getConceptChildren(
      UUID snapshotId, int conceptId, AuthenticatedUserRequest userRequest) {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    SnapshotBuilderDomainOption domainOption = getDomainOption(conceptId, snapshot, userRequest);

    Query query =
        queryBuilderFactory
            .conceptChildrenQueryBuilder()
            .buildConceptChildrenQuery(domainOption, conceptId);

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            query,
            snapshot,
            userRequest,
            Map.of(),
            AggregateBQQueryResultsUtils::toConcept,
            AggregateSynapseQueryResultsUtils::toConcept);
    return new SnapshotBuilderConceptsResponse().result(concepts);
  }

  @VisibleForTesting
  SqlRenderContext createContext(Snapshot snapshot, AuthenticatedUserRequest userRequest) {
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(snapshot.getCloudPlatform());
    TableNameGenerator tableNameGenerator =
        CloudPlatformWrapper.of(snapshot.getCloudPlatform())
            .choose(
                () ->
                    BigQueryVisitor.bqSnapshotTableName(
                        snapshotService.retrieveSnapshotModel(snapshot.getId(), userRequest)),
                () ->
                    SynapseVisitor.azureTableName(
                        snapshotService.getOrCreateExternalAzureDataSource(snapshot, userRequest)));
    return new SqlRenderContext(tableNameGenerator, platform);
  }

  @VisibleForTesting
  SqlRenderContext createContext(Dataset dataset, AuthenticatedUserRequest userRequest) {
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    TableNameGenerator tableNameGenerator =
        platform.choose(
            () ->
                BigQueryVisitor.bqDatasetTableName(
                    datasetService.retrieveModel(dataset, userRequest)),
            () ->
                SynapseVisitor.azureTableName(
                    datasetService.getOrCreateExternalAzureDataSource(dataset, userRequest)));
    return new SqlRenderContext(tableNameGenerator, platform);
  }

  public SnapshotBuilderCountResponse getCountResponse(
      UUID id, List<SnapshotBuilderCohort> cohorts, AuthenticatedUserRequest userRequest) {
    int rollupCount = getRollupCountForCohorts(id, cohorts, userRequest);
    return new SnapshotBuilderCountResponse()
        .result(new SnapshotBuilderCountResponseResult().total(fuzzyLowCount(rollupCount)));
  }

  // if the rollup count is 0 OR >=20, then we will return the actual value
  // if the rollup count is between 1 and 19, we will return 19 and in the UI we will display <20
  // This helps reduce the risk of re-identification
  public static int fuzzyLowCount(int rollupCount) {
    return rollupCount == 0 ? rollupCount : Math.max(rollupCount, 19);
  }

  public EnumerateSnapshotAccessRequest enumerateRequests(Collection<UUID> authorizedResources) {
    return new EnumerateSnapshotAccessRequest()
        .items(snapshotRequestDao.enumerate(authorizedResources));
  }

  public SnapshotBuilderConceptsResponse enumerateConcepts(
      UUID snapshotId, int domainId, String searchText, AuthenticatedUserRequest userRequest) {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getBySnapshotId(snapshotId);

    SnapshotBuilderDomainOption snapshotBuilderDomainOption =
        snapshotBuilderSettings.getDomainOptions().stream()
            .filter(domainOption -> domainOption.getId() == domainId)
            .findFirst()
            .orElseThrow(
                () ->
                    new BadRequestException(
                        "Invalid domain category is given: %s".formatted(domainId)));

    Query query =
        queryBuilderFactory
            .searchConceptsQueryBuilder()
            .buildSearchConceptsQuery(
                snapshotBuilderDomainOption, !StringUtils.isEmpty(searchText));

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            query,
            snapshot,
            userRequest,
            Map.of(FILTER_TEXT, searchText),
            AggregateBQQueryResultsUtils::toConcept,
            AggregateSynapseQueryResultsUtils::toConcept);
    return new SnapshotBuilderConceptsResponse().result(concepts);
  }

  public int getRollupCountForCohorts(
      UUID snapshotId, List<SnapshotBuilderCohort> cohorts, AuthenticatedUserRequest userRequest) {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getBySnapshotId(snapshotId);

    Query query =
        queryBuilderFactory
            .criteriaQueryBuilder(snapshotBuilderSettings)
            .generateRollupCountsQueryForCohorts(cohorts);

    return runSnapshotBuilderQuery(
            query,
            snapshot,
            userRequest,
            Map.of(),
            AggregateBQQueryResultsUtils::toCount,
            AggregateSynapseQueryResultsUtils::toCount)
        .get(0);
  }

  private SnapshotBuilderDomainOption getDomainOptionFromSettingsByName(
      String domainId, UUID snapshotId) {
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getBySnapshotId(snapshotId);

    return snapshotBuilderSettings.getDomainOptions().stream()
        .filter(domainOption -> domainOption.getName().equals(domainId))
        .findFirst()
        .orElseThrow(
            () ->
                new BadRequestException(
                    "Invalid domain category is given: %s".formatted(domainId)));
  }

  private String getDomainId(
      int conceptId, Snapshot snapshot, AuthenticatedUserRequest userRequest) {
    List<String> domainIdResult =
        runSnapshotBuilderQuery(
            queryBuilderFactory.conceptChildrenQueryBuilder().retrieveDomainId(conceptId),
            snapshot,
            userRequest,
            Map.of(),
            AggregateBQQueryResultsUtils::toDomainId,
            AggregateSynapseQueryResultsUtils::toDomainId);
    if (domainIdResult.size() == 1) {
      return domainIdResult.get(0);
    } else if (domainIdResult.isEmpty()) {
      throw new IllegalStateException("No domain Id found for concept: " + conceptId);
    } else {
      throw new IllegalStateException("Multiple domains found for concept: " + conceptId);
    }
  }

  // This method is used to generate the SQL query to get the rowIds for the snapshot creation
  // process from a snapshot access request
  public String generateRowIdQuery(
      SnapshotAccessRequestResponse accessRequest,
      Snapshot snapshot,
      AuthenticatedUserRequest userReq) {

    SnapshotBuilderSettings settings = snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId());
    Dataset dataset = snapshot.getSourceDataset();

    List<SnapshotBuilderCohort> cohorts = accessRequest.getSnapshotSpecification().getCohorts();

    Query sqlQuery =
        queryBuilderFactory.criteriaQueryBuilder(settings).generateRowIdQueryForCohorts(cohorts);
    return sqlQuery.renderSQL(createContext(dataset, userReq));
  }

  record ParentQueryResult(
      int parentId, int childId, String childName, String code, int count, boolean hasChildren) {
    ParentQueryResult(ResultSet rs) throws SQLException {
      this(
          rs.getInt(QueryBuilderFactory.PARENT_ID),
          rs.getInt(Concept.CONCEPT_ID),
          rs.getString(Concept.CONCEPT_NAME),
          rs.getString(Concept.CONCEPT_CODE),
          rs.getInt(QueryBuilderFactory.COUNT),
          rs.getInt(QueryBuilderFactory.HAS_CHILDREN) > 0);
    }

    ParentQueryResult(FieldValueList row) {
      this(
          (int) row.get(QueryBuilderFactory.PARENT_ID).getLongValue(),
          (int) row.get(Concept.CONCEPT_ID).getLongValue(),
          row.get(Concept.CONCEPT_NAME).getStringValue(),
          row.get(Concept.CONCEPT_CODE).getStringValue(),
          (int) row.get(QueryBuilderFactory.COUNT).getLongValue(),
          row.get(QueryBuilderFactory.HAS_CHILDREN).getLongValue() > 0);
    }

    SnapshotBuilderConcept toConcept() {
      return new SnapshotBuilderConcept()
          .id(childId)
          .name(childName)
          .hasChildren(hasChildren)
          .code(code)
          .count(SnapshotBuilderService.fuzzyLowCount(count));
    }
  }

  public SnapshotBuilderGetConceptHierarchyResponse getConceptHierarchy(
      UUID snapshotId, int conceptId, AuthenticatedUserRequest userRequest) {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    var domainOption = getDomainOption(conceptId, snapshot, userRequest);
    var query = queryBuilderFactory.hierarchyQueryBuilder().generateQuery(domainOption, conceptId);

    Map<Integer, SnapshotBuilderParentConcept> parents = new HashMap<>();
    runSnapshotBuilderQuery(
            query, snapshot, userRequest, Map.of(), ParentQueryResult::new, ParentQueryResult::new)
        .forEach(
            row -> {
              SnapshotBuilderParentConcept parent =
                  parents.computeIfAbsent(
                      row.parentId,
                      k ->
                          new SnapshotBuilderParentConcept()
                              .parentId(k)
                              .children(new ArrayList<>()));
              parent.addChildrenItem(row.toConcept());
            });
    if (parents.isEmpty()) {
      throw new BadRequestException("No parents found for concept %s".formatted(conceptId));
    }

    return new SnapshotBuilderGetConceptHierarchyResponse().result(List.copyOf(parents.values()));
  }

  public SnapshotAccessRequestResponse rejectRequest(UUID id) {
    snapshotRequestDao.updateStatus(id, SnapshotAccessRequestStatus.REJECTED);
    return snapshotRequestDao.getById(id);
  }

  public SnapshotAccessRequestResponse approveRequest(UUID id) {
    snapshotRequestDao.updateStatus(id, SnapshotAccessRequestStatus.APPROVED);
    return snapshotRequestDao.getById(id);
  }

  private SnapshotBuilderDomainOption getDomainOption(
      int conceptId, Snapshot snapshot, AuthenticatedUserRequest userRequest) {
    // domain is needed to join with the domain specific occurrence table
    // this does not work for the metadata domain
    String domainId = getDomainId(conceptId, snapshot, userRequest);
    return getDomainOptionFromSettingsByName(domainId, snapshot.getId());
  }
}
