package bio.terra.service.snapshotbuilder;

import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ValidationUtils;
import bio.terra.common.exception.ApiException;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestDetailsResponse;
import bio.terra.model.SnapshotAccessRequestMembersResponse;
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
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.notification.NotificationService;
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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
  private final NotificationService notificationService;
  private final TerraConfiguration terraConfiguration;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao,
      DatasetService datasetService,
      IamService iamService,
      SnapshotService snapshotService,
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      NotificationService notificationService,
      AzureSynapsePdao azureSynapsePdao,
      QueryBuilderFactory queryBuilderFactory,
      TerraConfiguration terraConfiguration) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
    this.datasetService = datasetService;
    this.iamService = iamService;
    this.snapshotService = snapshotService;
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.notificationService = notificationService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.queryBuilderFactory = queryBuilderFactory;
    this.terraConfiguration = terraConfiguration;
  }

  public SnapshotAccessRequestResponse createRequest(
      AuthenticatedUserRequest userRequest, SnapshotAccessRequest snapshotAccessRequest) {
    SnapshotAccessRequestModel snapshotAccessRequestModel =
        snapshotRequestDao.create(snapshotAccessRequest, userRequest.getEmail());
    try {
      iamService.createSnapshotBuilderRequestResource(
          userRequest,
          snapshotAccessRequest.getSourceSnapshotId(),
          snapshotAccessRequestModel.id());
    } catch (ApiException e) {
      snapshotRequestDao.delete(snapshotAccessRequestModel.id());
      throw e;
    }

    return snapshotAccessRequestModel.toApiResponse();
  }

  private <T> List<T> runSnapshotBuilderQuery(
      Query query,
      Snapshot snapshot,
      AuthenticatedUserRequest userRequest,
      BigQuerySnapshotPdao.Converter<T> bqConverter,
      AzureSynapsePdao.Converter<T> synapseConverter) {
    return runSnapshotBuilderQuery(
        query, snapshot, userRequest, Map.of(), bqConverter, synapseConverter);
  }

  private <T> List<T> runSnapshotBuilderQuery(
      Query query,
      Snapshot snapshot,
      AuthenticatedUserRequest userRequest,
      Map<String, String> paramMap,
      BigQuerySnapshotPdao.Converter<T> bqConverter,
      AzureSynapsePdao.Converter<T> synapseConverter) {
    String sql = query.renderSQL(createContext(snapshot, userRequest));
    Instant start = Instant.now();
    List<T> result =
        CloudPlatformWrapper.of(snapshot.getCloudPlatform())
            .choose(
                () -> bigQuerySnapshotPdao.runQuery(sql, paramMap, snapshot, bqConverter),
                () -> azureSynapsePdao.runQuery(sql, paramMap, synapseConverter));
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
    List<SnapshotAccessRequestModel> accessRequestModels =
        snapshotRequestDao.enumerate(authorizedResources);
    return generateResponseFromRequestModels(accessRequestModels);
  }

  public EnumerateSnapshotAccessRequest enumerateRequestsBySnapshot(UUID snapshotId) {
    return generateResponseFromRequestModels(snapshotRequestDao.enumerateBySnapshot(snapshotId));
  }

  private EnumerateSnapshotAccessRequest generateResponseFromRequestModels(
      List<SnapshotAccessRequestModel> models) {
    return new EnumerateSnapshotAccessRequest()
        .items(models.stream().map(SnapshotAccessRequestModel::toApiResponse).toList());
  }

  public SnapshotAccessRequestResponse getRequest(UUID id) {
    return snapshotRequestDao.getById(id).toApiResponse();
  }

  public SnapshotAccessRequestDetailsResponse getRequestDetails(
      AuthenticatedUserRequest userRequest, UUID id) {
    return generateModelDetails(userRequest, snapshotRequestDao.getById(id));
  }

  public void deleteRequest(AuthenticatedUserRequest user, UUID id) {
    iamService.deleteSnapshotBuilderRequest(user, id);
    snapshotRequestDao.updateStatus(id, SnapshotAccessRequestStatus.DELETED);
  }

  public SnapshotBuilderConceptsResponse enumerateConcepts(
      UUID snapshotId, int domainId, String filterText, AuthenticatedUserRequest userRequest) {
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
            .enumerateConceptsQueryBuilder()
            .buildEnumerateConceptsQuery(
                snapshotBuilderDomainOption, filterText != null && !filterText.isEmpty());

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            query,
            snapshot,
            userRequest,
            Map.of(QueryBuilderFactory.FILTER_TEXT, filterText),
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
      SnapshotAccessRequestModel accessRequest,
      Snapshot snapshot,
      AuthenticatedUserRequest userReq) {

    SnapshotBuilderSettings settings = snapshotBuilderSettingsDao.getBySnapshotId(snapshot.getId());
    Dataset dataset = snapshot.getSourceDataset();

    List<SnapshotBuilderCohort> cohorts = accessRequest.snapshotSpecification().getCohorts();

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
            query, snapshot, userRequest, ParentQueryResult::new, ParentQueryResult::new)
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
    return snapshotRequestDao.getById(id).toApiResponse();
  }

  public SnapshotAccessRequestResponse approveRequest(UUID id) {
    snapshotRequestDao.updateStatus(id, SnapshotAccessRequestStatus.APPROVED);
    return snapshotRequestDao.getById(id).toApiResponse();
  }

  public SnapshotAccessRequestMembersResponse getGroupMembers(UUID id) {
    SnapshotAccessRequestModel model = snapshotRequestDao.getById(id);
    ValidationUtils.requireNotBlank(
        model.samGroupName(),
        "Snapshot must be created from this request in order to manage group membership.");
    return new SnapshotAccessRequestMembersResponse()
        .members(iamService.getGroupPolicyEmails(model.samGroupName(), IamRole.MEMBER.toString()));
  }

  static void validateGroupParams(SnapshotAccessRequestModel model, String memberEmail) {
    if (memberEmail != null) {
      ValidationUtils.requireValidEmail(memberEmail, "Invalid member email");
    }
    ValidationUtils.requireNotBlank(
        model.samGroupName(),
        "Snapshot must be created from this request in order to manage group membership.");
  }

  public SnapshotAccessRequestMembersResponse addGroupMember(UUID id, String memberEmail) {
    SnapshotAccessRequestModel model = snapshotRequestDao.getById(id);
    validateGroupParams(model, memberEmail);
    return new SnapshotAccessRequestMembersResponse()
        .members(
            iamService.addEmailToGroup(
                model.samGroupName(), IamRole.MEMBER.toString(), memberEmail));
  }

  public SnapshotAccessRequestMembersResponse deleteGroupMember(UUID id, String memberEmail) {
    SnapshotAccessRequestModel model = snapshotRequestDao.getById(id);
    validateGroupParams(model, memberEmail);
    return new SnapshotAccessRequestMembersResponse()
        .members(
            iamService.removeEmailFromGroup(
                model.samGroupName(), IamRole.MEMBER.toString(), memberEmail));
  }

  private SnapshotAccessRequestDetailsResponse generateModelDetails(
      AuthenticatedUserRequest userRequest, SnapshotAccessRequestModel model) {
    List<Integer> conceptIds = model.generateConceptIds();
    SnapshotBuilderSettings settings =
        snapshotBuilderSettingsDao.getBySnapshotId(model.sourceSnapshotId());
    Map<Integer, String> concepts =
        conceptIds.isEmpty()
            ? Map.of()
            : runSnapshotBuilderQuery(
                    queryBuilderFactory
                        .enumerateConceptsQueryBuilder()
                        .getConceptsFromConceptIds(conceptIds),
                    snapshotService.retrieve(model.sourceSnapshotId()),
                    userRequest,
                    AggregateBQQueryResultsUtils::toConceptIdNamePair,
                    AggregateSynapseQueryResultsUtils::toConceptIdNamePair)
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return model.generateModelDetails(settings, concepts);
  }

  private SnapshotBuilderDomainOption getDomainOption(
      int conceptId, Snapshot snapshot, AuthenticatedUserRequest userRequest) {
    // domain is needed to join with the domain specific occurrence table
    // this does not work for the metadata domain
    String domainId = getDomainId(conceptId, snapshot, userRequest);
    return getDomainOptionFromSettingsByName(domainId, snapshot.getId());
  }

  public String createExportSnapshotLink(UUID snapshotId) {
    return "%s/import-data?snapshotId=%s&format=tdrexport&tdrSyncPermissions=false"
        .formatted(terraConfiguration.basePath(), snapshotId);
  }

  public void notifySnapshotReady(
      AuthenticatedUserRequest userRequest, String subjectId, UUID snapshotRequestId) {
    var snapshotAccessRequest = snapshotRequestDao.getById(snapshotRequestId);
    UUID snapshotId = snapshotAccessRequest.createdSnapshotId();
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    notificationService.snapshotReady(
        subjectId,
        createExportSnapshotLink(snapshotId),
        snapshot.getName(),
        generateModelDetails(userRequest, snapshotAccessRequest).getSummary());
  }
}
