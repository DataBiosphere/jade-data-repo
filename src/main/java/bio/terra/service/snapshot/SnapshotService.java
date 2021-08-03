package bio.terra.service.snapshot;

import bio.terra.app.controller.SnapshotsApiController;
import bio.terra.app.controller.exception.ValidationException;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.grammar.Query;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestAccessIncludeModel;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.StorageResourceModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.flight.create.SnapshotCreateFlight;
import bio.terra.service.snapshot.flight.delete.SnapshotDeleteFlight;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SnapshotService {

  private final JobService jobService;
  private final DatasetService datasetService;
  private final FireStoreDependencyDao dependencyDao;
  private final BigQueryPdao bigQueryPdao;
  private final SnapshotDao snapshotDao;

  @Autowired
  public SnapshotService(
      JobService jobService,
      DatasetService datasetService,
      FireStoreDependencyDao dependencyDao,
      BigQueryPdao bigQueryPdao,
      SnapshotDao snapshotDao) {
    this.jobService = jobService;
    this.datasetService = datasetService;
    this.dependencyDao = dependencyDao;
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotDao = snapshotDao;
  }

  /**
   * Kick-off snapshot creation Pre-condition: the snapshot request has been syntax checked by the
   * validator
   *
   * @return jobId (flightId) of the job
   */
  public String createSnapshot(
      SnapshotRequestModel snapshotRequestModel, AuthenticatedUserRequest userReq) {
    String description = "Create snapshot " + snapshotRequestModel.getName();
    return jobService
        .newJob(description, SnapshotCreateFlight.class, snapshotRequestModel, userReq)
        .submit();
  }

  public void undoCreateSnapshot(String snapshotName) throws InterruptedException {
    // Remove any file dependencies created
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotName);
    for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
      Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
      dependencyDao.deleteSnapshotFileDependencies(dataset, snapshot.getId().toString());
    }

    bigQueryPdao.deleteSnapshot(snapshot);
  }

  /**
   * Kick-off snapshot deletion
   *
   * @param id snapshot id to delete
   * @return jobId (flightId) of the job
   */
  public String deleteSnapshot(UUID id, AuthenticatedUserRequest userReq) {
    String description = "Delete snapshot " + id;
    return jobService
        .newJob(description, SnapshotDeleteFlight.class, null, userReq)
        .addParameter(JobMapKeys.SNAPSHOT_ID.getKeyName(), id.toString())
        .submit();
  }

  /**
   * Enumerate a range of snapshots ordered by created date for consistent offset processing
   *
   * @return list of summary models of snapshot
   */
  public EnumerateSnapshotModel enumerateSnapshots(
      int offset,
      int limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      List<UUID> datasetIds,
      List<UUID> resources) {
    if (resources.isEmpty()) {
      return new EnumerateSnapshotModel().total(0).items(Collections.emptyList());
    }
    MetadataEnumeration<SnapshotSummary> enumeration =
        snapshotDao.retrieveSnapshots(
            offset, limit, sort, direction, filter, region, datasetIds, resources);
    List<SnapshotSummaryModel> models =
        enumeration.getItems().stream()
            .map(this::makeSummaryModelFromSummary)
            .collect(Collectors.toList());
    return new EnumerateSnapshotModel()
        .items(models)
        .total(enumeration.getTotal())
        .filteredTotal(enumeration.getFilteredTotal());
  }

  /**
   * Return a single snapshot summary given the snapshot id. This is used in the create snapshot
   * flight to build the model response of the asynchronous job.
   *
   * @return summary model of the snapshot
   */
  public SnapshotSummaryModel retrieveSnapshotSummary(UUID id) {
    SnapshotSummary snapshotSummary = snapshotDao.retrieveSummaryById(id);
    return makeSummaryModelFromSummary(snapshotSummary);
  }

  /**
   * Convenience wrapper around fetching an existing Snapshot object and converting it to a Model
   * object. Unlike the Snapshot object, the Model object includes a reference to the associated
   * cloud project.
   *
   * <p>Note that this method will only return a snapshot if it is NOT exclusively locked. It is
   * intended for user-facing calls (e.g. from RepositoryApiController), not internal calls that may
   * require an exclusively locked snapshot to be returned (e.g. snapshot deletion).
   *
   * @param id in UUID format
   * @return a SnapshotModel = API output-friendly representation of the Snapshot
   */
  public SnapshotModel retrieveAvailableSnapshotModel(UUID id) {
    return retrieveAvailableSnapshotModel(id, getDefaultIncludes());
  }

  /**
   * Convenience wrapper around fetching an existing Snapshot object and converting it to a Model
   * object.
   *
   * <p>Unlike the Snapshot object, the Model object includes a reference to the associated cloud
   * project.
   *
   * <p>Note that this method will only return a snapshot if it is NOT exclusively locked. It is
   * intended for user-facing calls (e.g. from RepositoryApiController), not internal calls that may
   * require an exclusively locked snapshot to be returned (e.g. snapshot deletion).
   *
   * @param id in UUID format
   * @param include a list of what information to include
   * @return an API output-friendly representation of the Snapshot
   */
  public SnapshotModel retrieveAvailableSnapshotModel(
      UUID id, List<SnapshotRequestAccessIncludeModel> include) {
    Snapshot snapshot = retrieveAvailable(id);
    return populateSnapshotModelFromSnapshot(snapshot, include);
  }

  /**
   * Fetch existing Snapshot object using the id.
   *
   * @param id in UUID format
   * @return a Snapshot object
   */
  public Snapshot retrieve(UUID id) {
    return snapshotDao.retrieveSnapshot(id);
  }

  /**
   * Fetch existing Snapshot object that is NOT exclusively locked.
   *
   * @param id in UUID format
   * @return a Snapshot object
   */
  public Snapshot retrieveAvailable(UUID id) {
    return snapshotDao.retrieveAvailableSnapshot(id);
  }

  /**
   * Fetch existing Snapshot object that is NOT exclusively locked.
   *
   * @param id in UUID format
   * @return a Snapshot object
   */
  public SnapshotProject retrieveAvailableSnapshotProject(UUID id) {
    return snapshotDao.retrieveAvailableSnapshotProject(id);
  }

  /**
   * Fetch existing Snapshot object using the name.
   *
   * @return a Snapshot object
   */
  public Snapshot retrieveByName(String name) {
    return snapshotDao.retrieveSnapshotByName(name);
  }

  /**
   * Make a Snapshot structure with all of its parts from an incoming snapshot request. Note that
   * the structure does not have UUIDs or created dates filled in. Those are updated by the DAO when
   * it stores the snapshot in the repository metadata.
   *
   * @return Snapshot
   */
  public Snapshot makeSnapshotFromSnapshotRequest(SnapshotRequestModel snapshotRequestModel) {
    // Make this early so we can hook up back links to it
    Snapshot snapshot = new Snapshot();
    List<SnapshotRequestContentsModel> requestContentsList = snapshotRequestModel.getContents();
    // TODO: for MVM we only allow one source list
    if (requestContentsList.size() > 1) {
      throw new ValidationException("Only a single snapshot contents entry is currently allowed.");
    }

    SnapshotRequestContentsModel requestContents = requestContentsList.get(0);
    Dataset dataset = datasetService.retrieveByName(requestContents.getDatasetName());
    SnapshotSource snapshotSource = new SnapshotSource().snapshot(snapshot).dataset(dataset);
    switch (snapshotRequestModel.getContents().get(0).getMode()) {
      case BYASSET:
        // TODO: When we implement explicit definition of snapshot tables, we will handle that here.
        // For now, we generate the snapshot tables directly from the asset tables of the one source
        // allowed in a snapshot.
        AssetSpecification assetSpecification = getAssetSpecificationFromRequest(requestContents);
        snapshotSource.assetSpecification(assetSpecification);
        conjureSnapshotTablesFromAsset(
            snapshotSource.getAssetSpecification(), snapshot, snapshotSource);
        break;
      case BYFULLVIEW:
        conjureSnapshotTablesFromDatasetTables(snapshot, snapshotSource);
        break;
      case BYQUERY:
        SnapshotRequestQueryModel queryModel = requestContents.getQuerySpec();
        String assetName = queryModel.getAssetName();
        String snapshotQuery = queryModel.getQuery();
        Query query = Query.parse(snapshotQuery);
        List<String> datasetNames = query.getDatasetNames();
        // TODO this makes the assumption that there is only one dataset
        // (based on the validation flight step that already occurred.)
        // This will change when more than 1 dataset is allowed
        String datasetName = datasetNames.get(0);
        Dataset queryDataset = datasetService.retrieveByName(datasetName);
        AssetSpecification queryAssetSpecification =
            queryDataset
                .getAssetSpecificationByName(assetName)
                .orElseThrow(
                    () ->
                        new AssetNotFoundException(
                            "This dataset does not have an asset specification with name: "
                                + assetName));
        snapshotSource.assetSpecification(queryAssetSpecification);
        // TODO this is wrong? why dont we just pass the assetSpecification?
        conjureSnapshotTablesFromAsset(
            snapshotSource.getAssetSpecification(), snapshot, snapshotSource);
        break;
      case BYROWID:
        SnapshotRequestRowIdModel requestRowIdModel = requestContents.getRowIdSpec();
        conjureSnapshotTablesFromRowIds(requestRowIdModel, snapshot, snapshotSource);
        break;
      default:
        throw new InvalidSnapshotException("Snapshot does not have required mode information");
    }

    return snapshot
        .name(snapshotRequestModel.getName())
        .description(snapshotRequestModel.getDescription())
        .snapshotSources(Collections.singletonList(snapshotSource))
        .profileId(snapshotRequestModel.getProfileId())
        .relationships(createSnapshotRelationships(dataset.getRelationships(), snapshotSource));
  }

  public List<UUID> getSourceDatasetIdsFromSnapshotRequest(
      SnapshotRequestModel snapshotRequestModel) {
    return getSourceDatasetsFromSnapshotRequest(snapshotRequestModel).stream()
        .map(Dataset::getId)
        .collect(Collectors.toList());
  }

  public List<Dataset> getSourceDatasetsFromSnapshotRequest(
      SnapshotRequestModel snapshotRequestModel) {
    return snapshotRequestModel.getContents().stream()
        .map(c -> datasetService.retrieveByName(c.getDatasetName()))
        .collect(Collectors.toList());
  }

  public List<UUID> getSourceDatasetIdsFromSnapshotId(UUID snapshotId) {
    SnapshotModel snapshotModel = retrieveAvailableSnapshotModel(snapshotId);
    return snapshotModel.getSource().stream()
        .map(s -> s.getDataset().getId())
        .collect(Collectors.toList());
  }

  private AssetSpecification getAssetSpecificationFromRequest(
      SnapshotRequestContentsModel requestContents) {
    SnapshotRequestAssetModel requestAssetModel = requestContents.getAssetSpec();
    Dataset dataset = datasetService.retrieveByName(requestContents.getDatasetName());

    Optional<AssetSpecification> optAsset =
        dataset.getAssetSpecificationByName(requestAssetModel.getAssetName());
    if (!optAsset.isPresent()) {
      throw new AssetNotFoundException(
          "Asset specification not found: " + requestAssetModel.getAssetName());
    }

    // the map construction will go here. For MVM, we generate the mapping data directly from the
    // asset spec.
    return optAsset.get();
  }

  /**
   * Magic up the snapshot tables and snapshot map from the asset tables and columns. This method
   * sets the table lists into snapshot and snapshotSource.
   *
   * @param asset the one and only asset specification for this snapshot
   * @param snapshot snapshot to point back to and fill in
   * @param snapshotSource snapshotSource to point back to and fill in
   */
  private void conjureSnapshotTablesFromAsset(
      AssetSpecification asset, Snapshot snapshot, SnapshotSource snapshotSource) {

    List<SnapshotTable> tableList = new ArrayList<>();
    List<SnapshotMapTable> mapTableList = new ArrayList<>();

    for (AssetTable assetTable : asset.getAssetTables()) {
      // Create early so we can hook up back pointers.
      SnapshotTable table = new SnapshotTable();

      // Build the column lists in parallel, so we can easily connect the
      // map column to the snapshot column.
      List<Column> columnList = new ArrayList<>();
      List<SnapshotMapColumn> mapColumnList = new ArrayList<>();

      for (AssetColumn assetColumn : assetTable.getColumns()) {
        Column column = new Column(assetColumn.getDatasetColumn());
        columnList.add(column);

        mapColumnList.add(
            new SnapshotMapColumn().fromColumn(assetColumn.getDatasetColumn()).toColumn(column));
      }

      table.name(assetTable.getTable().getName()).columns(columnList);
      tableList.add(table);
      mapTableList.add(
          new SnapshotMapTable()
              .fromTable(assetTable.getTable())
              .toTable(table)
              .snapshotMapColumns(mapColumnList));
    }

    snapshotSource.snapshotMapTables(mapTableList);
    snapshot.snapshotTables(tableList);
  }

  /**
   * Map from a list of source relationships (from a dataset or asset) into snapshot relationships.
   *
   * @param sourceRelationships relationships from a dataset or asset
   * @param snapshotSource source with mapping between dataset tables and columns -> snapshot tables
   *     and columns
   * @return a list of relationships tied to the snapshot tables
   */
  public List<Relationship> createSnapshotRelationships(
      List<Relationship> sourceRelationships, SnapshotSource snapshotSource) {
    // We'll copy the asset relationships into the snapshot.
    List<Relationship> snapshotRelationships = new ArrayList<>();

    // Create lookups from dataset table and column ids -> snapshot tables and columns, respectively
    Map<UUID, Table> tableLookup = new HashMap<>();
    Map<UUID, Column> columnLookup = new HashMap<>();

    for (SnapshotMapTable mapTable : snapshotSource.getSnapshotMapTables()) {
      tableLookup.put(mapTable.getFromTable().getId(), mapTable.getToTable());

      for (SnapshotMapColumn mapColumn : mapTable.getSnapshotMapColumns()) {
        columnLookup.put(mapColumn.getFromColumn().getId(), mapColumn.getToColumn());
      }
    }

    for (Relationship sourceRelationship : sourceRelationships) {
      UUID fromTableId = sourceRelationship.getFromTable().getId();
      UUID fromColumnId = sourceRelationship.getFromColumn().getId();
      UUID toTableId = sourceRelationship.getToTable().getId();
      UUID toColumnId = sourceRelationship.getToColumn().getId();

      if (tableLookup.containsKey(fromTableId)
          && tableLookup.containsKey(toTableId)
          && columnLookup.containsKey(fromColumnId)
          && columnLookup.containsKey(toColumnId)) {
        Table fromTable = tableLookup.get(fromTableId);
        Column fromColumn = columnLookup.get(fromColumnId);
        Table toTable = tableLookup.get(toTableId);
        Column toColumn = columnLookup.get(toColumnId);
        snapshotRelationships.add(
            new Relationship()
                .name(sourceRelationship.getName())
                .fromTable(fromTable)
                .fromColumn(fromColumn)
                .toTable(toTable)
                .toColumn(toColumn));
      }
    }
    return snapshotRelationships;
  }

  private void conjureSnapshotTablesFromRowIds(
      SnapshotRequestRowIdModel requestRowIdModel,
      Snapshot snapshot,
      SnapshotSource snapshotSource) {
    // TODO this will need to be changed when we have more than one dataset per snapshot (>1
    // contentsModel)
    List<SnapshotTable> tableList = new ArrayList<>();
    snapshot.snapshotTables(tableList);
    List<SnapshotMapTable> mapTableList = new ArrayList<>();
    snapshotSource.snapshotMapTables(mapTableList);
    Dataset dataset = snapshotSource.getDataset();

    // create a lookup from tableName -> table spec from the request
    Map<String, SnapshotRequestRowIdTableModel> requestTableLookup =
        requestRowIdModel.getTables().stream()
            .collect(
                Collectors.toMap(
                    SnapshotRequestRowIdTableModel::getTableName, Function.identity()));

    // for each dataset table specified in the request, create a table in the snapshot with the same
    // name
    for (DatasetTable datasetTable : dataset.getTables()) {
      if (!requestTableLookup.containsKey(datasetTable.getName())) {
        continue; // only capture the dataset tables in the request model
      }
      List<Column> columnList = new ArrayList<>();
      SnapshotTable snapshotTable =
          new SnapshotTable().name(datasetTable.getName()).columns(columnList);
      tableList.add(snapshotTable);
      List<SnapshotMapColumn> mapColumnList = new ArrayList<>();
      mapTableList.add(
          new SnapshotMapTable()
              .fromTable(datasetTable)
              .toTable(snapshotTable)
              .snapshotMapColumns(mapColumnList));

      // for each dataset column specified in the request, create a column in the snapshot w the
      // same name & array
      Set<String> requestColumns =
          new HashSet<>(requestTableLookup.get(datasetTable.getName()).getColumns());
      datasetTable.getColumns().stream()
          .filter(c -> requestColumns.contains(c.getName()))
          .forEach(
              datasetColumn -> {
                Column snapshotColumn = Column.toSnapshotColumn(datasetColumn);
                SnapshotMapColumn snapshotMapColumn =
                    new SnapshotMapColumn().fromColumn(datasetColumn).toColumn(snapshotColumn);
                columnList.add(snapshotColumn);
                mapColumnList.add(snapshotMapColumn);
              });
    }
  }

  private void conjureSnapshotTablesFromDatasetTables(
      Snapshot snapshot, SnapshotSource snapshotSource) {
    // TODO this will need to be changed when we have more than one dataset per snapshot (>1
    // contentsModel)
    // for each dataset table specified in the request, create a table in the snapshot with the same
    // name
    Dataset dataset = snapshotSource.getDataset();
    List<SnapshotTable> tableList = new ArrayList<>();
    List<SnapshotMapTable> mapTableList = new ArrayList<>();

    for (DatasetTable datasetTable : dataset.getTables()) {
      List<Column> columnList = new ArrayList<>();
      List<SnapshotMapColumn> mapColumnList = new ArrayList<>();

      // for each dataset column specified in the request, create a column in the snapshot w the
      // same name & array
      for (Column datasetColumn : datasetTable.getColumns()) {
        Column snapshotColumn = Column.toSnapshotColumn(datasetColumn);
        SnapshotMapColumn snapshotMapColumn =
            new SnapshotMapColumn().fromColumn(datasetColumn).toColumn(snapshotColumn);
        columnList.add(snapshotColumn);
        mapColumnList.add(snapshotMapColumn);
      }

      // create snapshot tables & mapping with the proper dataset name and columns
      SnapshotTable snapshotTable =
          new SnapshotTable().name(datasetTable.getName()).columns(columnList);
      tableList.add(snapshotTable);

      mapTableList.add(
          new SnapshotMapTable()
              .fromTable(datasetTable)
              .toTable(snapshotTable)
              .snapshotMapColumns(mapColumnList));
    }
    // set the snapshot tables and mapping
    snapshot.snapshotTables(tableList);
    snapshotSource.snapshotMapTables(mapTableList);
  }

  public SnapshotSummaryModel makeSummaryModelFromSummary(SnapshotSummary snapshotSummary) {
    SnapshotSummaryModel summaryModel =
        new SnapshotSummaryModel()
            .id(snapshotSummary.getId())
            .name(snapshotSummary.getName())
            .description(snapshotSummary.getDescription())
            .createdDate(snapshotSummary.getCreatedDate().toString())
            .profileId(snapshotSummary.getProfileId())
            .storage(storageResourceModelFromSnapshotSummary(snapshotSummary));
    return summaryModel;
  }

  private static List<StorageResourceModel> storageResourceModelFromSnapshotSummary(
      SnapshotSummary snapshotSummary) {
    return snapshotSummary.getStorage().stream()
        .map(StorageResource::toModel)
        .collect(Collectors.toList());
  }

  private SnapshotModel populateSnapshotModelFromSnapshot(
      Snapshot snapshot, List<SnapshotRequestAccessIncludeModel> include) {
    SnapshotModel snapshotModel =
        new SnapshotModel()
            .id(snapshot.getId())
            .name(snapshot.getName())
            .description(snapshot.getDescription())
            .createdDate(snapshot.getCreatedDate().toString());

    // In case NONE is specified, this should supersede any other value being passed in
    if (include.contains(SnapshotRequestAccessIncludeModel.NONE)) {
      return snapshotModel;
    }

    if (include.contains(SnapshotRequestAccessIncludeModel.SOURCES)) {
      snapshotModel.source(
          snapshot.getSnapshotSources().stream()
              .map(this::makeSourceModelFromSource)
              .collect(Collectors.toList()));
    }
    if (include.contains(SnapshotRequestAccessIncludeModel.TABLES)) {
      snapshotModel.tables(
          snapshot.getTables().stream()
              .map(this::makeTableModelFromTable)
              .collect(Collectors.toList()));
    }
    if (include.contains(SnapshotRequestAccessIncludeModel.RELATIONSHIPS)) {
      snapshotModel.relationships(
          snapshot.getRelationships().stream()
              .map(this::makeRelationshipModelFromRelationship)
              .collect(Collectors.toList()));
    }
    if (include.contains(SnapshotRequestAccessIncludeModel.PROFILE)) {
      snapshotModel.profileId(snapshot.getProfileId());
    }
    if (include.contains(SnapshotRequestAccessIncludeModel.DATA_PROJECT)) {
      snapshotModel.dataProject(snapshot.getProjectResource().getGoogleProjectId());
    }
    if (include.contains(SnapshotRequestAccessIncludeModel.ACCESS_INFORMATION)) {
      snapshotModel.accessInformation(MetadataDataAccessUtils.accessInfoFromSnapshot(snapshot));
    }
    return snapshotModel;
  }

  private RelationshipModel makeRelationshipModelFromRelationship(Relationship relationship) {
    RelationshipTermModel fromModel =
        new RelationshipTermModel()
            .table(relationship.getFromTable().getName())
            .column(relationship.getFromColumn().getName());
    RelationshipTermModel toModel =
        new RelationshipTermModel()
            .table(relationship.getToTable().getName())
            .column(relationship.getToColumn().getName());
    return new RelationshipModel().name(relationship.getName()).from(fromModel).to(toModel);
  }

  private SnapshotSourceModel makeSourceModelFromSource(SnapshotSource source) {
    // TODO: when source summary methods are available, use those. Here I roll my own
    Dataset dataset = source.getDataset();
    DatasetSummaryModel summaryModel =
        new DatasetSummaryModel()
            .id(dataset.getId())
            .name(dataset.getName())
            .description(dataset.getDescription())
            .defaultProfileId(dataset.getDefaultProfileId())
            .createdDate(dataset.getCreatedDate().toString())
            .storage(
                dataset.getDatasetSummary().getStorage().stream()
                    .map(StorageResource::toModel)
                    .collect(Collectors.toList()));

    SnapshotSourceModel sourceModel = new SnapshotSourceModel().dataset(summaryModel);

    AssetSpecification assetSpec = source.getAssetSpecification();
    if (assetSpec != null) {
      sourceModel.asset(assetSpec.getName());
    }

    return sourceModel;
  }

  // TODO: share these methods with dataset table in some common place
  private TableModel makeTableModelFromTable(Table table) {
    Long rowCount = table.getRowCount();
    return new TableModel()
        .name(table.getName())
        .rowCount(rowCount != null ? rowCount.intValue() : null)
        .columns(
            table.getColumns().stream()
                .map(this::makeColumnModelFromColumn)
                .collect(Collectors.toList()));
  }

  private ColumnModel makeColumnModelFromColumn(Column column) {
    return new ColumnModel()
        .name(column.getName())
        .datatype(column.getType())
        .arrayOf(column.isArrayOf());
  }

  private static List<SnapshotRequestAccessIncludeModel> getDefaultIncludes() {
    return Arrays.stream(
            StringUtils.split(SnapshotsApiController.RETRIEVE_INCLUDE_DEFAULT_VALUE, ','))
        .map(SnapshotRequestAccessIncludeModel::fromValue)
        .collect(Collectors.toList());
  }
}
