package bio.terra.service.snapshot;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.SnapshotProvidedIdsRequestContentsModel;
import bio.terra.model.SnapshotProvidedIdsRequestModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.snapshot.flight.create.SnapshotCreateFlight;
import bio.terra.service.snapshot.flight.create.SnapshotCreateWithProvidedIdsFlight;
import bio.terra.service.snapshot.flight.delete.SnapshotDeleteFlight;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Table;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestSourceModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class SnapshotService {
    private final JobService jobService;
    private final DatasetDao datasetDao;
    private final SnapshotDao snapshotDao;
    private final DataLocationService dataLocationService;

    @Autowired
    public SnapshotService(JobService jobService,
                           DatasetDao datasetDao,
                           SnapshotDao snapshotDao,
                           DataLocationService dataLocationService) {
        this.jobService = jobService;
        this.datasetDao = datasetDao;
        this.snapshotDao = snapshotDao;
        this.dataLocationService = dataLocationService;
    }

    /**
     * Kick-off snapshot creation
     * Pre-condition: the snapshot request has been syntax checked by the validator
     *
     * @param snapshotRequestModel
     * @returns jobId (flightId) of the job
     */
    public String createSnapshot(SnapshotRequestModel snapshotRequestModel, AuthenticatedUserRequest userReq) {
        String description = "Create snapshot " + snapshotRequestModel.getName();
        return jobService
            .newJob(description, SnapshotCreateFlight.class, snapshotRequestModel, userReq)
            .submit();
    }

    public String createSnapshotWithProvidedIds(
        SnapshotProvidedIdsRequestModel spiRequestModel,
        AuthenticatedUserRequest userReq) {
        String description = "Create snapshot using provided ids:" + spiRequestModel.getName();
        return jobService
            .newJob(description, SnapshotCreateWithProvidedIdsFlight.class, spiRequestModel, userReq)
            .submit();
    }

    /**
     * Kick-off snapshot deletion
     *
     * @param id snapshot id to delete
     * @returns jobId (flightId) of the job
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
     * @param offset
     * @param limit
     * @return list of summary models of snapshot
     */
    public EnumerateSnapshotModel enumerateSnapshots(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter,
        List<UUID> resources) {
        if (resources.isEmpty()) {
            return new EnumerateSnapshotModel().total(0);
        }
        MetadataEnumeration<SnapshotSummary> enumeration = snapshotDao.retrieveSnapshots(offset, limit, sort, direction,
            filter, resources);
        List<SnapshotSummaryModel> models = enumeration.getItems()
                .stream()
                .map(this::makeSummaryModelFromSummary)
                .collect(Collectors.toList());
        return new EnumerateSnapshotModel().items(models).total(enumeration.getTotal());
    }

    /**
     * Return a single snapshot summary given the snapshot id.
     * This is used in the create snapshot flight to build the model response
     * of the asynchronous job.
     *
     * @param id
     * @return summary model of the snapshot
     */
    public SnapshotSummaryModel retrieveSnapshotSummary(UUID id) {
        SnapshotSummary snapshotSummary = snapshotDao.retrieveSnapshotSummary(id);
        return makeSummaryModelFromSummary(snapshotSummary);
    }

    /** Convenience wrapper around fetching an existing Snapshot object and converting it to a Model object.
     * Unlike the Snapshot object, the Model object includes a reference to the associated cloud project.
     * @param id in UUID formant
     * @return a SnapshotModel = API output-friendly representation of the Snapshot
     */
    public SnapshotModel retrieveModel(UUID id) {
        Snapshot snapshot = retrieve(id);
        SnapshotDataProject dataProject = dataLocationService.getProjectOrThrow(snapshot);
        return populateSnapshotModelFromSnapshot(snapshot).dataProject(dataProject.getGoogleProjectId());
    }

    /** Fetch existing Snapshot object using the id.
     * @param id in UUID format
     * @return a Snapshot object
     */
    public Snapshot retrieve(UUID id) {
        return snapshotDao.retrieveSnapshot(id);
    }

    /** Fetch existing Snapshot object using the name.
     * @param name
     * @return a Snapshot object
     */
    public Snapshot retrieveByName(String name) {
        return snapshotDao.retrieveSnapshotByName(name);
    }

    /**
     * Make a Snapshot structure with all of its parts from an incoming snapshot request.
     * Note that the structure does not have UUIDs or created dates filled in. Those are
     * updated by the DAO when it stores the snapshot in the repository metadata.
     *
     * @param snapshotRequestModel
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
        SnapshotSource snapshotSource = makeSourceFromRequestContents(requestContents, snapshot);

        // TODO: When we implement explicit definition of snapshot tables, we will handle that here.
        // For now, we generate the snapshot tables directly from the asset tables of the one source
        // allowed in a snapshot.
        conjureSnapshotTablesFromAsset(snapshotSource.getAssetSpecification(), snapshot, snapshotSource);
        snapshot.name(snapshotRequestModel.getName())
                .description(snapshotRequestModel.getDescription())
                .snapshotSources(Collections.singletonList(snapshotSource))
                .profileId(UUID.fromString(snapshotRequestModel.getProfileId()));

        return snapshot;
    }

    public Snapshot makeSnapshotFromSnapshotProvidedIdsRequest(SnapshotProvidedIdsRequestModel spiRequestModel) {
        Snapshot snapshot = new Snapshot();
        if (spiRequestModel.getContents().size() > 1) {
            throw new ValidationException("Only a single snapshot contents entry is currently allowed.");
        }
        SnapshotSource source = makeSourceFromProvidedIdsContentsModel(spiRequestModel.getContents().get(0), snapshot);
        List<SnapshotTable> tableList = new ArrayList<>();
        List<SnapshotMapTable> mapTableList = new ArrayList<>();
        Dataset dataset = source.getDataset();
        for (DatasetTable datasetTable : dataset.getTables()) {
            SnapshotTable snapshotTable = new SnapshotTable();
            List<Column> columnList = new ArrayList<>();
            List<SnapshotMapColumn> mapColumnList = new ArrayList<>();

            for (Column datasetColumn : datasetTable.getColumns()) {
                columnList.add(datasetColumn);
                mapColumnList.add(new SnapshotMapColumn()
                    .fromColumn(datasetColumn)
                    .toColumn(datasetColumn));
            }

            snapshotTable
                .name(datasetTable.getName())
                .columns(columnList);
            tableList.add(snapshotTable);
            mapTableList.add(new SnapshotMapTable()
                .fromTable(datasetTable)
                .toTable(snapshotTable)
                .snapshotMapColumns(mapColumnList));
        }

        source.snapshotMapTables(mapTableList);
        return snapshot
            .snapshotTables(tableList)
            .name(spiRequestModel.getName())
            .description(spiRequestModel.getDescription())
            .snapshotSources(Collections.singletonList(source))
            .profileId(UUID.fromString(spiRequestModel.getProfileId()));
    }

    private SnapshotSource makeSourceFromProvidedIdsContentsModel(
        SnapshotProvidedIdsRequestContentsModel spiRequestContentsModel,
        Snapshot snapshot) {
        Dataset dataset = datasetDao.retrieveByName(spiRequestContentsModel.getDatasetName());
        // we are leaving the asset spec null since this is intended for when we do not want to walk the relationships
        return new SnapshotSource()
            .snapshot(snapshot)
            .dataset(dataset);
    }

    private SnapshotSource makeSourceFromRequestContents(
        SnapshotRequestContentsModel requestContents,
        Snapshot snapshot) {
        SnapshotRequestSourceModel requestSource = requestContents.getSource();
        Dataset dataset = datasetDao.retrieveByName(requestSource.getDatasetName());

        Optional<AssetSpecification> optAsset = dataset.getAssetSpecificationByName(requestSource.getAssetName());
        if (!optAsset.isPresent()) {
            throw new AssetNotFoundException("Asset specification not found: " + requestSource.getAssetName());
        }

        // TODO: When we implement explicit definition of the snapshot tables and mapping to dataset tables,
        // the map construction will go here. For MVM, we generate the mapping data directly from the asset spec.

        return new SnapshotSource()
               .snapshot(snapshot)
               .dataset(dataset)
               .assetSpecification(optAsset.get());
    }

    /**
     * Magic up the snapshot tables and snapshot map from the asset tables and columns.
     * This method sets the table lists into snapshot and snapshotSource.
     *
     * @param asset  the one and only asset specification for this snapshot
     * @param snapshot snapshot to point back to and fill in
     * @param snapshotSource snapshotSource to point back to and fill in
     */
    private void conjureSnapshotTablesFromAsset(AssetSpecification asset,
                                               Snapshot snapshot,
                                               SnapshotSource snapshotSource) {

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

                mapColumnList.add(new SnapshotMapColumn()
                    .fromColumn(assetColumn.getDatasetColumn())
                    .toColumn(column));
            }

            table.name(assetTable.getTable().getName())
                    .columns(columnList);
            tableList.add(table);
            mapTableList.add(new SnapshotMapTable()
                    .fromTable(assetTable.getTable())
                    .toTable(table)
                    .snapshotMapColumns(mapColumnList));
        }

        snapshotSource.snapshotMapTables(mapTableList);
        snapshot.snapshotTables(tableList);
    }

    public SnapshotSummaryModel makeSummaryModelFromSummary(SnapshotSummary snapshotSummary) {
        SnapshotSummaryModel summaryModel = new SnapshotSummaryModel()
                .id(snapshotSummary.getId().toString())
                .name(snapshotSummary.getName())
                .description(snapshotSummary.getDescription())
                .createdDate(snapshotSummary.getCreatedDate().toString())
                .profileId(snapshotSummary.getProfileId().toString());
        return summaryModel;
    }

    private SnapshotModel populateSnapshotModelFromSnapshot(Snapshot snapshot) {
        return new SnapshotModel()
                .id(snapshot.getId().toString())
                .name(snapshot.getName())
                .description(snapshot.getDescription())
                .createdDate(snapshot.getCreatedDate().toString())
                .profileId(snapshot.getProfileId().toString())
                .source(snapshot.getSnapshotSources()
                        .stream()
                        .map(source -> makeSourceModelFromSource(source))
                        .collect(Collectors.toList()))
                .tables(snapshot.getTables()
                        .stream()
                        .map(table -> makeTableModelFromTable(table))
                        .collect(Collectors.toList()));
    }

    private SnapshotSourceModel makeSourceModelFromSource(SnapshotSource source) {
        // TODO: when source summary methods are available, use those. Here I roll my own
        Dataset dataset = source.getDataset();
        DatasetSummaryModel summaryModel = new DatasetSummaryModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .defaultProfileId(dataset.getDefaultProfileId().toString())
                .createdDate(dataset.getCreatedDate().toString());

        SnapshotSourceModel sourceModel = new SnapshotSourceModel()
                .asset(source.getAssetSpecification().getName())
                .dataset(summaryModel);

        return sourceModel;
    }

    // TODO: share these methods with dataset table in some common place
    private TableModel makeTableModelFromTable(Table table) {
        return new TableModel()
                .name(table.getName())
                .columns(table.getColumns()
                        .stream()
                        .map(column -> makeColumnModelFromColumn(column))
                        .collect(Collectors.toList()));
    }

    private ColumnModel makeColumnModelFromColumn(Column column) {
        return new ColumnModel()
            .name(column.getName())
            .datatype(column.getType())
            .arrayOf(column.isArrayOf());
    }
}
