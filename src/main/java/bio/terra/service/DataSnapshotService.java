package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.DataSnapshotDao;
import bio.terra.dao.DrDatasetDao;
import bio.terra.flight.datasnapshot.create.DataSnapshotCreateFlight;
import bio.terra.flight.datasnapshot.delete.DataSnapshotDeleteFlight;
import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.Column;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotMapColumn;
import bio.terra.metadata.DataSnapshotMapTable;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.DataSnapshotSummary;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Table;
import bio.terra.model.ColumnModel;
import bio.terra.model.DataSnapshotModel;
import bio.terra.model.DataSnapshotRequestContentsModel;
import bio.terra.model.DataSnapshotRequestModel;
import bio.terra.model.DataSnapshotRequestSourceModel;
import bio.terra.model.DataSnapshotSourceModel;
import bio.terra.model.DataSnapshotSummaryModel;
import bio.terra.model.EnumerateDataSnapshotModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.model.TableModel;
import bio.terra.service.exception.AssetNotFoundException;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class DataSnapshotService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.DataSnapshotService");

    private final Stairway stairway;
    private final DrDatasetDao datasetDao;
    private final DataSnapshotDao dataSnapshotDao;

    @Autowired
    public DataSnapshotService(Stairway stairway,
                               DrDatasetDao datasetDao,
                               DataSnapshotDao dataSnapshotDao) {
        this.stairway = stairway;
        this.datasetDao = datasetDao;
        this.dataSnapshotDao = dataSnapshotDao;
    }

    /**
     * Kick-off dataSnapshot creation
     * Pre-condition: the dataSnapshot request has been syntax checked by the validator
     *
     * @param dataSnapshotRequestModel
     * @returns jobId (flightId) of the job
     */
    public String createDataSnapshot(DataSnapshotRequestModel dataSnapshotRequestModel,
                                     AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Create dataSnapshot " + dataSnapshotRequestModel.getName());
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), dataSnapshotRequestModel);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        return stairway.submit(DataSnapshotCreateFlight.class, flightMap);
    }

    /**
     * Kick-off dataSnapshot deletion
     *
     * @param id dataSnapshot id to delete
     * @returns jobId (flightId) of the job
     */
    public String deleteDataSnapshot(UUID id, AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Delete dataSnapshot " + id);
        // TODO talk about conventions for naming data in the input map
        // and in the step whether to pass data from input map to steps or let each step retrieve what they need
//        flightMap.put(JobMapKeys.REQUEST.getKeyName(), id);
        flightMap.put("id", id);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        return stairway.submit(DataSnapshotDeleteFlight.class, flightMap);
    }

    /**
     * Enumerate a range of dataSnapshots ordered by created date for consistent offset processing
     * @param offset
     * @param limit
     * @return list of summary models of dataSnapshot
     */
    public EnumerateDataSnapshotModel enumerateDataSnapshots(
        int offset,
        int limit,
        String sort,
        String direction,
        String filter
    ) {
        MetadataEnumeration<DataSnapshotSummary> enumeration = dataSnapshotDao.retrieveDataSnapshots(
            offset, limit, sort, direction, filter);
        List<DataSnapshotSummaryModel> models = enumeration.getItems()
                .stream()
                .map(summary -> makeSummaryModelFromSummary(summary))
                .collect(Collectors.toList());
        return new EnumerateDataSnapshotModel().items(models).total(enumeration.getTotal());
    }

    /**
     * Return a single dataSnapshot summary given the dataSnapshot id.
     * This is used in the create dataSnapshot flight to build the model response
     * of the asynchronous job.
     *
     * @param id
     * @return summary model of the dataSnapshot
     */
    public DataSnapshotSummaryModel retrieveDataSnapshotSummary(UUID id) {
        DataSnapshotSummary dataSnapshotSummary = dataSnapshotDao.retrieveDataSnapshotSummary(id);
        return makeSummaryModelFromSummary(dataSnapshotSummary);
    }

    /**
     * Return the output form of dataSnapshot
     * @param id
     * @return dataSnapshot model
     */
    public DataSnapshotModel retrieveDataSnapshot(UUID id) {
        DataSnapshot dataSnapshot = dataSnapshotDao.retrieveDataSnapshot(id);
        return makeDataSnapshotModelFromDataSnapshot(dataSnapshot);
    }

    /**
     * Make a DataSnapshot structure with all of its parts from an incoming dataSnapshot request.
     * Note that the structure does not have UUIDs or created dates filled in. Those are
     * updated by the DAO when it stores the dataSnapshot in the repository metadata.
     *
     * @param dataSnapshotRequestModel
     * @return DataSnapshot
     */
    public DataSnapshot makeDataSnapshotFromDataSnapshotRequest(DataSnapshotRequestModel dataSnapshotRequestModel) {
        // Make this early so we can hook up back links to it
        DataSnapshot dataSnapshot = new DataSnapshot();

        List<DataSnapshotRequestContentsModel> requestContentsList = dataSnapshotRequestModel.getContents();
        // TODO: for MVM we only allow one source list
        if (requestContentsList.size() > 1) {
            throw new ValidationException("Only a single dataSnapshot contents entry is currently allowed.");
        }
        DataSnapshotRequestContentsModel requestContents = requestContentsList.get(0);
        DataSnapshotSource dataSnapshotSource = makeSourceFromRequestContents(requestContents, dataSnapshot);

        // TODO: When we implement explicit definition of data snapshot tables, we will handle that here.
        // For now, we generate the data snapshot tables directly from the asset tables of the one source
        // allowed in a data snapshot.
        conjureDataSnapshotTablesFromAsset(
            dataSnapshotSource.getAssetSpecification(), dataSnapshot, dataSnapshotSource);

        dataSnapshot.name(dataSnapshotRequestModel.getName())
                .description(dataSnapshotRequestModel.getDescription())
                .dataSnapshotSources(Collections.singletonList(dataSnapshotSource));

        return dataSnapshot;
    }

    private DataSnapshotSource makeSourceFromRequestContents(
        DataSnapshotRequestContentsModel requestContents, DataSnapshot dataSnapshot) {
        DataSnapshotRequestSourceModel requestSource = requestContents.getSource();
        DrDataset dataset = datasetDao.retrieveByName(requestSource.getDatasetName());

        Optional<AssetSpecification> optAsset = dataset.getAssetSpecificationByName(requestSource.getAssetName());
        if (!optAsset.isPresent()) {
            throw new AssetNotFoundException("Asset specification not found: " + requestSource.getAssetName());
        }

        // TODO: When we implement explicit definition of the data snapshot tables and mapping to dataset tables,
        // the map construction will go here. For MVM, we generate the mapping data directly from the asset spec.

        return new DataSnapshotSource()
               .dataSnapshot(dataSnapshot)
               .dataset(dataset)
               .assetSpecification(optAsset.get());
    }

    /**
     * Magic up the dataSnapshot tables and dataSnapshot map from the asset tables and columns.
     * This method sets the table lists into dataSnapshot and dataSnapshotSource.
     *
     * @param asset  the one and only asset specification for this dataSnapshot
     * @param dataSnapshot dataSnapshot to point back to and fill in
     * @param dataSnapshotSource dataSnapshotSource to point back to and fill in
     */
    private void conjureDataSnapshotTablesFromAsset(AssetSpecification asset,
                                               DataSnapshot dataSnapshot,
                                               DataSnapshotSource dataSnapshotSource) {

        List<Table> tableList = new ArrayList<>();
        List<DataSnapshotMapTable> mapTableList = new ArrayList<>();

        for (AssetTable assetTable : asset.getAssetTables()) {
            // Create early so we can hook up back pointers.
            Table table = new Table();

            // Build the column lists in parallel, so we can easily connect the
            // map column to the data snapshot column.
            List<Column> columnList = new ArrayList<>();
            List<DataSnapshotMapColumn> mapColumnList = new ArrayList<>();

            for (AssetColumn assetColumn : assetTable.getColumns()) {
                Column column = new Column(assetColumn.getDatasetColumn());
                columnList.add(column);

                mapColumnList.add(new DataSnapshotMapColumn()
                    .fromColumn(assetColumn.getDatasetColumn())
                    .toColumn(column));
            }

            table.name(assetTable.getTable().getName())
                    .columns(columnList);
            tableList.add(table);
            mapTableList.add(new DataSnapshotMapTable()
                    .fromTable(assetTable.getTable())
                    .toTable(table)
                    .dataSnapshotMapColumns(mapColumnList));
        }

        dataSnapshotSource.dataSnapshotMapTables(mapTableList);
        dataSnapshot.dataSnapshotTables(tableList);
    }

    public DataSnapshotSummaryModel makeSummaryModelFromSummary(DataSnapshotSummary dataSnapshotSummary) {
        DataSnapshotSummaryModel summaryModel = new DataSnapshotSummaryModel()
                .id(dataSnapshotSummary.getId().toString())
                .name(dataSnapshotSummary.getName())
                .description(dataSnapshotSummary.getDescription())
                .createdDate(dataSnapshotSummary.getCreatedDate().toString());
        return summaryModel;
    }

    private DataSnapshotModel makeDataSnapshotModelFromDataSnapshot(DataSnapshot dataSnapshot) {
        return new DataSnapshotModel()
                .id(dataSnapshot.getId().toString())
                .name(dataSnapshot.getName())
                .description(dataSnapshot.getDescription())
                .createdDate(dataSnapshot.getCreatedDate().toString())
                .source(dataSnapshot.getDataSnapshotSources()
                        .stream()
                        .map(source -> makeSourceModelFromSource(source))
                        .collect(Collectors.toList()))
                .tables(dataSnapshot.getTables()
                        .stream()
                        .map(table -> makeTableModelFromTable(table))
                        .collect(Collectors.toList()));
    }

    private DataSnapshotSourceModel makeSourceModelFromSource(DataSnapshotSource source) {
        // TODO: when source summary methods are available, use those. Here I roll my own
        DrDataset dataset = source.getDataset();
        DrDatasetSummaryModel summaryModel = new DrDatasetSummaryModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .createdDate(dataset.getCreatedDate().toString());

        DataSnapshotSourceModel sourceModel = new DataSnapshotSourceModel()
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
