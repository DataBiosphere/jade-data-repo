package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.DatasetDao;
import bio.terra.dao.StudyDao;
import bio.terra.flight.dataset.create.DatasetCreateFlight;
import bio.terra.flight.dataset.delete.DatasetDeleteFlight;
import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.Column;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestContentsModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetRequestSourceModel;
import bio.terra.model.DatasetSourceModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.StudySummaryModel;
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
public class DatasetService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.DatasetService");

    private final Stairway stairway;
    private final StudyDao studyDao;
    private final DatasetDao datasetDao;

    @Autowired
    public DatasetService(Stairway stairway,
                          StudyDao studyDao,
                          DatasetDao datasetDao) {
        this.stairway = stairway;
        this.studyDao = studyDao;
        this.datasetDao = datasetDao;
    }

    /**
     * Kick-off dataset creation
     * Pre-condition: the dataset request has been syntax checked by the validator
     *
     * @param datasetRequestModel
     * @returns jobId (flightId) of the job
     */
    public String createDataset(DatasetRequestModel datasetRequestModel, AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Create dataset " + datasetRequestModel.getName());
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), datasetRequestModel);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        return stairway.submit(DatasetCreateFlight.class, flightMap);
    }

    /**
     * Kick-off dataset deletion
     *
     * @param id dataset id to delete
     * @returns jobId (flightId) of the job
     */
    public String deleteDataset(UUID id, AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Delete dataset " + id);
        // TODO talk about conventions for naming data in the input map
        // and in the step whether to pass data from input map to steps or let each step retrieve what they need
//        flightMap.put(JobMapKeys.REQUEST.getKeyName(), id);
        flightMap.put("id", id);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        return stairway.submit(DatasetDeleteFlight.class, flightMap);
    }

    /**
     * Enumerate a range of datasets ordered by created date for consistent offset processing
     * @param offset
     * @param limit
     * @return list of summary models of dataset
     */
    public EnumerateDatasetModel enumerateDatasets(
            int offset,
            int limit,
            String sort,
            String direction,
            String filter) {
        MetadataEnumeration<DatasetSummary> enumeration = datasetDao.retrieveDatasets(offset, limit, sort, direction,
            filter);
        List<DatasetSummaryModel> models = enumeration.getItems()
                .stream()
                .map(summary -> makeSummaryModelFromSummary(summary))
                .collect(Collectors.toList());
        return new EnumerateDatasetModel().items(models).total(enumeration.getTotal());
    }

    /**
     * Return a single dataset summary given the dataset id.
     * This is used in the create dataset flight to build the model response
     * of the asynchronous job.
     *
     * @param id
     * @return summary model of the dataset
     */
    public DatasetSummaryModel retrieveDatasetSummary(UUID id) {
        DatasetSummary datasetSummary = datasetDao.retrieveDatasetSummary(id);
        return makeSummaryModelFromSummary(datasetSummary);
    }

    /**
     * Return the output form of dataset
     * @param id
     * @return dataset model
     */
    public DatasetModel retrieveDataset(UUID id) {
        Dataset dataset = datasetDao.retrieveDataset(id);
        return makeDatasetModelFromDataset(dataset);
    }

    /**
     * Make a Dataset structure with all of its parts from an incoming dataset request.
     * Note that the structure does not have UUIDs or created dates filled in. Those are
     * updated by the DAO when it stores the dataset in the repository metadata.
     *
     * @param datasetRequestModel
     * @return Dataset
     */
    public Dataset makeDatasetFromDatasetRequest(DatasetRequestModel datasetRequestModel) {
        // Make this early so we can hook up back links to it
        Dataset dataset = new Dataset();

        List<DatasetRequestContentsModel> requestContentsList = datasetRequestModel.getContents();
        // TODO: for MVM we only allow one source list
        if (requestContentsList.size() > 1) {
            throw new ValidationException("Only a single dataset contents entry is currently allowed.");
        }
        DatasetRequestContentsModel requestContents = requestContentsList.get(0);
        DatasetSource datasetSource = makeSourceFromRequestContents(requestContents, dataset);

        // TODO: When we implement explicit definition of dataset tables, we will handle that here.
        // For now, we generate the dataset tables directly from the asset tables of the one source
        // allowed in a dataset.
        conjureDatasetTablesFromAsset(datasetSource.getAssetSpecification(), dataset, datasetSource);
        dataset.name(datasetRequestModel.getName())
                .description(datasetRequestModel.getDescription())
                .datasetSources(Collections.singletonList(datasetSource))
                .profileId(UUID.fromString(datasetRequestModel.getProfileId()));

        return dataset;
    }

    private DatasetSource makeSourceFromRequestContents(DatasetRequestContentsModel requestContents, Dataset dataset) {
        DatasetRequestSourceModel requestSource = requestContents.getSource();
        Study study = studyDao.retrieveByName(requestSource.getStudyName());

        Optional<AssetSpecification> optAsset = study.getAssetSpecificationByName(requestSource.getAssetName());
        if (!optAsset.isPresent()) {
            throw new AssetNotFoundException("Asset specification not found: " + requestSource.getAssetName());
        }

        // TODO: When we implement explicit definition of the dataset tables and mapping to study tables,
        // the map construction will go here. For MVM, we generate the mapping data directly from the asset spec.

        return new DatasetSource()
               .dataset(dataset)
               .study(study)
               .assetSpecification(optAsset.get());
    }

    /**
     * Magic up the dataset tables and dataset map from the asset tables and columns.
     * This method sets the table lists into dataset and datasetSource.
     *
     * @param asset  the one and only asset specification for this dataset
     * @param dataset dataset to point back to and fill in
     * @param datasetSource datasetSource to point back to and fill in
     */
    private void conjureDatasetTablesFromAsset(AssetSpecification asset,
                                               Dataset dataset,
                                               DatasetSource datasetSource) {

        List<Table> tableList = new ArrayList<>();
        List<DatasetMapTable> mapTableList = new ArrayList<>();

        for (AssetTable assetTable : asset.getAssetTables()) {
            // Create early so we can hook up back pointers.
            Table table = new Table();

            // Build the column lists in parallel, so we can easily connect the
            // map column to the dataset column.
            List<Column> columnList = new ArrayList<>();
            List<DatasetMapColumn> mapColumnList = new ArrayList<>();

            for (AssetColumn assetColumn : assetTable.getColumns()) {
                Column column = new Column(assetColumn.getStudyColumn());
                columnList.add(column);

                mapColumnList.add(new DatasetMapColumn()
                    .fromColumn(assetColumn.getStudyColumn())
                    .toColumn(column));
            }

            table.name(assetTable.getTable().getName())
                    .columns(columnList);
            tableList.add(table);
            mapTableList.add(new DatasetMapTable()
                    .fromTable(assetTable.getTable())
                    .toTable(table)
                    .datasetMapColumns(mapColumnList));
        }

        datasetSource.datasetMapTables(mapTableList);
        dataset.datasetTables(tableList);
    }

    public DatasetSummaryModel makeSummaryModelFromSummary(DatasetSummary datasetSummary) {
        DatasetSummaryModel summaryModel = new DatasetSummaryModel()
                .id(datasetSummary.getId().toString())
                .name(datasetSummary.getName())
                .description(datasetSummary.getDescription())
                .createdDate(datasetSummary.getCreatedDate().toString())
                .profileId(datasetSummary.getProfileId().toString());
        return summaryModel;
    }

    private DatasetModel makeDatasetModelFromDataset(Dataset dataset) {
        return new DatasetModel()
                .id(dataset.getId().toString())
                .name(dataset.getName())
                .description(dataset.getDescription())
                .createdDate(dataset.getCreatedDate().toString())
                .profileId(dataset.getProfileId().toString())
                .source(dataset.getDatasetSources()
                        .stream()
                        .map(source -> makeSourceModelFromSource(source))
                        .collect(Collectors.toList()))
                .tables(dataset.getTables()
                        .stream()
                        .map(table -> makeTableModelFromTable(table))
                        .collect(Collectors.toList()));
    }

    private DatasetSourceModel makeSourceModelFromSource(DatasetSource source) {
        // TODO: when source summary methods are available, use those. Here I roll my own
        Study study = source.getStudy();
        StudySummaryModel summaryModel = new StudySummaryModel()
                .id(study.getId().toString())
                .name(study.getName())
                .description(study.getDescription())
                .defaultProfileId(study.getDefaultProfileId().toString())
                .createdDate(study.getCreatedDate().toString());

        DatasetSourceModel sourceModel = new DatasetSourceModel()
                .asset(source.getAssetSpecification().getName())
                .study(summaryModel);

        return sourceModel;
    }

    // TODO: share these methods with study table in some common place
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
