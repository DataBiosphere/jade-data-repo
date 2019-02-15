package bio.terra.service;

import bio.terra.controller.exception.ApiException;
import bio.terra.dao.StudyDao;
import bio.terra.exceptions.NotFoundException;
import bio.terra.exceptions.ValidationException;
import bio.terra.metadata.AssetColumn;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.AssetTable;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Column;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.Table;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetRequestSourceModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Component
public class DatasetService {
    private Stairway stairway;
    private StudyDao studyDao;

    @Autowired
    public DatasetService(Stairway stairway, StudyDao studyDao) {
        this.stairway = stairway;
        this.studyDao = studyDao;
    }

    /**
     * Kick-off dataset creation
     * Pre-condition: the dataset request has been syntax checked by the validator
     *
     * @param datasetRequestModel
     * @returns jobId (flightId) of the job
     */
    public String createDataset(DatasetRequestModel datasetRequestModel) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Create dataset " + datasetRequestModel.getName());
        flightMap.put("request", datasetRequestModel);
        // TODO: wire this up
        //return stairway.submit(DatasetCreateFlight.class, flightMap);
        throw new ApiException("Create dataset is not implemented");
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

        List<DatasetRequestSourceModel> requestSourceList = datasetRequestModel.getSource();
        // TODO: for MVM we only allow one source list
        if (requestSourceList.size() > 1) {
            throw new ValidationException("Only a single dataset source is currently allowed.");
        }
        DatasetRequestSourceModel requestSource = requestSourceList.get(0);
        DatasetSource datasetSource = makeSourceFromRequestSource(requestSource, dataset);



        // TODO: When we implement explicit definition of dataset tables, we will handle that here.
        // For now, we generate the dataset tables directly from the asset tables of the one source
        // allowed in a dataset.
        conjureDatasetTablesFromAsset(datasetSource.getAssetSpecification(), dataset, datasetSource);

        dataset.name(datasetRequestModel.getName())
                .description(datasetRequestModel.getDescription())
                .datasetSources(Collections.singletonList(datasetSource));

        return dataset;
    }

    private DatasetSource makeSourceFromRequestSource(DatasetRequestSourceModel requestSource, Dataset dataset) {
        Optional<Study> optStudy = studyDao.retrieveByName(requestSource.getStudyName());
        if (!optStudy.isPresent()) {
            throw new NotFoundException("Study not found: " + requestSource.getStudyName());
        }
        Study study = optStudy.get();

        Optional<AssetSpecification> optAsset = study.getAssetSpecificationByName(requestSource.getAssetName());
        if (!optAsset.isPresent()) {
            throw new NotFoundException("Asset specification not found: " + requestSource.getAssetName());
        }
        AssetSpecification asset = optAsset.get();
        AssetTable assetTable = asset.getRootTable();

        // We don't save the studyColumn in the dataset source; we can navigate to it, but we need to
        // validate that the column name exists.
        Optional<StudyTableColumn> optColumn = assetTable.getStudyColumnByName(requestSource.getFieldName());
        if (!optColumn.isPresent()) {
            throw new NotFoundException("Source column not found: " + requestSource.getFieldName());
        }

        // TODO: When we implement explicit definition of the dataset tables and mapping to study tables,
        // the map construction will go here. For MVM, we generate the mapping data directly from the asset spec.

        return new DatasetSource()
               .dataset(dataset)
               .study(study)
               .assetSpecification(asset);
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
                Column column = new Column()
                        .table(table)
                        .name(assetColumn.getStudyColumn().getName())
                        .type(assetColumn.getStudyColumn().getType());
                columnList.add(column);

                mapColumnList.add(new DatasetMapColumn()
                        .fromColumn(assetColumn.getStudyColumn())
                        .toColumn(column));
            }

            table.name(assetTable.getStudyTable().getName())
                    .columns(columnList);
            tableList.add(table);
            mapTableList.add(new DatasetMapTable()
                    .fromTable(assetTable.getStudyTable())
                    .toTable(table));
        }

        datasetSource.datasetMapTables(mapTableList);
        dataset.datasetTables(tableList);
    }

}
