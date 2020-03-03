package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoConstant;
import bio.terra.common.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightUtils;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import liquibase.util.StringUtils;


/**
 * The setup step required to generate the staging file name.
 *
 * You might ask, "why can't you do that in the staging table step?"
 * The answer is that we need a step boundary so that the staging
 * table name is written to the database. Otherwise, on a failure of
 * stairway, the staging table name would be lost and could not be
 * found to either continue the ingest or to undo it.
 *
 * In addition, it sets up several other things:
 *
 * First, it does an existence check on the source blob to ensure it is accessible
 * to the data repository. We could put this off and only do it in the load step.
 * I am thinking that it is better to do a sanity check before we create objects
 * in BigQuery. It is no guarantee of course, since the file could be deleted
 * by the time we try the load.
 *
 * Second, it stores away the dataset name. Several steps only need the dataset name
 * and not the dataset object.
 */
public class IngestSetupStep implements Step {
    private DatasetService datasetService;

    public IngestSetupStep(DatasetService datasetService) {
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
        // We don't actually care about the output here since BQ takes the raw "gs://" string as input.
        // As long as parsing succeeds, we're good to move forward.
        IngestUtils.parseBlobUri(ingestRequestModel.getPath());

        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        IngestUtils.putDatasetName(context, dataset.getName());

        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String baseName = PdaoConstant.PDAO_PREFIX + StringUtils.substring(targetTable.getName(), 0, 10);
        String sgName = FlightUtils.randomizeNameInfix(baseName, "_st_");
        IngestUtils.putStagingTableName(context, sgName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to undo
        return StepResult.getStepResultSuccess();
    }
}
