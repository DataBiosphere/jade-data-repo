package bio.terra.flight.dataset.ingest;

import bio.terra.flight.FlightUtils;
import bio.terra.flight.exception.IngestFileNotFoundException;
import bio.terra.flight.exception.InvalidUriException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoConstant;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import liquibase.util.StringUtils;

/**
 *  * The setup step required to generate the staging file name.
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
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        IngestUtils.putDatasetName(context, dataset.getName());

        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String baseName = PdaoConstant.PDAO_PREFIX + StringUtils.substring(targetTable.getName(), 0, 10);
        String sgName = FlightUtils.randomizeNameInfix(baseName, "_st_");
        IngestUtils.putStagingTableName(context, sgName);

        IngestRequestModel requestModel = IngestUtils.getIngestRequestModel(context);
        IngestUtils.GsUrlParts gsParts = IngestUtils.parseBlobUri(requestModel.getPath());

        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(gsParts.getBucket(), gsParts.getPath());
            Blob blob = storage.get(blobId);
            if (!blob.exists()) {
                throw new IngestFileNotFoundException("Ingest source file not found: '" + requestModel.getPath());
            }
        } catch (StorageException ex) {
            throw new InvalidUriException("Failed to access ingest source file: '" + requestModel.getPath());
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to undo
        return StepResult.getStepResultSuccess();
    }
}
