package bio.terra.flight.dataset.ingest;

import bio.terra.flight.FlightUtils;
import bio.terra.flight.exception.IngestFileNotFoundException;
import bio.terra.flight.exception.InvalidIngestStrategyException;
import bio.terra.flight.exception.InvalidUriException;
import bio.terra.metadata.Column;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoConstant;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.bigquery.BigQueryProject;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import liquibase.util.StringUtils;

import java.util.List;

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
    private BigQueryPdao bigQueryPdao;

    static void validateSourceUri(String sourcePath) {
        IngestUtils.GsUrlParts gsParts = IngestUtils.parseBlobUri(sourcePath);

        // Bucket wildcards are never supported.
        if (gsParts.getBucket().indexOf('*') > -1) {
            throw new InvalidUriException("Buckets for ingest source files cannot contain '*': " + sourcePath);
        }

        int globIndex = gsParts.getPath().indexOf('*');
        if (globIndex == -1) {
            // If the user is trying to ingest a single file, verify it exists.
            // TODO: If we're OK letting BQ hit and return the "not found" error in the wildcard case,
            // could we also be OK with that behavior here?
            try {
                Storage storage = StorageOptions.getDefaultInstance().getService();
                BlobId blobId = BlobId.of(gsParts.getBucket(), gsParts.getPath());
                Blob blob = storage.get(blobId);
                if (blob == null || !blob.exists()) {
                    throw new IngestFileNotFoundException("Ingest source file not found: " + sourcePath);
                }
            } catch (StorageException ex) {
                throw new InvalidUriException("Failed to access ingest source file: " + sourcePath, ex);
            }
        } else {
            // BigQuery's wilcard support is restricted to a single glob.
            int lastGlobIndex = gsParts.getPath().lastIndexOf('*');
            if (globIndex != lastGlobIndex) {
                throw new InvalidUriException("Ingest source files may only contain one wildcard: " + sourcePath);
            }
        }
    }

    public IngestSetupStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
        validateSourceUri(ingestRequestModel.getPath());

        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        IngestUtils.putDatasetName(context, dataset.getName());

        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String baseName = PdaoConstant.PDAO_PREFIX + StringUtils.substring(targetTable.getName(), 0, 10);
        String sgName = FlightUtils.randomizeNameInfix(baseName, "_st_");
        IngestUtils.putStagingTableName(context, sgName);

        IngestRequestModel.StrategyEnum ingestStrategy = ingestRequestModel.getStrategy();

        if (ingestStrategy == IngestRequestModel.StrategyEnum.UPSERT) {
            List<Column> primaryKey = dataset
                .getTableByName(targetTable.getName()).orElseThrow(IllegalStateException::new)
                .getPrimaryKey();
            if (primaryKey.size() < 1) {
                throw new InvalidIngestStrategyException(
                    "Cannot use ingestStrategy `upsert` on table with no primary key: " + targetTable.getName());
            }

            Schema overlappingTableSchema = bigQueryPdao.buildOverlappingTableSchema();
            BigQueryProject bigQueryProject = bigQueryPdao.bigQueryProjectForDataset(dataset);

            String olName = FlightUtils.randomizeNameInfix(baseName, "_ol_");

            IngestUtils.putOverlappingTableName(context, olName);
            String overlappingTableName = IngestUtils.getOverlappingTableName(context);

            bigQueryProject.createTable(bigQueryPdao.prefixName(dataset.getName()),
                overlappingTableName,
                overlappingTableSchema);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to undo
        return StepResult.getStepResultSuccess();
    }
}
