package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.exception.PdaoInvalidUriException;
import bio.terra.common.exception.PdaoSourceFileNotFoundException;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.InvalidFileRefException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


public class CreateExternalTablesStep implements Step {

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;
    private final Storage storage;

    private static Logger logger = LoggerFactory.getLogger(CreateExternalTablesStep.class);

    public CreateExternalTablesStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.storage = StorageOptions.getDefaultInstance().getService();
    }

    private boolean fileExistsForTable(DataDeletionTableModel table) {
        // TODO: support other file types (assuming header-less csv here)
        String path  = table.getGcsFileSpec().getPath();
        try {
            return GcsPdao.getBlobFromGsPath(storage, path).exists();
        } catch (PdaoInvalidUriException | PdaoSourceFileNotFoundException ex) {
            logger.warn("Could not fetch details about: " + path, ex);
            throw ex;
        }
    }

    private void validateFilesExistForTables(DataDeletionRequest request) {
        // TODO: revisit the name of this? table could be confusing here
        List<DataDeletionTableModel> tablesMissingFiles = request.getTables()
            .stream()
            .filter(t -> !fileExistsForTable(t))
            .collect(Collectors.toList());

        if (tablesMissingFiles.size() > 0) {
            List<String> badPaths = tablesMissingFiles
                .stream()
                .map(t -> t.getGcsFileSpec().getPath())
                .collect(Collectors.toList());
            throw new InvalidFileRefException("Not all files referenced in request exist.", badPaths);
        }
    }

    private String suffix(FlightContext context) {
        return context.getFlightId().replace('-', '_');
    }

    private DataDeletionRequest request(FlightContext context) {
        return context.getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);
    }

    private Dataset dataset(FlightContext context) {
        String datasetId = context.getInputParameters()
            .get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        return datasetService.retrieve(UUID.fromString(datasetId));
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = dataset(context);
        String suffix = suffix(context);
        DataDeletionRequest dataDeletionRequest = request(context);

        validateFilesExistForTables(dataDeletionRequest);

        dataDeletionRequest.getTables().forEach(table -> {
            String path = table.getGcsFileSpec().getPath();
            bigQueryPdao.createExternalTable(dataset, path, table.getTableName(), suffix);
        });

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        Dataset dataset = dataset(context);
        String suffix = suffix(context);

        for (DataDeletionTableModel table : request(context).getTables()) {
            bigQueryPdao.deleteExternalTable(dataset, table.getTableName(), suffix);
        }

        return StepResult.getStepResultSuccess();
    }
}

