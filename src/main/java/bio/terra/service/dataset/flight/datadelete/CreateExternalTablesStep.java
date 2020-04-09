package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.exception.PdaoInvalidUriException;
import bio.terra.common.exception.PdaoSourceFileNotFoundException;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.InvalidFileRefException;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getSuffix;


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
        IngestUtils.GsUrlParts gsUrlParts = IngestUtils.parseBlobUri(path);
        try {
            if (gsUrlParts.getIsWildcard()) {
                // There's not an easy way to check this with the SDK. Let BQ handle catching this on its side.
                return true;
            } else {
                return GcsPdao.getBlobFromGsPath(storage, path).exists();
            }
        } catch (PdaoInvalidUriException | PdaoSourceFileNotFoundException ex) {
            logger.warn("Could not fetch details about: " + path, ex);
            throw ex;
        }
    }

    private void validateFilesExistForTables(DataDeletionRequest request) {
        List<String> missingFiles = request.getTables()
            .stream()
            .filter(t -> !fileExistsForTable(t))
            .map(t -> t.getGcsFileSpec().getPath())
            .collect(Collectors.toList());

        if (missingFiles.size() > 0) {
            throw new InvalidFileRefException("Not all files referenced in request exist.", missingFiles);
        }
    }

    private void validateTablesExistInDataset(DataDeletionRequest request, Dataset dataset) {
        List<String> missingTables = request.getTables()
            .stream()
            .filter(t -> !dataset.getTableByName(t.getTableName()).isPresent())
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());

        if (missingTables.size() > 0) {
            throw new TableNotFoundException("Not all tables from request exist in dataset: " +
                String.join(", ", missingTables));
        }
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = getDataset(context, datasetService);
        String suffix = getSuffix(context);
        DataDeletionRequest dataDeletionRequest = getRequest(context);

        validateTablesExistInDataset(dataDeletionRequest, dataset);
        validateFilesExistForTables(dataDeletionRequest);

        dataDeletionRequest.getTables().forEach(table -> {
            String path = table.getGcsFileSpec().getPath();
            bigQueryPdao.createSoftDeleteExternalTable(dataset, path, table.getTableName(), suffix);
        });

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        Dataset dataset = getDataset(context, datasetService);
        String suffix = getSuffix(context);

        for (DataDeletionTableModel table : getRequest(context).getTables()) {
            bigQueryPdao.deleteSoftDeleteExternalTable(dataset, table.getTableName(), suffix);
        }

        return StepResult.getStepResultSuccess();
    }
}

