package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.filedata.google.gcs.GcsBufferedReader;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFileStep implements Step {
    // TODO: MAYBE make these configuration items?
    private static final int BATCH_SIZE = 50;
    private static final int MAX_BAD_LINES = 5;

    private final LoadService loadService;
    private final ObjectMapper objectMapper;

    public IngestPopulateFileStateFromFileStep(LoadService loadService, ObjectMapper objectMapper) {
        this.loadService = loadService;
        this.objectMapper = objectMapper;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        BulkLoadRequestModel loadRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);

        FlightMap workingMap = context.getWorkingMap();
        UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

        Storage storage = StorageOptions.getDefaultInstance().getService();
        List<String> errorDetails = new ArrayList<>();

        try (BufferedReader reader = new GcsBufferedReader(storage, loadRequest.getLoadControlFile())) {
            long lineCount = 0;
            List<BulkLoadFileModel> fileList = new ArrayList<>();

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                lineCount++;

                try {
                    BulkLoadFileModel loadFile = objectMapper.readValue(line, BulkLoadFileModel.class);
                    fileList.add(loadFile);
                } catch (IOException ex) {
                    errorDetails.add("Format error at line " + lineCount + ": " + ex.getMessage());
                    if (errorDetails.size() > MAX_BAD_LINES) {
                        throw new BulkLoadControlFileException(
                            "More than " + MAX_BAD_LINES + " bad lines in the control file",
                            errorDetails);
                    }
                }

                // Keep this check and load out of the inner try; it should only catch objectMapper failures
                if (fileList.size() > BATCH_SIZE) {
                    loadService.populateFiles(loadId, fileList);
                    fileList.clear();
                }
            }

            // If there are errors in the load file, don't do the load
            if (errorDetails.size() > 0) {
                throw new BulkLoadControlFileException(
                    "There were " + errorDetails.size() + " bad lines in the control file",
                    errorDetails);
            }

            if (fileList.size() > 0) {
                loadService.populateFiles(loadId, fileList);
            }

        } catch (IOException ex) {
            throw new BulkLoadControlFileException("Failure accessing the load control file", ex);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

        loadService.cleanFiles(loadId);
        return StepResult.getStepResultSuccess();
    }
}
