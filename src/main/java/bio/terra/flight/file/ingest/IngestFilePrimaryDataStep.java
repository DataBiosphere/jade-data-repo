package bio.terra.flight.file.ingest;

import bio.terra.dao.FileDao;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.service.FileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestFilePrimaryDataStep implements Step {
    private final FileDao fileDao;
    private final FileService fileService;

    public IngestFilePrimaryDataStep(FileDao fileDao, FileService fileService) {
        this.fileDao = fileDao;
        this.fileService = fileService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // TODO: actually do primary data work
        // For now, just jam in pretend data for the result

        FlightMap workingMap = context.getWorkingMap();
        //UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));
        workingMap.put(FileMapKeys.CHECKSUM, "theChecksum");
        workingMap.put(FileMapKeys.GSPATH, "gs://bucket/path/file.ext");
        workingMap.put(FileMapKeys.SIZE, 42L);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
