package bio.terra.filesystem.flight.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.flight.FileMapKeys;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFileInfo;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestFilePrimaryDataStep implements Step {
    private final FireStoreDao fileDao;
    private final GcsPdao gcsPdao;
    private final Dataset dataset;

    public IngestFilePrimaryDataStep(FireStoreDao fileDao,
                                     Dataset dataset,
                                     GcsPdao gcsPdao) {
        this.fileDao = fileDao;
        this.gcsPdao = gcsPdao;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        FSFileInfo fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, objectId);
        workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
// TODO: Need to fix this
/*
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);

        gcsPdao.deleteFile(dataset, fileLoadModel, objectId);
*/
        return StepResult.getStepResultSuccess();
    }

}
