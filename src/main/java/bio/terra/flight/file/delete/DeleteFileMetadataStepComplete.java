package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.Dataset;
import bio.terra.model.DeleteResponseModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;


public class DeleteFileMetadataStepComplete implements Step {
    private final FireStoreFileDao fileDao;
    private final String fileId;
    private final Dataset dataset;

    public DeleteFileMetadataStepComplete(FireStoreFileDao fileDao,
                                          String fileId,
                                          Dataset dataset) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        boolean found = fileDao.deleteFileComplete(dataset, fileId, context.getFlightId());
        DeleteResponseModel.ObjectStateEnum stateEnum =
            (found) ? DeleteResponseModel.ObjectStateEnum.DELETED : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
        DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
        FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible
        return StepResult.getStepResultSuccess();
    }

}
