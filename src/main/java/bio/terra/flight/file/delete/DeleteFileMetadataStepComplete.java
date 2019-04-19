package bio.terra.flight.file.delete;

import bio.terra.filesystem.FileDao;
import bio.terra.flight.FlightUtils;
import bio.terra.model.DeleteResponseModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteFileMetadataStepComplete implements Step {
    private final FileDao fileDao;
    private final UUID fileId;

    public DeleteFileMetadataStepComplete(FileDao fileDao, String fileId) {
        this.fileDao = fileDao;
        this.fileId = UUID.fromString(fileId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        boolean found = fileDao.deleteFileComplete(fileId, context.getFlightId());
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
