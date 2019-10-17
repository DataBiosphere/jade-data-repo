package bio.terra.service.filedata.flight.delete;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.FlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.model.DeleteResponseModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;


public class DeleteFileDirectoryStep implements Step {
    private final FireStoreDao fileDao;
    private final String fileId;
    private final Dataset dataset;

    public DeleteFileDirectoryStep(FireStoreDao fileDao,
                                   String fileId,
                                   Dataset dataset) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        boolean found = fileDao.deleteDirectoryEntry(dataset, fileId);
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
