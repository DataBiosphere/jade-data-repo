package bio.terra.flight.study.delete;

import bio.terra.dao.StudyDao;
import bio.terra.flight.FlightUtils;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteStudyMetadataStep implements Step {
    private StudyDao studyDao;

    public DeleteStudyMetadataStep(StudyDao studyDao) {
        this.studyDao = studyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID studyId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        boolean success = studyDao.delete(studyId);
        DeleteResponseModel.ObjectStateEnum stateEnum =
            (success) ? DeleteResponseModel.ObjectStateEnum.DELETED : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
        DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
        FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // no undo is possible
        return StepResult.getStepResultSuccess();
    }

}
