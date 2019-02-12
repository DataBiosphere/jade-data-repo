package bio.terra.flight.study.create;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateStudyMetadataStep implements Step {

    private StudyDao studyDao;

    public CreateStudyMetadataStep(StudyDao studyDao) {
        this.studyDao = studyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get("request", StudyRequestModel.class);
        Study newStudy = StudyJsonConversion.studyRequestToStudy(studyRequest);
        studyDao.create(newStudy);
        StudySummaryModel studySummary = StudyJsonConversion.studySummaryFromStudy(newStudy);
        workingMap.put("response", studySummary);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get("request", StudyRequestModel.class);
        String studyName = studyRequest.getName();
        studyDao.deleteByName(studyName);
        return StepResult.getStepResultSuccess();
    }
}

