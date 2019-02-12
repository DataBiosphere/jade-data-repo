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

    private StudyDao studyDAO;

    public CreateStudyMetadataStep(StudyDao studyDAO) {
        this.studyDAO = studyDAO;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get("request", StudyRequestModel.class);
        Study newStudy = StudyJsonConversion.studyRequestToStudy(studyRequest);
//        UUID studyid =
        studyDAO.create(newStudy);
        // TODO: get the id back, fetch the Study by ID and return a summary with created_date
        StudySummaryModel studySummary = StudyJsonConversion.studySummaryFromStudy(newStudy);
        workingMap.put("response", studySummary);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // TODO: delete study with DAO
        return StepResult.getStepResultSuccess();
    }
}

