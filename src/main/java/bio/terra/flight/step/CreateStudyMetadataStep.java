package bio.terra.flight.step;

import bio.terra.dao.RelationshipDao;
import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateStudyMetadataStep implements Step {

    private StudyDao studyDAO;
    private RelationshipDao relationshipDao;

    public CreateStudyMetadataStep(StudyDao studyDAO) {
        this.studyDAO = studyDAO;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get("request", StudyRequestModel.class);
        Study newStudy = new Study(studyRequest);
        UUID studyid = studyDAO.create(newStudy);
        // TODO: get the id back, fetch the Study by ID and return a summary
        StudySummaryModel studySummary = new StudySummaryModel()
                .id(studyid.toString())
                .name(newStudy.getName())
                .description(newStudy.getDescription());
        workingMap.put("response", studySummary);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

