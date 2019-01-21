package bio.terra.flight.step;

import bio.terra.dao.StudyDAO;
import bio.terra.metadata.Study;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateStudyMetadataStep implements Step {

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FlightMap inputParameters = context.getInputParameters();
        Study study = inputParameters.get("study", Study.class);
        StudyDAO studyDAO = inputParameters.get("studyDAO", StudyDAO.class);
        studyDAO.create(study);
        // TODO: get the id back, fetch the Study by ID and return a summary
        StudySummaryModel studySummary = new StudySummaryModel()
                .name(study.getName())
                .description(study.getDescription());
        workingMap.put("summary", studySummary);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

