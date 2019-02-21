package bio.terra.fixtures;

import bio.terra.model.StudySummaryModel;


public final class StudyFixtures {
    private StudyFixtures() {}

    public static StudySummaryModel makeStudySummary() {

        final StudySummaryModel minimalStudySummary = new StudySummaryModel()
                .id("Minimal")
                .name("Minimal")
                .description("This is a sample study definition");
        return minimalStudySummary;
    }
}
