package bio.terra.fixtures;

import bio.terra.model.StudySummaryModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import org.springframework.http.HttpStatus;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Optional;


public final class StudyFixtures {
    private StudyFixtures() {}

    public static final StudySummaryModel studySummary = new StudySummaryModel()
            .name("Minimal")
            .description("This is a sample study definition");
    public static final StudySummaryModel minimalStudySummary = new StudySummaryModel()
            .id("Minimal")
            .name("Minimal")
            .description("This is a sample study definition");

    }
}
