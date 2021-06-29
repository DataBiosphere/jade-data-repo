package bio.terra.flight;

import bio.terra.common.category.Unit;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightMap;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@Category(Unit.class)
public class ProfileDeleteFlightTest {

    @Mock
    private ApplicationContext context;

    @Test
    public void testConstructFlight() {
        var flight = new ProfileDeleteFlight(new FlightMap(), context);
        List steps = flight.context().getStepClassNames();

        var packageName = "bio.terra.service.profile.flight.delete";

        assertEquals(steps.size(), 5);
        assertEquals(steps.get(0), packageName + ".DeleteProfileMarkUnusedProjects");
        assertEquals(steps.get(1), packageName + ".DeleteProfileDeleteUnusedProjects");
        assertEquals(steps.get(2), packageName + ".DeleteProfileProjectMetadata");
        assertEquals(steps.get(3), packageName + ".DeleteProfileMetadataStep");
        assertEquals(steps.get(4), packageName + ".DeleteProfileAuthzIamStep");
    }

}
