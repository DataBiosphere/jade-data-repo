package bio.terra.flight;

import bio.terra.common.category.Unit;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(Unit.class)
public class ProfileUpdateFlightTest {

    @Mock
    ApplicationContext context;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConstructFlight() {
        var flight = new ProfileUpdateFlight(new FlightMap(), context);
        List steps = flight.context().getStepClassNames();

        var packageName = "bio.terra.service.profile.flight.update";

        assertEquals(steps.size(), 4);
        assertEquals(steps.get(0), packageName + ".UpdateProfileRetrieveExistingProfileStep");
        assertEquals(steps.get(1), packageName + ".UpdateProfileMetadataStep");
        assertEquals(steps.get(2), packageName + ".UpdateProfileVerifyAccountStep");
        assertEquals(steps.get(3), packageName + ".UpdateProfileUpdateGCloudProject");
    }
}
