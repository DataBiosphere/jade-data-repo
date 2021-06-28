package bio.terra.flight;

import bio.terra.common.category.Unit;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

@Category(Unit.class)
public class ProfileCreateFlightTest {

    @Mock
    ApplicationContext context;

    @Mock
    ProfileService profileService;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConstructFlight() {
        when(context.getBean(eq("profileService"))).thenReturn(profileService);

        ProfileCreateFlight flight = new ProfileCreateFlight(new FlightMap(), context);
        List steps = flight.context().getStepClassNames();

        var packageName = "bio.terra.service.profile.flight.create";

        assertEquals(steps.get(0), packageName + ".CreateProfileMetadataStep");
        assertEquals(steps.get(1), packageName + ".CreateProfileVerifyAccountStep");
        assertEquals(steps.get(2), packageName + ".CreateProfileVerifyDeployedApplicationStep");
        assertEquals(steps.get(3), packageName + ".CreateProfileAuthzIamStep");
    }

}
