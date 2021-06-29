package bio.terra.flight;

import bio.terra.common.category.Unit;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.stairway.FlightMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@Category(Unit.class)
public class ProfileCreateFlightTest {

    @Mock
    private ApplicationContext context;

    @Test
    public void testConstructFlight() {
        var flight = new ProfileCreateFlight(new FlightMap(), context);

        var packageName = "bio.terra.service.profile.flight.create";

        assertThat(flight.context().getStepClassNames(), is(List.of(
                packageName + ".CreateProfileMetadataStep",
                packageName + ".CreateProfileVerifyAccountStep",
                packageName + ".CreateProfileVerifyDeployedApplicationStep",
                packageName + ".CreateProfileAuthzIamStep"
        )));
    }

}
