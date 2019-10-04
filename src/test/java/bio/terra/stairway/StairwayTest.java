package bio.terra.stairway;

import bio.terra.category.StairwayUnit;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

// Stairway class tests - mostly validating error conditions
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(StairwayUnit.class)
public class StairwayTest {
    private Stairway stairway;
    private UserRequestInfo testUser = new UserRequestInfo()
        .subjectId("StairwayUnit")
        .name("stairway@unit.com");

    @Autowired
    private StairwayJdbcConfiguration jdbcConfiguration;

    @Before
    public void setup() {
        stairway = TestUtil.setupStairway(jdbcConfiguration);
    }

    @Test(expected = MakeFlightException.class)
    public void testNullFlightClass() {
        FlightMap flightMap = new FlightMap();
        stairway.submit("nullflightclass", null, flightMap, testUser);
    }

    @Test(expected = MakeFlightException.class)
    public void testNullInputParams() {
        stairway.submit("nullinput", TestFlight.class, null, null);
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadFlightDone() {
        TestUtil.isDone(stairway, "abcdefg");
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadFlightGetResult() {
        stairway.getFlightState("abcdefg");
    }

}
