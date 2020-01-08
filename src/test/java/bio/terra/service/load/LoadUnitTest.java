package bio.terra.service.load;

import bio.terra.common.category.Unit;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class LoadUnitTest {
    private static final String tag1 = "myLoadTag1";
    private static final String tag2 = "myLoadTag2";
    private static final String flight1 = "myFlightId1";
    private static final String flight2 = "myFlightId2";

    @Autowired
    private LoadService loadService;

    @Test
    public void loadLocKTest() throws Exception {
        loadService.lockLoad(tag1, flight1);
        // Relock of the same (tag, flight) should work
        loadService.lockLoad(tag1, flight1);
        loadService.lockLoad(tag2, flight2);
        loadService.unlockLoad(tag2, flight2);
        loadService.unlockLoad(tag1, flight1);
        // Duplicate unlock should work
        loadService.unlockLoad(tag1, flight1);
    }

    @Test(expected = LoadLockedException.class)
    public void alreadyLockedTest() throws Exception {
        loadService.lockLoad(tag1, flight1);
        loadService.lockLoad(tag1, flight2);
    }

    @Test(expected = LoadLockedException.class)
    public void cannotUnlockTest() throws Exception {
        loadService.lockLoad(tag1, flight1);
        loadService.unlockLoad(tag1, flight2);
    }

    @Test
    public void computeLoadTagTest() throws Exception {
        String loadTag = loadService.computeLoadTag(null);
        assertThat("generated load tag", loadTag, startsWith("load-at-"));
        loadTag = loadService.computeLoadTag(tag1);
        assertThat("pass through load tag", loadTag, equalTo(tag1));
    }

    @Test
    public void getLoadTagTest() throws Exception {
        // Should get tag from working map
        FlightMap inputParams = new FlightMap();
        FlightContext flightContext = new FlightContext(inputParams, null, null);
        FlightMap workingMap = flightContext.getWorkingMap();
        workingMap.put(LoadMapKeys.LOAD_TAG, tag1);

        String loadTag = loadService.getLoadTag(flightContext);
        assertThat("working map load tag", loadTag, equalTo(tag1));

        // Should get from input Params
        FlightMap inputParams1 = new FlightMap();
        inputParams1.put(LoadMapKeys.LOAD_TAG, tag1);
        flightContext = new FlightContext(inputParams1, null, null);
        workingMap = flightContext.getWorkingMap();
        workingMap.put(LoadMapKeys.LOAD_TAG, tag2);

        loadTag = loadService.getLoadTag(flightContext);
        assertThat("input params load tag", loadTag, equalTo(tag1));
    }

    @Test(expected = LoadLockFailureException.class)
    public void getLoadTagFailTest() throws Exception {
        FlightMap inputParams = new FlightMap();
        FlightContext flightContext = new FlightContext(inputParams, null, null);
        loadService.getLoadTag(flightContext);
    }
}
