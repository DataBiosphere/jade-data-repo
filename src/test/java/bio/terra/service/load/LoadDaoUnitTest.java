package bio.terra.service.load;

import bio.terra.common.category.Unit;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.load.exception.LoadLockedException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class LoadDaoUnitTest {
    private final Logger logger = LoggerFactory.getLogger(LoadDaoUnitTest.class);

    @Autowired
    private LoadDao loadDao;

    @Autowired
    private ConfigurationService configService;

    @Test
    public void loadFilesTest() throws Exception {
        UUID loadId = populateFiles(8);

        // First set of candidates
        LoadCandidates candidates = loadDao.findCandidates(loadId, 3);
        testLoadCandidates(candidates, 0, 0, 3);

        List<LoadFile> loadSet1 = candidates.getCandidateFiles();

        loadDao.setLoadFileSucceeded(loadId, loadSet1.get(0).getTargetPath(), "fileidA");
        loadDao.setLoadFileFailed(loadId, loadSet1.get(1).getTargetPath(), "failureB");
        loadDao.setLoadFileRunning(loadId, loadSet1.get(2).getTargetPath(), "flightC");

        // Second set of candidates - set prior running to succeeded
        candidates = loadDao.findCandidates(loadId, 3);
        testLoadCandidates(candidates, 1, 1, 3);
        List<LoadFile> loadSet2 = candidates.getCandidateFiles();

        loadDao.setLoadFileSucceeded(loadId, loadSet1.get(2).getTargetPath(), "fileidC");

        loadDao.setLoadFileRunning(loadId, loadSet2.get(0).getTargetPath(), "flightD");
        loadDao.setLoadFileRunning(loadId, loadSet2.get(1).getTargetPath(), "flightE");
        loadDao.setLoadFileRunning(loadId, loadSet2.get(2).getTargetPath(), "flightF");

        // Third set of candidates - set all 3 prior to failed
        candidates = loadDao.findCandidates(loadId, 3);
        testLoadCandidates(candidates, 1, 3, 2);
        List<LoadFile> loadSet3 = candidates.getCandidateFiles();

        loadDao.setLoadFileFailed(loadId, loadSet2.get(0).getTargetPath(), "errorD");
        loadDao.setLoadFileFailed(loadId, loadSet2.get(1).getTargetPath(), "errorE");
        loadDao.setLoadFileFailed(loadId, loadSet2.get(2).getTargetPath(), "errorF");

        loadDao.setLoadFileRunning(loadId, loadSet3.get(0).getTargetPath(), "flightG");
        loadDao.setLoadFileRunning(loadId, loadSet3.get(1).getTargetPath(), "flightH");

        // No more candidates, but things are still running
        candidates = loadDao.findCandidates(loadId, 3);
        testLoadCandidates(candidates, 4, 2, 0);

        loadDao.setLoadFileSucceeded(loadId, loadSet3.get(0).getTargetPath(), "fileidG");
        loadDao.setLoadFileSucceeded(loadId, loadSet3.get(1).getTargetPath(), "fileidH");

        // No more candidates and nothing running; this would be the bulk load completed state
        candidates = loadDao.findCandidates(loadId, 3);
        testLoadCandidates(candidates, 4, 0, 0);

        // clean up after ourselves - check that we properly find nothing
        loadDao.cleanFiles(loadId);
        candidates = loadDao.findCandidates(loadId, 3);
        testLoadCandidates(candidates, 0, 0, 0);
    }

    @Test
    public void serialLockTest() throws Exception {
        final String loadTag = "serialLoadTag";
        final String flightX = "flightIdX";
        final String flightY = "flightIdY";

        boolean xlocks = tryLockLoad(loadTag, flightX);
        assertTrue("x gets lock", xlocks);

        boolean ylocks = tryLockLoad(loadTag, flightY);
        assertFalse("y does not get lock", ylocks);

        loadDao.unlockLoad(loadTag, flightX);

        ylocks = tryLockLoad(loadTag, flightY);
        assertTrue("y gets lock", ylocks);
    }

    private boolean tryLockLoad(String loadTag, String flightId) {
        try {
            loadDao.lockLoad(loadTag, flightId);
            return true;
        } catch (LoadLockedException ex) {
            return false;
        }
    }

    @Test
    public void concurrentLockTest() throws Exception {
        // The test calls the loadLock method from two threads. Thread A will read the record first (we give it a
        // 2 second head start) and then will hit the LOAD_LOCK_CONFLICT_STOP_FAULT and pause, waiting for
        // the LOAD_LOCK_CONFLICT_CONTINUE_FAULT to be enabled.
        // Thread B won't stop at the STOP fault - the fault is a one shot count - so it will just run
        // through. Since thread A did the select first, when thread B tries the update, it gets a serialization
        // failure. When thread B completes, the test code enables the CONTINUE fault allowing thread A
        // to continue and make its update to the load tag.
        final String loadTag = "concurrentLoadTag";

        configService.setFault(ConfigEnum.LOAD_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // Initialize the load tag by locking and unlocking it, so neither thread will perform the creation
        // and miss the faults.
        loadDao.lockLoad(loadTag, "initFlight");
        loadDao.unlockLoad(loadTag, "initFlight");

        LoadLockTester loadLockA = new LoadLockTester(loadDao, loadTag, "flightIdA");
        LoadLockTester loadLockB = new LoadLockTester(loadDao, loadTag, "flightIdB");

        Thread threadA = new Thread(loadLockA);

        threadA.start();
        TimeUnit.SECONDS.sleep(2); // Let threadA get to the fault/lookup first
        Thread threadB = new Thread(loadLockB);
        threadB.start();
        threadB.join();
        assertThat("Thread B succeeded", loadLockB.getResult(), equalTo(1));

        configService.setFault(ConfigEnum.LOAD_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);

        threadA.join();
        assertThat("Thread A failed", loadLockA.getResult(), equalTo(2));
    }

    private void testLoadCandidates(LoadCandidates candidates, int failures, int running, int notTried) {
        assertThat("right number of failures", candidates.getFailedLoads(), equalTo(failures));
        assertThat("right number of running", candidates.getRunningLoads().size(), equalTo(running));
        assertThat("right number of not_tried", candidates.getCandidateFiles().size(), equalTo(notTried));
    }

    private UUID populateFiles(int n) {
        Load load = loadDao.lockLoad("myLoadTag", "myFlightId");

        List<BulkLoadFileModel> loadList = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            loadList.add(
                new BulkLoadFileModel()
                    .sourcePath("gs://path" + i)
                    .targetPath("/target/path" + i)
                    .description("number " + i));
        }

        loadDao.populateFiles(load.getId(), loadList);
        return load.getId();
    }

}
