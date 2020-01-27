package bio.terra.service.load;

import bio.terra.common.category.Unit;
import bio.terra.model.BulkLoadFileModel;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class LoadDaoUnitTest {
    @Autowired
    private LoadDao loadDao;

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
