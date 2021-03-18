package bio.terra.integration;

import bio.terra.common.category.Integration;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.UUID;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class SnapshotDeleteTest extends UsersBase {

    private static Logger logger = LoggerFactory.getLogger(SnapshotDeleteTest.class);

    @Autowired
    private SnapshotService snapshotService;

    @Autowired
    private BigQueryPdao bigQueryPdao;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private FireStoreDao fileDao;

    @Autowired
    private FireStoreDependencyDao dependencyDao;

    private String datasetId;
    private String snapshotId;
    private String profileId;

    @Before
    public void setup() throws Exception {
        super.setup();
        profileId = "390e7a85-d47f-4531-b612-165fc977d3bd";
        datasetId = "ecb5601e-9026-428c-b49d-3c5f1807ecb7";
        snapshotId = "4122d403-8d70-4922-ab6e-7db13e44a9a5";
        /*dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetSummaryModel.getId(), IamRole.CUSTODIAN, custodian().getEmail());*/
    }

    @Test
    public void PrimaryDataStepSnapshotDelete() throws InterruptedException {
        Snapshot snapshot = snapshotService.retrieve(UUID.fromString(snapshotId));

        bigQueryPdao.deleteSnapshot(snapshot);
        // 1. determines the BQ Dataset to delete
        // 2. Determines the sources of the snapshot
        // List<SnapshotSource> sources = snapshot.getSnapshotSources();
        // "SELECT id, dataset_id, asset_id FROM snapshot_source WHERE snapshot_id = :snapshot_id

        // 3. For each source, get dataset, and delete View ACLs
        //if (sources.size() > 0) {
        //    String datasetName = sources.get(0).getDataset().getName();
        //    String datasetBqDatasetName = prefixName(datasetName);
        //    deleteViewAcls(datasetBqDatasetName, snapshot, projectId);
        //}


        // Remove snapshot file references from the underlying datasets
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
            dependencyDao.deleteSnapshotFileDependencies(
                dataset,
                snapshotId);
        }
        fileDao.deleteFilesFromSnapshot(snapshot);
    }

}
