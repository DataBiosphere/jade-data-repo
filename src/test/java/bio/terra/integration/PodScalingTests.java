package bio.terra.integration;

import bio.terra.clienttests.KubernetesClientUtils;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.Names;
import bio.terra.model.*;
import bio.terra.service.iam.IamRole;
import io.kubernetes.client.openapi.ApiException;
import org.junit.After;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class PodScalingTests extends UsersBase {

    private static Logger logger = LoggerFactory.getLogger(PodScalingTests.class);

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfiguration;

    private DatasetSummaryModel datasetSummaryModel;
    private String datasetId;
    private String profileId;
    private KubernetesClientUtils kubeUtils;
    // TODO: Pass this in through config
    private String namespace = "sh";

    @Before
    public void setup() throws Exception {
        super.setup();
        profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
        datasetSummaryModel = dataRepoFixtures.createDataset(steward(), "file-acl-test-dataset.json");
        datasetId = datasetSummaryModel.getId();
        logger.info("created dataset " + datasetId);
        dataRepoFixtures.addDatasetPolicyMember(
            steward(), datasetSummaryModel.getId(), IamRole.CUSTODIAN, custodian().getEmail());
        KubernetesClientUtils.getKubernetesClientObject();
        KubernetesClientUtils.scaleDeployment(namespace, 1);
        kubeUtils = new KubernetesClientUtils();
    }

    @After
    public void tearDown() throws Exception {
        if (datasetId != null) {
            dataRepoFixtures.deleteDataset(steward(), datasetId);
        }
        KubernetesClientUtils.scaleDeployment(namespace, 1);
    }

    public interface KubernetesAdjustmentInterface {
        void adjustDeployment() throws ApiException;
    }

    // Test scaling pods up and down during a file ingest
    @Test
    public void scalePodsTest() throws Exception {
        KubernetesAdjustmentInterface scalePodModel = new ScaleUpAndDown();
        longFileLoadTest(scalePodModel);
    }

    class ScaleUpAndDown implements KubernetesAdjustmentInterface {
        public void adjustDeployment() throws ApiException {
            KubernetesClientUtils.scaleDeployment(namespace, 2);
            KubernetesClientUtils.scaleDeployment(namespace, 1);
        }
    }


    // Test killing pods and letting them come back up during a file ingest
    @Test
    public void killPodsTest() throws Exception {
        KubernetesAdjustmentInterface killPodModel = new DeletePods();
        longFileLoadTest(killPodModel);
    }

    class DeletePods implements KubernetesAdjustmentInterface {
        public void adjustDeployment() throws ApiException {
            kubeUtils.killPod(namespace);
        }
    }



    // The purpose of this test is to have a long-running workload that completes successfully
    // while we delete pods and have them recover.
    // Marked ignore for normal testing.
    private void longFileLoadTest(PodScalingTests.KubernetesAdjustmentInterface kubeCallback) throws Exception {
        // TODO: want this to run about 5 minutes on 2 DRmanager instances. The speed of loads is when they are
        //  not local is about 2.5GB/minutes. With a fixed size of 1GB, each instance should do 2.5 files per minute,
        //  so two instances should do 5 files per minute. To run 5 minutes we should run 25 files.
        //  (There are 25 files in the directory, so if we need more we should do a reuse scheme like the fileLoadTest)
        final int filesToLoad = 25;

        String loadTag = Names.randomizeName("longtest");

        BulkLoadArrayRequestModel arrayLoad = new BulkLoadArrayRequestModel()
            .profileId(profileId)
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

        logger.info("longFileLoadTest loading " + filesToLoad + " files into dataset id " + datasetId);

        for (int i = 0; i < filesToLoad; i++) {
            String tailPath = String.format("/fileloadscaletest/file1GB-%02d.txt", i);
            String sourcePath = "gs://jade-testdata-uswestregion" + tailPath;
            String targetPath = "/" + loadTag + tailPath;

            BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
            model.description("bulk load file " + i)
                .sourcePath(sourcePath)
                .targetPath(targetPath);
            arrayLoad.addLoadArrayItem(model);
        }

        // kubeCallback.adjustDeployment();
        TestConfiguration.User stewardUser = steward();
        boolean alreadyRun = false;
        // INITIAL REQUEST //
        DataRepoResponse<JobModel> launchResponse = null;
        IOException lastException = null;
        IllegalStateException illegalStateException = null;
        DataRepoResponse<BulkLoadArrayResultModel> response = null;
        for(int i = 0; i < 4; i++) {
            try {
                launchResponse = dataRepoFixtures.buildBulkLoadArrayRequest(
                    stewardUser, datasetId, arrayLoad);
                // MANIPULATE KUBERNETES DEPLOY //
                alreadyRun = dataRepoClient.pollForResponse(stewardUser, launchResponse, 5);
                if (!alreadyRun) {
                    // kill pod/scale pod/etc
                    // poll again
                    // do something else
                    logger.info("not yet complete");
                    kubeUtils.killPod(namespace);

                    // dataRepoClient.pollForResponse(stewardUser, launchResponse, 5);

                }
                 response =
                    dataRepoClient.waitForResponse(stewardUser, launchResponse, BulkLoadArrayResultModel.class);
                break;
            } catch (IOException ex) {
                logger.info("Caught IOException. Sleeping then retry.");
                TimeUnit.SECONDS.sleep(30);
                lastException = ex;
            } catch (IllegalStateException ex) {
                logger.info("Caught IOException. Sleeping then retry.");
                TimeUnit.SECONDS.sleep(30);
                illegalStateException  = ex;
            }
        }
        if (response == null && lastException != null) {
            throw lastException;
        }
        if (response == null && illegalStateException != null) {
            throw illegalStateException;
        }



        // actually get response

        // } catch (IOException ex) {
        // the kubeCallback could kill kubernetes pods causing the request to fail.
        // This code gives the pods a chance to come back up retry the request
        // IOException lastException = null;
        // logger.info("Exception caught; Sleep for 30 seconds and retrying.");
        // TimeUnit.SECONDS.sleep(30);}

        BulkLoadArrayResultModel result = dataRepoFixtures.checkBulkArrayRequestStatus(response, lastException);

        // BulkLoadArrayResultModel result = dataRepoFixtures.bulkLoadArray(steward(), datasetId, arrayLoad);
        BulkLoadResultModel loadSummary = result.getLoadSummary();
        logger.info("Total files    : " + loadSummary.getTotalFiles());
        logger.info("Succeeded files: " + loadSummary.getSucceededFiles());
        logger.info("Failed files   : " + loadSummary.getFailedFiles());
        logger.info("Not Tried files: " + loadSummary.getNotTriedFiles());
    }

}
