package bio.terra.service.filedata.google.firestore;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.service.iam.IamService;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class FireStoreCleanProject {
    @Autowired
    private MockMvc mvc;

    @MockBean
    private IamService samService;

    @MockBean
    private TestConfiguration testConfiguration;

    @MockBean
    private JsonLoader jsonLoader;

    /*
     * NOTE: This is not a test. This is a tool to delete all FireStore collections from a project.
     * It is written as a JUnit test because that is the simplest way for me to package it today.
     * It only deletes the top-level of documents. Since we don't use documents with sub-collections,
     * that doesn't matter. Just to be clear - it is not general purpose.
     */
    private static final Logger logger = LoggerFactory.getLogger(FireStoreCleanProject.class);

    // To run this tool, follow these steps exactly:
    // 1. Edit the project id
    // 2. Comment out the ignore
    // 3. Run the test
    // 4. Uncomment the ignore
    // 5. Unset the project id
    @Ignore
    @Test
    public void cleanFireStore() throws Exception {
        String projectId = "YOUR DATA PROJECT HERE";
        int batchSize = 500;
        Firestore firestore = FireStoreProject.get(projectId).getFirestore();
        for (CollectionReference collection : firestore.listCollections()) {
            logger.info("Deleting collection " + collection.getId());
            int batchCount = 0;
            int visited;
            do {
                visited = 0;
                ApiFuture<QuerySnapshot> future = collection.limit(batchSize).get();
                List<QueryDocumentSnapshot> documents = future.get().getDocuments();
                batchCount++;
                logger.info("Visiting batch " + batchCount + " of ~" + batchSize + " documents");
                for (QueryDocumentSnapshot document : documents) {
                    document.getReference().delete();
                    visited++;
                }
            } while (visited >= batchSize);
        }
    }

}
