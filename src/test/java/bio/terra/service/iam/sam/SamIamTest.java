package bio.terra.service.iam.sam;

import bio.terra.category.Unit;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willThrow;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class SamIamTest {

    @MockBean
    private ResourcesApi samResourceApi;

    @Autowired
    private SamIam sam;

    private AuthenticatedUserRequest testUser = new AuthenticatedUserRequest()
            .email("blah").subjectId("myId").token(Optional.of("blah"));

    @Test(expected = IamUnauthorizedException.class)
    public void testCreateDatasetResourceException() throws Exception {
        UUID datasetId = UUID.randomUUID();
        // TODO this code below is not mocked correctly
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .createResource(ArgumentMatchers.eq(IamResourceType.DATASET.toString()), any());
        sam.createDatasetResource(testUser, datasetId);
    }

    @Test(expected = IamUnauthorizedException.class)
    public void testCreateSnapshotResourceException() throws Exception {
        UUID snapshotId = UUID.randomUUID();
        // TODO this code below is not mocked correctly
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .createResource(eq(IamResourceType.DATASNAPSHOT.toString()), any());
        List<String> readerList = Collections.singletonList("email@email.com");
        sam.createSnapshotResource(testUser, snapshotId, readerList);
    }

    @Test(expected = IamUnauthorizedException.class)
    public void testCreateSnapshotResourceExceptionWithReaders() throws Exception {
        UUID snapshotId = UUID.randomUUID();
        List<String> readersList = Collections.singletonList("email@email.com");
        CreateResourceRequest createResourceRequest = new CreateResourceRequest();
        CreateResourceCorrectRequest createResourceCorrectRequest = new CreateResourceCorrectRequest();
        createResourceCorrectRequest.addPoliciesItem(
            IamRole.READER.toString(),
            sam.createAccessPolicy(IamRole.READER.toString(), readersList));

        // TODO this code below is not mocked correctly
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .createResource(eq(IamResourceType.DATASNAPSHOT.toString()), eq(createResourceRequest));

        sam.createSnapshotResource(testUser, snapshotId, readersList);
    }

    @Test(expected = IamUnauthorizedException.class)
    public void testDeleteDatasetResourceException() throws Exception {
        UUID datasetId = UUID.randomUUID();
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .deleteResource(eq(IamResourceType.DATASET.toString()), eq(datasetId.toString()));
        sam.deleteDatasetResource(testUser, datasetId);
    }

    @Test(expected = IamUnauthorizedException.class)
    public void testDeleteSnapshotResourceException() throws Exception {
        UUID snapshotId = UUID.randomUUID();
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .deleteResource(eq(IamResourceType.DATASNAPSHOT.toString()), eq(snapshotId.toString()));
        sam.deleteSnapshotResource(testUser, snapshotId);
    }
}
