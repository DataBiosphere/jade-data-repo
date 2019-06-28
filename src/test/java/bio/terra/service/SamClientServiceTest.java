package bio.terra.service;

import bio.terra.category.Unit;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.model.sam.CreateResourceCorrectRequest;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
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
@Category(Unit.class)
public class SamClientServiceTest {

    @MockBean
    private ResourcesApi samResourceApi;

    @Autowired
    private SamClientService sam;

    @Test(expected = ApiException.class)
    public void testCreateStudyResourceException() throws Exception {
        UUID studyId = UUID.randomUUID();
        // TODO this code below is not mocked correctly
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .createResource(eq(SamClientService.ResourceType.STUDY.toString()), any());
        sam.createStudyResource(new AuthenticatedUserRequest("blah", "blah"), studyId);
    }

    @Test(expected = ApiException.class)
    public void testCreateDataSnapshotResourceException() throws Exception {
        UUID dataSnapshotId = UUID.randomUUID();
        // TODO this code below is not mocked correctly
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .createResource(eq(SamClientService.ResourceType.DATASET.toString()), any());
        Optional<List<String>> readerList = Optional.of(Collections.singletonList("email@email.com"));
        sam.createDataSnapshotResource(new AuthenticatedUserRequest("blah", "blah"), dataSnapshotId, readerList);
    }

    @Test(expected = ApiException.class)
    public void testCreateDataSnapshotResourceExceptionWithReaders() throws Exception {
        UUID dataSnapshotId = UUID.randomUUID();
        Optional<List<String>> readersList = Optional.of(Collections.singletonList("email@email.com"));
        CreateResourceRequest createResourceRequest = new CreateResourceRequest();
        CreateResourceCorrectRequest createResourceCorrectRequest = new CreateResourceCorrectRequest();
        createResourceCorrectRequest.addPoliciesItem(
            SamClientService.DataRepoRole.READER.toString(),
            sam.createAccessPolicy(SamClientService.DataRepoRole.READER.toString(), readersList.get()));

        // TODO this code below is not mocked correctly
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .createResource(eq(SamClientService.ResourceType.DATASET.toString()), eq(createResourceRequest));

        sam.createDataSnapshotResource(new AuthenticatedUserRequest("blah", "blah"), dataSnapshotId, readersList);
    }

    @Test(expected = ApiException.class)
    public void testDeleteStudyResourceException() throws Exception {
        UUID studyId = UUID.randomUUID();
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .deleteResource(eq(SamClientService.ResourceType.STUDY.toString()), eq(studyId.toString()));
        sam.deleteStudyResource(new AuthenticatedUserRequest("blah", "blah"), studyId);
    }

    @Test(expected = ApiException.class)
    public void testDeleteDataSnapshotResourceException() throws Exception {
        UUID dataSnapshotId = UUID.randomUUID();
        willThrow(new ApiException("test"))
            .given(samResourceApi)
            .deleteResource(eq(SamClientService.ResourceType.DATASET.toString()), eq(dataSnapshotId.toString()));
        sam.deleteDataSnapshotResource(new AuthenticatedUserRequest("blah", "blah"), dataSnapshotId);
    }
}
