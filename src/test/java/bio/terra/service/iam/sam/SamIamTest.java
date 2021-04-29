package bio.terra.service.iam.sam;


import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Category(Unit.class)
public class SamIamTest {

    @Mock
    private SamConfiguration samConfig;
    @Mock
    private ConfigurationService configurationService;
    @Mock
    ResourcesApi samApi = mock(ResourcesApi.class);
    @Mock
    StatusApi samStatusApi = mock(StatusApi.class);
    @Mock
    AuthenticatedUserRequest userReq;

    SamIam samIam;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        samIam = spy(new SamIam(samConfig, configurationService));
        when(userReq.getRequiredToken()).thenReturn("some_token");
        when(configurationService.getParameterValue(ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS))
            .thenReturn(1);
        when(configurationService.getParameterValue(ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS))
            .thenReturn(0);
        when(configurationService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS))
            .thenReturn(1);
        // Mock out samApi and samStatusApi in individual tests
        doAnswer(a -> samApi).when(samIam).samResourcesApi(anyString());

    }

    @Test
    public void testExtractErrorMessageSimple() {
        ErrorReport errorReport = new ErrorReport()
            .message("FOO")
            .source("sam");

        assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO");
    }

    @Test
    public void testExtractErrorMessageSimpleNested() {
        ErrorReport errorReport = new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport()
                .message("BAR")
                .source("sam"));

        assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO: BAR");
    }

    @Test
    public void testExtractErrorMessageDeepNested() {
        ErrorReport errorReport = new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport()
                .message("BAR")
                .source("sam")
                .addCausesItem(
                    new ErrorReport()
                        .message("BAZ1")
                        .source("sam")
                )
                .addCausesItem(
                    new ErrorReport()
                        .message("BAZ2")
                        .source("sam")
                        .addCausesItem(new ErrorReport()
                            .message("QUX")
                            .source("sam"))
                )
            );

        assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO: BAR: (BAZ1, BAZ2: QUX)");
    }

    @Test
    public void testIgnoresNonUUIDResourceName() throws ApiException, InterruptedException {
        String goodId = UUID.randomUUID().toString();
        String badId = "badUUID";
        when(samApi.listResourcesAndPolicies(IamResourceType.SPEND_PROFILE.getSamResourceName()))
            .thenReturn(Arrays.asList(
                new ResourceAndAccessPolicy().resourceId(goodId),
                new ResourceAndAccessPolicy().resourceId(badId)
            ));

        List<UUID> uuids = samIam.listAuthorizedResources(userReq, IamResourceType.SPEND_PROFILE);
        assertThat(uuids).containsExactly(UUID.fromString(goodId));
    }

}
