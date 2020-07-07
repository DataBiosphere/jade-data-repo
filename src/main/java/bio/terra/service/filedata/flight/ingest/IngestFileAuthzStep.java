package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class IngestFileAuthzStep implements Step {
    private static final Set<IamRole> readerRoles = EnumSet.of(
        IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.INGESTER);

    private final Dataset dataset;
    private final GcsPdao gcsPdao;
    private final IamService iamService;
    private final AuthenticatedUserRequest userReq;

    public IngestFileAuthzStep(
        Dataset dataset,
        GcsPdao gcsPdao,
        IamService iamService,
        AuthenticatedUserRequest userReq) {
        this.dataset = dataset;
        this.gcsPdao = gcsPdao;
        this.iamService = iamService;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);

        Map<IamRole, String> policyEmails = iamService.getDatasetPolicyEmails(userReq, dataset.getId());
        for (IamRole role : readerRoles) {
            gcsPdao.setAclOnFiles(dataset, Collections.singletonList(fileId), policyEmails.get(role));
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);

        Map<IamRole, String> policyEmails = iamService.getDatasetPolicyEmails(userReq, dataset.getId());
        for (IamRole role : readerRoles) {
            gcsPdao.removeAclOnFiles(dataset, Collections.singletonList(fileId), policyEmails.get(role));
        }

        return StepResult.getStepResultSuccess();
    }
}
