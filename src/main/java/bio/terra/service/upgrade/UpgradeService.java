package bio.terra.service.upgrade;

import bio.terra.common.exception.NotImplementedException;
import bio.terra.model.UpgradeModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobService;
import bio.terra.service.upgrade.exception.InvalidCustomNameException;
import bio.terra.service.upgrade.flight.UpgradeProfileFlight;
import bio.terra.stairway.Flight;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpgradeService {

    private enum CustomFlight {
        BILLING_PROFILE_PERMISSION(UpgradeProfileFlight.class);

        private final Class<? extends Flight> flightClass;

        CustomFlight(Class<? extends Flight> flightClass) {
            this.flightClass = flightClass;
        }

        public Class<? extends Flight> getFlightClass() {
            return flightClass;
        }
    }

    private final JobService jobService;

    @Autowired
    public UpgradeService(JobService jobService) {
        this.jobService = jobService;
    }

    public String upgrade(UpgradeModel request, AuthenticatedUserRequest user) {
        if (request.getUpgradeType() != UpgradeModel.UpgradeTypeEnum.CUSTOM) {
            throw new NotImplementedException("Upgrade type is not implemented: " + request.getUpgradeType().name());
        }

        CustomFlight customFlight;
        try {
            customFlight = CustomFlight.valueOf(request.getCustomName());
        } catch (NullPointerException ex) {
            throw new InvalidCustomNameException("Custom name is required for custom upgrade type");
        } catch (IllegalArgumentException ex) {
            throw new InvalidCustomNameException("Invalid custom name provided to upgrade: "
                + request.getCustomName());
        }

        return jobService
            .newJob(request.getCustomName(), customFlight.getFlightClass(), request, user)
            .submit();
    }
}
