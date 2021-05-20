package bio.terra.service.profile;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.constraints.NotNull;
import java.util.regex.Pattern;

@Component
public class ProfileRequestValidator implements Validator {

    // This is specific to Google's billing account id representation. This will likely need to be split into cloud
    // specific subclasses tagged with profiles.
    private static final String VALID_BILLING_ACCOUNT_ID_REGEX = "[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}";

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }

    @Override
    public void validate(@NotNull Object target, Errors errors) {
        if (target != null && target instanceof BillingProfileRequestModel) {
            BillingProfileRequestModel billingProfileRequestModel = (BillingProfileRequestModel) target;
            String billingAccountId = billingProfileRequestModel.getBillingAccountId();
            if (billingAccountId == null || !isValidAccountId(billingAccountId)) {
                errors.rejectValue("billingAccountId",
                    "The id must be 3 sets of 6 capitalized alphanumeric characters separated by dashes");
            }
            if (billingProfileRequestModel.getId() == null || StringUtils.isEmpty(billingProfileRequestModel.getId())) {
                errors.rejectValue("id",
                    "The billing profile id must be specified");
            }
            isValidCloudPlatform(billingProfileRequestModel, errors);
        }
    }

    public static boolean isValidAccountId(String billingAccountId) {
        return Pattern.matches(VALID_BILLING_ACCOUNT_ID_REGEX, billingAccountId);
    }

    public static void isValidCloudPlatform(BillingProfileRequestModel billingProfileRequestModel, Errors errors) {
        if (billingProfileRequestModel.getCloudPlatform() == CloudPlatform.AZURE) {
            String errorCode = "For Azure, a valid UUID `%s` must be provided";
            if (billingProfileRequestModel.getTenantId() == null) {
                errors.rejectValue("tenant", String.format(errorCode, "tenant"));
            }
            if (billingProfileRequestModel.getSubscriptionId() == null) {
                errors.rejectValue("subscription", String.format(errorCode, "subscription"));
            }
            if (billingProfileRequestModel.getResourceGroupId() == null) {
                errors.rejectValue("resourceGroup", String.format(errorCode, "resourceGroup"));
            }
        } else {
            // GCP is the default cloud platform, so there should be no Azure info from here on.
            String errorCode = "For GCP, no Azure information should be provided";
            if (billingProfileRequestModel.getTenantId() != null) {
                errors.rejectValue("tenant", errorCode);
            }
            if (billingProfileRequestModel.getSubscriptionId() != null) {
                errors.rejectValue("subscription", errorCode);
            }
            if (billingProfileRequestModel.getResourceGroupId() != null) {
                errors.rejectValue("resourceGroup", errorCode);
            }
        }
    }
}
