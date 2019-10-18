package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileRequestModel;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.constraints.NotNull;
import java.util.regex.Pattern;

@Component
public class BillingProfileRequestValidator implements Validator {

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
            if (!isValidAccountId(billingAccountId)) {
                errors.rejectValue("billingAccountId",
                    "The id must be 3 sets of 6 capitalized alphanumeric characters separated by dashes");
            }
        }
    }

    public static boolean isValidAccountId(String billingAccountId) {
        return Pattern.matches(VALID_BILLING_ACCOUNT_ID_REGEX, billingAccountId);
    }
}
