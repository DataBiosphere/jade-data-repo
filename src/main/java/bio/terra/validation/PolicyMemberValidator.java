package bio.terra.validation;

import bio.terra.model.PolicyMemberRequest;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.constraints.NotNull;

@Component
public class PolicyMemberValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }

    private void ValidateEmail(String email, Errors errors) {
    if (email == null || email.isEmpty())
        errors.rejectValue("email", "EmailNotSupplied");
    else if (!ValidationUtils.isValidEmail(email))
        errors.rejectValue("email", "InvalidEmailFormat");
    }

    @Override
    public void validate(@NotNull Object target, Errors errors) {
        if (target != null && target instanceof PolicyMemberRequest) {
            PolicyMemberRequest policyMemberReq = (PolicyMemberRequest) target;
            ValidateEmail(policyMemberReq.getEmail(), errors);
        }
    }

}
