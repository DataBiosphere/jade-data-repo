package bio.terra.app.controller;


import bio.terra.model.ErrorModel;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.WebUtils;

import java.util.List;

import static java.util.stream.Collectors.toList;

@ControllerAdvice
public class ApiValidationExceptionHandler extends ResponseEntityExceptionHandler {

    /**
     * Since the default {@link ResponseEntityExceptionHandler#handleExceptionInternal}
     * method is often passed a null body, we override it and substitute the exception's error message
     * into an ErrorModel and return that in the response instead.
     */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
        Exception ex, @Nullable Object body, HttpHeaders headers, HttpStatus status, WebRequest request) {

        if (HttpStatus.INTERNAL_SERVER_ERROR == status) {
            request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, ex, WebRequest.SCOPE_REQUEST);
        }

        Object responseBody = body;
        if (responseBody == null) {
            responseBody = new ErrorModel().message(status + " - see error details")
                .addErrorDetailItem(ex.getMessage());
        }

        return new ResponseEntity<>(responseBody, headers, status);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
        MethodArgumentNotValidException ex,
        HttpHeaders headers,
        HttpStatus status,
        WebRequest request
    ) {
        BindingResult bindingResult = ex.getBindingResult();

        List<String> errorDetails = bindingResult
            .getFieldErrors()
            .stream()
            .map(this::formatFieldError)
            .collect(toList());

        ErrorModel errorModel = new ErrorModel()
            .message("Validation errors - see error details")
            .errorDetail(errorDetails);

        return new ResponseEntity<>(errorModel, HttpStatus.BAD_REQUEST);
    }

    private String formatFieldError(FieldError error) {
        StringBuilder builder = new StringBuilder()
            .append(String.format("%s: '%s'", error.getField(), error.getCode()));
        String defaultMessage = error.getDefaultMessage();
        if (StringUtils.isNotEmpty(defaultMessage)) {
            builder.append(String.format(" (%s)", defaultMessage));
        }
        return builder.toString();
    }
}
