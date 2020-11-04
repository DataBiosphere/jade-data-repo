# Profile Package Readme

This profile package is constructed as separately as possible from the rest of TDR. The objective is to eventually
pull it out into a separate service, as described here:
[Terra Spend Profile Manager Design](https://docs.google.com/document/d/10m7HKCu9-77dAMVldJycMwLuuscPtMDFTmDM1f9Y2pY/edit#)

The immediate work required for TDR production is described here:
[Billing Profile Access Control - Design Note](https://docs.google.com/document/d/1hlEq3qPqRVAeKDXa5KZjtn9Uc8yAQr8F4SA_owQcDZQ/edit#)

There are gaps between what TDR will implement and the complete Spend Profile Manager.

## Implementation Structure and Refactoring Notes

The profile package has its own Controller class (ProfileApiController) that dispatches the (mis-named)
`resources` interface. Again, the idea is that the `resources` interface will become, more or less,
the spend profile manager interface in the fullness of time.

The profile package has a Service class (ProfileService) that implements the operations of the controller.
For now, operation of TDR that interact with profiles (create dataset, create snapshot, ingest files)
will call the Service class directly. In the future, we can replace the Service class implementation with
one that calls the Spend Profile Manager.

Similarly, the update operation in the profile service will directly call TDR methods to perform the billing
account update. When we refactor into Spend Profile Manager, those will become REST API calls from SPM into
TDR.

The profile Service class calls out to TDR's IamService to manage permissions on the spend profile resource.
That code will have to be refactored when we move this package into the Spend Profile Manager, but it is a waste
to duplicate the code with TDR.

The profile package has a DAO class (ProfileDao) that manages a single database table that comprises the
profile collection. Other TDR resource tables refer to to profiles by profile identifier, but there are
no queries that join across the profile table and other tables.

The profile package contains the GoogleBillingUtils module that provides utility methods to interact with
the Google Cloud Billing API. That code should eventually be refactored into a library where it can be
shared across Terra components.

For convenience when running inside TDR, the profile package uses the AuthenticatedUserRequest class.
It holds the user email and access token. When billing profile code is extracted from TDR,
it may or may not want to continue using that class.

The profile linking operations are the operations that might create a project:
 * create dataset
 * create snapshot
 * ingest files

We use the Service class' `authorizeLinking` endpoint to:
 # Ensure that the user doing the operation has `link` permission on the billing profile
 # Validate the billing profile.

There are two things that make the billing profile valid. First, the billing account must be enabled.
Second, some owner of the billing profile must have `billing.resourceAssociations.create` permission
on the billing account. We make that check even if we do not need to create a new project, to ensure
that we are not trying to incur new costs on an unusable or unauthorized billing account.

