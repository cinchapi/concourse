# Authentication

Concourse uses a username and password combination to authenticate every user. For every request, Concourse requires the user's identity to be successfully verified. 

When connecting to Concourse via the a driver, the REST API or the shell, you must initially provide a username and password. Those credentials are transparently exchanged for an `access token` that is automatically used on subsequent requests to verify your identity.

## Access Tokens
Concourse issues access tokens in exchange for a valid username and password combination during a login request. The access tokens themselves contain no identifiable information about the users they represent but are associated with a user within a secure enclave of Concourse Server.

Access Tokens are temporary and non-persistent. They automatically expire after 24 hours or when Concourse Server shutsdown, whichever is sooner.

TODO: add note that some drivers will automatically renew tokens by keeping credentials client side