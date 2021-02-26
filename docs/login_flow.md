# Login Flow

![login_flow_sequence_diagram](diagrams/login_flow.png)

1.  The user calls `LogIn`.
1.  The API server generates a random `login_token` and nonce.
1.  " stores these in a database for later lookup by `login_token`.
1.  " builds an authentication request URI using these parameters, with the
    `login_token` as the `state` parameter.
1.  " returns the `login_token` and authentication URI in `LogIn` response.
1.  The user navigates to the returned authorization URI and signs in and/or
    grants consent.
1.  " is redirected to the OM API server's `redirect_uri` with the `code` and
    `state` parameters.
1.  The API server's `redirect_uri` endpoint verifies the authentication
    response and updates the database entry for the `login_token` with the
    authorization code.
1.  The user calls `ExchangeLoginToken`, passing the `login_token` value.
1.  The OM API server looks up the database entry for that `login_token`
1.  " makes a token request to the OP.
1.  " verifies the ID Token.
1.  " deletes the database entry for the `login_token`.
1.  " returns the ID Token in the `ExchangeLoginToken` response.
