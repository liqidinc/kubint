/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.liqid.k8s.exceptions.ConfigurationDataException;

import java.util.Base64;

public class CredentialMangler {

    private final String _mangledString;
    private final String _username;
    private final String _password;

    public CredentialMangler(
        final String username,
        final String password
    ) {
        _username = username;
        _password = password;
        String temp = username;
        if (password != null) {
            temp += ":" + password;
        }
        _mangledString = Base64.getEncoder().encodeToString(temp.getBytes());
    }

    public CredentialMangler(
        final String mangledString
    ) throws ConfigurationDataException {
        _mangledString = mangledString;
        var bytes = Base64.getDecoder().decode(_mangledString);
        var composite = new String(bytes);
        var split = composite.split(":");
        if (split.length > 2) {
            throw new ConfigurationDataException("Malformed decoded Base64 string detected while obtaining Liqid credentials");
        }
        _username = split[0];
        _password = split.length == 2 ? split[1] : null;
    }

    public String getMangledString() { return _mangledString; }
    public String getUsername() { return _username; }
    public String getPassword() { return _password; }
}
