/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import java.util.Arrays;

public enum CommandType {
    COMPOSE("compose"),
    INITIALIZE("initialize"),
    PLAN("plan"),
    RESET("reset"),
    RECONFIGURE("reconfigure"),
    RESOURCES("resources"),
    VALIDATE("validate"),
    ;

    private final String _token;

    CommandType(final String value) { _token = value; }

    public String getToken() { return _token; }

    public static CommandType get(
        final String token
    ) {
        return Arrays.stream(values()).filter(ct -> ct.getToken().equals(token)).findFirst().orElse(null);
    }
}
