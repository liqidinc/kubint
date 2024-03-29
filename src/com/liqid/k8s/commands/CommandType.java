/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import java.util.Arrays;

public enum CommandType {
    ADOPT("adopt"),
    ANNOTATE("annotate"),
    COMPOSE("compose"),
    INITIALIZE("initialize"),
    LINK("link"),
    NODES("nodes"),
    RELEASE("release"),
    RESET("reset"),
    RESOURCES("resources"),
    UNLINK("unlink")
    ;

    private final String _token;

    CommandType(final String value) {_token = value; }

    public String getToken() { return _token; }

    public static CommandType get(
        final String token
    ) {
        return Arrays.stream(values()).filter(ct -> ct.getToken().equals(token)).findFirst().orElse(null);
    }
}
