/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

public abstract class ScriptException extends Exception {

    public ScriptException(final String message) {
        super(message);
    }
}
