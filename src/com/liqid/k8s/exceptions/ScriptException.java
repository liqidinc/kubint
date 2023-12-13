/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

/**
 * Base class for all ScriptException entities
 * TODO I'm not sure we really need this
 */
public abstract class ScriptException extends Exception {

    public ScriptException(final String message) {
        super(message);
    }
}
