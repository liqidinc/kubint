/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

/**
 * Base class for all ScriptException entities
 * There is no immediate need for the KubInt exception classes to have this base class,
 * but it is here in case the need eventually arises.
 */
public abstract class ScriptException extends Exception {

    public ScriptException(final String message) {
        super(message);
    }
}
