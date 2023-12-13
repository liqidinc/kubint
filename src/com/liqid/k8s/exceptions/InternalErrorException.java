/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

/**
 * Indicates that we've discovered a programming error - some situation which should not be.
 */
public class InternalErrorException extends ScriptException {

    public InternalErrorException(final String message) {
        super(message);
    }
}
