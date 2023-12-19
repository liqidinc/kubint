/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

import com.bearsnake.k8sclient.K8SException;
import com.liqid.sdk.LiqidException;

/**
 * Something went wrong and we cannot continue processing.
 * One example of this would be detecting a problem which might otherwise be ignorable,
 * however the force flag was not specified.
 */
public class ProcessingException extends ScriptException {

    public ProcessingException() {
        super("Processing Exception");
    }

    public ProcessingException(final String message) {
        super(message);
    }

    public ProcessingException(
        final LiqidException lex
    ) {
        super(String.format("Caught exception from Liqid SDK:" + lex.getMessage()));
    }

    public ProcessingException(
        final K8SException kex
    ) {
        super(String.format("Caught exception from Kubernetes Client:" + kex.getMessage()));
    }
}
