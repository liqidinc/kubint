/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

import com.bearsnake.k8sclient.K8SException;
import com.liqid.sdk.LiqidException;

/**
 * TODO Not sure we need this anymore...
 */
public class ProcessingException extends ScriptException {

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
