/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.bearsnake.k8sclient.K8SException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;

public abstract class Action {

    private final ActionType _actionType;

    protected Action(
        final ActionType actionType
    ) {
        _actionType = actionType;
    }

    public final ActionType getAction() { return _actionType; }

    protected void checkForNull(
        final String parameterName,
        final Object parameter
    ) throws InternalErrorException {
        if (parameter == null) {
            throw new InternalErrorException(String.format("Internal Error:%s was not set for %s",
                                                           parameterName, this.getClass().getName()));
        }
    }

    public abstract void checkParameters() throws InternalErrorException;

    /**
     * Helpful wrapper to create a full annotation key
     */
    protected String createAnnotationKeyFor(
        final String keySuffix
    ) {
        return String.format("%s/%s", K8S_ANNOTATION_PREFIX, keySuffix);
    }

    public abstract void perform(
        final ExecutionContext context
    ) throws InternalErrorException, K8SException, LiqidException, ProcessingException;
}
