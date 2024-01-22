/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.plan.ExecutionContext;

public class NoOperationAction extends Action {

    public NoOperationAction() {
        super(ActionType.NO_OPERATION);
    }

    @Override
    public void checkParameters() {}

    @Override
    public void perform(
        final ExecutionContext context
    ) {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);
        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "No Operation";
    }
}
