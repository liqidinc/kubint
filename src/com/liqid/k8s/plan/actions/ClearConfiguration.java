/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

public class ClearConfiguration extends Action {

    public ClearConfiguration() {
        super(ActionType.CLEAR_CONFIGURATION);
    }

    @Override
    public void checkParameters() {}

    @Override
    public void perform(
        final ExecutionContext context
    ) throws LiqidException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        context.getLiqidClient().clearGroups();

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Clear Liqid Configuration";
    }
}
