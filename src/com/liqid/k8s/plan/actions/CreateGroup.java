/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

/**
 * Creates a group with the specified name, in the Liqid Cluster
 */
public class CreateGroup extends Action {

    private String _groupName;

    public CreateGroup() {
        super(ActionType.CREATE_GROUP);
    }

    public CreateGroup setGroupName(final String value) { _groupName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("GroupName", _groupName);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws LiqidException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        var group = context.getLiqidClient().createGroup(_groupName);
        context.getLiqidInventory().notifyGroupCreated(group);

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Create Group " + _groupName + " in the Liqid Cluster";
    }
}
