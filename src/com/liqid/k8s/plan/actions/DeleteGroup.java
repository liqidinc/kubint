/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

/**
 * Deletes a group from the Liqid cluster
 */
public class DeleteGroup extends Action {

    private String _groupName;

    public DeleteGroup() {
        super(ActionType.DELETE_GROUP);
    }

    public DeleteGroup setGroupName(final String value) { _groupName = value; return this; }

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

        var group = context.getLiqidInventory()._groupsByName.get(_groupName);
        if (group == null) {
            System.out.printf("INFO:Group %s does not exist in the Liqid Cluster\n", _groupName);
            context.getLogger().trace("%s returning", fn);
            return;
        }

        context.getLiqidClient().deleteGroup(group.getGroupId());
        context.getLiqidInventory().notifyGroupDeleted(group.getGroupId());

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Delete Group " + _groupName + " from Liqid Cluster";
    }
}
