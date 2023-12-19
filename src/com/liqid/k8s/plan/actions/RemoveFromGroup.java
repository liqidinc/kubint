/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

public class RemoveFromGroup extends Action {

    private String _groupName;
    private Set<String> _deviceNames;

    public RemoveFromGroup(
    ) {
        super(ActionType.REMOVE_RESOURCES_FROM_GROUP);
    }

    public RemoveFromGroup addDeviceName(final String value) { _deviceNames.add(value); return this; }
    public RemoveFromGroup setDeviceNames(final Collection<String> list) { _deviceNames = new TreeSet<>(list); return this; }
    public RemoveFromGroup setGroupName(final String value) { _groupName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("GroupName", _groupName);
        checkForNull("DeviceNames", _deviceNames);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws ProcessingException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        boolean editInProgress = false;
        Integer groupId = null;
        try {
            var group = context.getLiqidInventory()._groupsByName.get(_groupName);
            if (group == null) {
                System.out.printf("INFO:Group %s does not exist in the Liqid Cluster\n", _groupName);
                context.getLogger().trace("%s returning", fn);
                return;
            }

            groupId = group.getGroupId();
            context.getLiqidClient().groupPoolEdit(groupId);
            editInProgress = true;
            for (var devName : _deviceNames) {
                var devStat = context.getLiqidInventory()._deviceStatusByName.get(devName);
                var devId = devStat.getDeviceId();
                context.getLiqidClient().removeDeviceFromGroup(devId, groupId);
                context.getLiqidInventory().notifyDeviceRemovedFromGroup(devId);
            }
            context.getLiqidClient().groupPoolDone(groupId);
            editInProgress = false;
        } catch (LiqidException lex) {
            context.getLogger().catching(lex);
            var pex = new ProcessingException(lex);
            context.getLogger().throwing(pex);
            throw pex;
        } finally {
            if (editInProgress) {
                try {
                    context.getLiqidClient().cancelGroupPoolEdit(groupId);
                } catch (LiqidException lex2) {
                    //  nothing can be done here
                    context.getLogger().catching(lex2);
                    System.err.printf("ERROR:Could not cancel group edit-in-progress for Liqid Cluster group %s\n",
                                       _groupName);
                }
            }
        }

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return String.format("Remove device%s %s from Liqid Cluster Group %s",
                             _deviceNames.size() > 1 ? "s" : "",
                             String.join(", ", _deviceNames),
                             _groupName);
    }
}
