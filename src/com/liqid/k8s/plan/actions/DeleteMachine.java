/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

/**
 * Deletes a machine with the specified name, as part of a specified group.
 */
public class DeleteMachine extends Action {

    private String _machineName;

    public DeleteMachine() {
        super(ActionType.DELETE_MACHINE);
    }

    public DeleteMachine setMachineName(final String value) { _machineName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("MachineName", _machineName);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws LiqidException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        var machine = context.getLiqidInventory()._machinesByName.get(_machineName);
        if (machine == null) {
            System.out.printf("INFO:Machine %s does not exist in the Liqid Cluster\n", _machineName);
            context.getLogger().trace("%s returning", fn);
            return;
        }

        context.getLiqidClient().deleteMachine(machine.getMachineId());
        context.getLiqidInventory().notifyMachineDeleted(machine.getMachineId());

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Delete Machine " + _machineName + " from Liqid Cluster";
    }
}
