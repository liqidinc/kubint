/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

public class EnableP2PForMachineAction extends Action {

    private String _machineName;

    public EnableP2PForMachineAction() {
        super(ActionType.ENABLE_P2P_FOR_MACHINE);
    }

    public EnableP2PForMachineAction setMachineName(final String value) {_machineName = value; return this; }
    public String getMachineName() { return _machineName; }

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

        var machine = context.getLiqidInventory().getMachine(_machineName);
        if (machine == null) {
            System.out.printf("INFO:Machine %s does not exist in the Liqid Cluster\n", _machineName);
            context.getLogger().trace("%s returning", fn);
            return;
        }

        var machineId = machine.getMachineId();
        context.getLiqidClient().enableP2PForMachine(machineId, true);

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Enable P2P for Machine " + _machineName;
    }
}
