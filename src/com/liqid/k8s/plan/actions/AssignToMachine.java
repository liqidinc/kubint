/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.bearsnake.k8sclient.K8SException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.TreeSet;

public class AssignToMachine extends Action {

    private String _machineName;
    private String _nodeName;
    private TreeSet<String> _deviceNames = new TreeSet<>();

    public AssignToMachine() {
        super(ActionType.ASSIGN_RESOURCES_TO_MACHINE);
    }

    public AssignToMachine addDeviceName(final String value) { _deviceNames.add(value); return this; }
    public AssignToMachine setDeviceNames(final Collection<String> list) { _deviceNames = new TreeSet<>(list); return this; }
    public AssignToMachine setMachineName(final String value) { _machineName = value; return this; }
    public AssignToMachine setNodeName(final String value) { _nodeName = value; return this; }

    public String getMachineName() { return _machineName; }
    public String getNodeName() { return _nodeName; }
    public Collection<String> getDeviceNames() { return _deviceNames; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("MachineName", _machineName);
        checkForNull("DeviceNames", _deviceNames);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws ProcessingException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        boolean editInProgress = false;
        boolean nodeCordoned = false;
        Integer machineId = null;

        // we wrap this in try-catch in order to minimize the deleterious effects of something going badly in the middle.
        try {
            var machine = context.getLiqidInventory()._machinesByName.get(_machineName);
            if (machine == null) {
                System.out.printf("INFO:Machine %s does not exist in the Liqid Cluster\n", _machineName);
                context.getLogger().trace("%s returning", fn);
                return;
            }

            machineId = machine.getMachineId();

            if (_nodeName != null) {
                context.getK8SClient().cordonNode(_nodeName);
                nodeCordoned = true;
                context.getK8SClient().evictPodsForNode(_nodeName, true);
            }

            context.getLiqidClient().editFabric(machineId);
            editInProgress = true;
            var groupId = machine.getGroupId();
            for (var devName : _deviceNames) {
                var devStat = context.getLiqidInventory()._deviceStatusByName.get(devName);
                var devId = devStat.getDeviceId();
                context.getLiqidClient().addDeviceToMachine(devId, groupId, machineId);
                context.getLiqidInventory().notifyDeviceAddedToMachine(devId, machineId);
            }
            context.getLiqidClient().reprogramFabric(machineId);
            editInProgress = false;

            if (nodeCordoned) {
                context.getK8SClient().uncordonNode(_nodeName);
                nodeCordoned = false;
            }
        } catch (K8SException kex) {
            context.getLogger().catching(kex);
            var pex = new ProcessingException(kex);
            context.getLogger().throwing(pex);
            throw pex;
        } catch (LiqidException lex) {
            context.getLogger().catching(lex);
            var pex = new ProcessingException(lex);
            context.getLogger().throwing(pex);
            throw pex;
        } finally {
            if (editInProgress) {
                try {
                    context.getLiqidClient().cancelEditFabric(machineId);
                    editInProgress = false;
                } catch (LiqidException lex) {
                    // cannot fix this
                    context.getLogger().catching(lex);
                    System.err.println("ERROR:Could not cancel fabric edit-in-progress for Liqid Cluster");
                }
            }

            if (nodeCordoned && !editInProgress) {
                try {
                    context.getK8SClient().uncordonNode(_nodeName);
                } catch (K8SException kex) {
                    // cannot fix this either
                    context.getLogger().catching(kex);
                    System.err.printf("ERROR:Could not un-cordon Kubernetes node %s\n", _nodeName);
                }
            }
        }

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Assign to Machine ").append(_machineName);
        if (_nodeName != null) {
            sb.append(" and Node ").append(_nodeName);
        }

        sb.append(" device(s) ").append(String.join(",", _deviceNames));

        return sb.toString();
    }
}
