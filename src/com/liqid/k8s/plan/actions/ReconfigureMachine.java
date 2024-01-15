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

import java.util.Collection;
import java.util.TreeSet;

public class ReconfigureMachine extends Action {

    private String _machineName;
    private String _nodeName;
    private TreeSet<String> _deviceNamesToAdd = new TreeSet<>();
    private TreeSet<String> _deviceNamesToRemove = new TreeSet<>();

    public ReconfigureMachine() {
        super(ActionType.RECONFIGURE_MACHINE);
    }

    public ReconfigureMachine addDeviceNameToAdd(final String value) { _deviceNamesToAdd.add(value); return this; }
    public ReconfigureMachine setDeviceNamesToAdd(final Collection<String> list) {_deviceNamesToAdd = new TreeSet<>(list); return this; }
    public ReconfigureMachine addDeviceNameToRemove(final String value) { _deviceNamesToRemove.add(value); return this; }
    public ReconfigureMachine setDeviceNamesToRemove(final Collection<String> list) {_deviceNamesToRemove = new TreeSet<>(list); return this; }
    public ReconfigureMachine setMachineName(final String value) {_machineName = value; return this; }
    public ReconfigureMachine setNodeName(final String value) {_nodeName = value; return this; }

    public String getMachineName() { return _machineName; }
    public String getNodeName() { return _nodeName; }
    public Collection<String> getDeviceNamesToAdd() { return _deviceNamesToAdd; }
    public Collection<String> getDeviceNamesToRemove() { return _deviceNamesToRemove; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("MachineName", _machineName);
        checkForNull("DeviceNamesToAdd", _deviceNamesToAdd);
        checkForNull("DeviceNamesToRemove", _deviceNamesToRemove);
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
            var machine = context.getLiqidInventory().getMachine(_machineName);
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

            for (var devName : _deviceNamesToAdd) {
                var devStat = context.getLiqidInventory().getDeviceItem(devName).getDeviceStatus();
                var devId = devStat.getDeviceId();
                context.getLiqidClient().addDeviceToMachine(devId, groupId, machineId);
                context.getLiqidInventory().notifyDeviceAssignedToMachine(devId, machineId);
            }

            for (var devName : _deviceNamesToRemove) {
                var devStat = context.getLiqidInventory().getDeviceItem(devName).getDeviceStatus();
                var devId = devStat.getDeviceId();
                context.getLiqidClient().removeDeviceFromMachine(devId, groupId, machineId);
                context.getLiqidInventory().notifyDeviceRemovedFromMachine(devId);
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
        sb.append("Reconfigure Machine ").append(_machineName);
        if (_nodeName != null) {
            sb.append(" and Node ").append(_nodeName);
        }

        if (!_deviceNamesToAdd.isEmpty()) {
            sb.append(" adding ").append(String.join(",", _deviceNamesToAdd));
        }

        if (!_deviceNamesToRemove.isEmpty()) {
            sb.append(" removing ").append(String.join(",", _deviceNamesToRemove));
        }

        return sb.toString();
    }
}
