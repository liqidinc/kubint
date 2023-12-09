/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.exceptions.ProcessingException;
import com.bearsnake.k8sclient.K8SException;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.LinkedList;

public class AssignToMachine extends Step {

//    private final String _machineName;
//    private final String _k8sNodeName;
//    private final Collection<String> _deviceNames = new LinkedList<>();
//
//    public AssignToMachine(
//        final String machineName,
//        final String k8sNodeName,
//        final Collection<String> deviceNames
//    ) {
//        super(Action.ASSIGN_RESOURCES_TO_MACHINE);
//        _machineName = machineName;
//        _k8sNodeName = k8sNodeName;
//        _deviceNames.addAll(deviceNames);
//    }
//
//    @Override
//    public void perform(
//        final ExecutionContext context
//    ) throws ProcessingException {
//        var fn = "perform";
//        context._logger.trace("Entering %s", fn);
//
//        boolean editInProgress = false;
//        boolean nodeCordoned = false;
//        Integer machineId = null;
//        try {
//            var machine = context.getMachineByName(_machineName);
//            machineId = machine.getMachineId();
//            context._k8sClient.cordonNode(_k8sNodeName);
//            nodeCordoned = true;
//            context._k8sClient.evictPodsForNode(_k8sNodeName, true);
//
//            context._liqidClient.editFabric(machineId);
//            editInProgress = true;
//            var groupId = context.getGroup().getGroupId();
//            for (var devName : _deviceNames) {
//                var devStat = context.getDeviceStatusByName(devName);
//                context._liqidClient.addDeviceToMachine(devStat.getDeviceId(), groupId, machineId);
//            }
//            context._liqidClient.reprogramFabric(machineId);
//            editInProgress = false;
//
//            context._k8sClient.uncordonNode(_k8sNodeName);
//            nodeCordoned = false;
//        } catch (K8SException kex) {
//            context._logger.catching(kex);
//            var t = new ProcessingException(kex);
//            context._logger.throwing(t);
//            throw t;
//        } catch (LiqidException lex) {
//            context._logger.catching(lex);
//            var t = new ProcessingException(lex);
//            context._logger.throwing(t);
//            throw t;
//        } finally {
//            if (editInProgress) {
//                try {
//                    context._liqidClient.cancelEditFabric(machineId);
//                    editInProgress = false;
//                } catch (LiqidException lex) {
//                    // cannot fix this
//                    context._logger.catching(lex);
//                }
//            }
//
//            if (nodeCordoned && !editInProgress) {
//                try {
//                    context._k8sClient.uncordonNode(_k8sNodeName);
//                } catch (K8SException kex) {
//                    // cannot fix this
//                    context._logger.catching(kex);
//                }
//            }
//        }
//
//        context._logger.trace("%s returning", fn);
//    }
//
//    @Override
//    public String toString() {
//        return String.format("Assign to Machine %s: %s", _machineName, String.join(", ", _deviceNames));
//    }
}
