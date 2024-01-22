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

/**
 * Deletes a machine with the specified name, as part of a specified group.
 * If a node name is specified, we cordon/uncordon the node as part of this process.
 */
public class DeleteMachineAction extends Action {

    private String _machineName;
    private String _nodeName;

    public DeleteMachineAction() {
        super(ActionType.DELETE_MACHINE);
    }

    public DeleteMachineAction setMachineName(final String value) {_machineName = value; return this; }
    public DeleteMachineAction setNodeName(final String value) {_nodeName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("MachineName", _machineName);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws LiqidException, ProcessingException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

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

            if (_nodeName != null) {
                System.out.printf("Cordoning node %s...\n", _nodeName);
                context.getK8SClient().cordonNode(_nodeName);
                nodeCordoned = true;
                context.getK8SClient().evictPodsForNode(_nodeName, true);
            }

            context.getLiqidClient().deleteMachine(machine.getMachineId());
            context.getLiqidInventory().notifyMachineRemoved(machine.getMachineId());

            if (nodeCordoned) {
                System.out.printf("Uncordoning node %s...\n", _nodeName);
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
            if (nodeCordoned) {
                try {
                    System.out.printf("Uncordoning node %s...\n", _nodeName);
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
        return "Delete Machine " + _machineName + " from Liqid Cluster";
    }
}
