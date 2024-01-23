/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.plan.actions.Action;
import com.liqid.k8s.plan.actions.AssignToMachineAction;
import com.liqid.k8s.plan.actions.NoOperationAction;
import com.liqid.k8s.plan.actions.ReconfigureMachineAction;
import com.liqid.k8s.plan.actions.RemoveFromMachineAction;
import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// TODO do we need this?
public class Variance {

    private final Machine _machine;
    private final List<Integer> _deviceIdsToAdd = new LinkedList<>();
    private final List<Integer> _deviceIdsToRemove = new LinkedList<>();

    public Variance(
        final Machine machine,
        final Collection<Integer> addingDeviceIds,
        final Collection<Integer> losingDeviceIds
    ) {
        _machine = machine;
        _deviceIdsToAdd.addAll(addingDeviceIds);
        _deviceIdsToRemove.addAll(losingDeviceIds);
    }

    public boolean canBifurcate() { return hasAdditions() && hasRemovals(); }
    public Machine getMachine() { return _machine; }
    public Collection<Integer> getDeviceIdsToAdd() { return _deviceIdsToAdd; }
    public Collection<Integer> getDeviceIdsToRemove() { return _deviceIdsToRemove; }
    public boolean hasAdditions() { return !_deviceIdsToAdd.isEmpty(); }
    public boolean hasRemovals() { return !_deviceIdsToRemove.isEmpty(); }

    /**
     * If we can create an action, we do so, updating the given unassignedDevices collection appropriately
     * We can create the action if there are no devices-to-be-added which are not found in unassignedDevices.
     *
     * @param inventory LiqidInventory
     * @param unassignedDevices collection of device IDs of currently unassigned devices
     *                          we add to this when the action removes devices from the machine, and we
     *                          remove from this when the action adds devices to the machine.
     *                          Returns null if we cannot create an action.
     */
    public Action createAction(
        final LiqidInventory inventory,
        final Set<Integer> unassignedDevices
    ) {
        for (var devs : _deviceIdsToAdd) {
            if (!unassignedDevices.contains(devs)) {
                return null;
            }
        }

        var add = !_deviceIdsToAdd.isEmpty();
        var remove = !_deviceIdsToRemove.isEmpty();
        var nodeName = inventory.getK8sNodeNameFromMachine(_machine.getMachineId());

        if (add && remove) {
            var action = new ReconfigureMachineAction();
            action.setMachineName(_machine.getMachineName());
            action.setNodeName(nodeName);

            for (Integer id : _deviceIdsToAdd) {
                action.addDeviceNameToAdd(inventory.getDeviceItem(id).getDeviceName());
                unassignedDevices.remove(id);
            }

            for (Integer id : _deviceIdsToRemove) {
                action.addDeviceNameToRemove(inventory.getDeviceItem(id).getDeviceName());
                unassignedDevices.add(id);
            }

            return action;
        } else if (add) {
            var action = new AssignToMachineAction();
            action.setMachineName(_machine.getMachineName());

            for (Integer id : _deviceIdsToAdd) {
                action.addDeviceName(inventory.getDeviceItem(id).getDeviceName());
                unassignedDevices.remove(id);
            }

            return action;
        } else if (remove) {
            var action = new RemoveFromMachineAction();
            action.setMachineName(_machine.getMachineName());
            action.setNodeName(nodeName);

            for (Integer id : _deviceIdsToRemove) {
                action.addDeviceName(inventory.getDeviceItem(id).getDeviceName());
                unassignedDevices.add(id);
            }

            return action;
        }

        return new NoOperationAction();
    }

    /**
     * Splits this variance into two unique Variance objects, one for removing devices, and one for adding them,
     * May return an empty list or a singleton depending on what there is to be done.
     */
    public Collection<Variance> bifurcate() {
        if (canBifurcate()) {
            var result = new LinkedList<Variance>();
            if (!_deviceIdsToRemove.isEmpty()) {
                result.add(new Variance(_machine, Collections.emptyList(), _deviceIdsToRemove));
            }
            if (!_deviceIdsToAdd.isEmpty()) {
                result.add(new Variance(_machine, _deviceIdsToAdd, Collections.emptyList()));
            }
            return result;
        } else {
            return Collections.singletonList(this);
        }
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("{machine='").append(_machine.getMachineName()).append("'");

        var addStrings = _deviceIdsToAdd.stream()
                                        .map(String::valueOf)
                                        .collect(Collectors.toCollection(LinkedList::new));
        var addStr = String.join(", ", addStrings);
        sb.append(", adding=[").append(addStr).append("]");

        var remStrings = _deviceIdsToRemove.stream()
                                           .map(String::valueOf)
                                           .collect(Collectors.toCollection(LinkedList::new));
        var remStr = String.join(", ", remStrings);
        sb.append(", removing=[").append(remStr).append("]}");

        return sb.toString();
    }
}
