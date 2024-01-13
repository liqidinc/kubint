/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.plan.actions.Action;
import com.liqid.k8s.plan.actions.AssignToMachine;
import com.liqid.k8s.plan.actions.NoOperation;
import com.liqid.k8s.plan.actions.ReconfigureMachine;
import com.liqid.k8s.plan.actions.RemoveFromMachine;
import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class Variance {

    private final Machine _machine;
    private final String _nodeName;
    private final List<Integer> _deviceIdsToAdd = new LinkedList<>();
    private final List<Integer> _deviceIdsToRemove = new LinkedList<>();

    public Variance(
        final Machine machine,
        final String nodeName,
        final Collection<Integer> addingDeviceIds,
        final Collection<Integer> losingDeviceIds
    ) {
        _machine = machine;
        _nodeName = nodeName;
        _deviceIdsToAdd.addAll(addingDeviceIds);
        _deviceIdsToRemove.addAll(losingDeviceIds);
    }

    public Machine getMachine() { return _machine; }
    public String getNodeName() { return _nodeName; }
    public Collection<Integer> getDeviceIdsToAdd() { return _deviceIdsToAdd; }
    public Collection<Integer> getDeviceIdsToRemove() { return _deviceIdsToRemove; }

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
        final HashSet<Integer> unassignedDevices
    ) {
        for (var devs : _deviceIdsToAdd) {
            if (!unassignedDevices.contains(devs)) {
                return null;
            }
        }

        var remove = !_deviceIdsToRemove.isEmpty();
        var add = !_deviceIdsToAdd.isEmpty();

        if (add && remove) {
            var action = new ReconfigureMachine();
            action.setMachineName(_machine.getMachineName());
            action.setNodeName(_nodeName);

            for (Integer id : _deviceIdsToAdd) {
                action.addDeviceNameToAdd(inventory._deviceStatusById.get(id).getName());
                unassignedDevices.remove(id);
            }

            for (Integer id : _deviceIdsToRemove) {
                action.addDeviceNameToRemove(inventory._deviceStatusById.get(id).getName());
                unassignedDevices.add(id);
            }

            return action;
        } else if (add) {
            var action = new AssignToMachine();
            action.setMachineName(_machine.getMachineName());
            action.setNodeName(_nodeName);

            for (Integer id : _deviceIdsToAdd) {
                action.addDeviceName(inventory._deviceStatusById.get(id).getName());
                unassignedDevices.remove(id);
            }

            return action;
        } else if (remove) {
            var action = new RemoveFromMachine();
            action.setMachineName(_machine.getMachineName());
            action.setNodeName(_nodeName);

            for (Integer id : _deviceIdsToRemove) {
                action.addDeviceName(inventory._deviceStatusById.get(id).getName());
                unassignedDevices.add(id);
            }

            return action;
        }

        return new NoOperation();
    }

    /**
     * Splits this variance into two unique Variance objects, one for removing devices, and one for adding them,
     * May return an empty list or a singleton depending on what there is to be done.
     */
    public Collection<Variance> bifurcate() {
        var result = new LinkedList<Variance>();
        if (!_deviceIdsToRemove.isEmpty()) {
            result.add(new Variance(_machine, _nodeName, Collections.emptyList(), _deviceIdsToRemove));
        }
        if (!_deviceIdsToAdd.isEmpty()) {
            result.add(new Variance(_machine, _nodeName, _deviceIdsToAdd, Collections.emptyList()));
        }
        return result;
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