/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;
import com.liqid.sdk.PreDevice;
import com.liqid.sdk.PreDeviceType;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the state of the basic configuration of a Liqid cluster.
 */
public class LiqidConfiguration {
//
//    private final Map<String, DeviceStatus> _devicesByName = new HashMap<>();
//    private final Collection<Group> _groups = new LinkedList<>();
//    private final Collection<Machine> _machines = new LinkedList<>();
//    private final Collection<PreDevice> _preDevices = new LinkedList<>();
//
//    private LiqidConfiguration(
//        final Collection<DeviceStatus> devices,
//        final Collection<Group> groups,
//        final Collection<Machine> machines,
//        final Collection<PreDevice> preDevices
//    ) {
//        devices.forEach(d -> _devicesByName.put(d.getName(), d));
//        _groups.addAll(groups);
//        _machines.addAll(machines);
//        _preDevices.addAll(preDevices);
//    }
//
//    public static LiqidConfiguration create(
//        final LiqidClient liqidClient
//    ) throws LiqidException {
//        var devices = liqidClient.getAllDevicesStatus();
//        var groups = liqidClient.getGroups();
//        var machines = liqidClient.getMachines();
//        var preDevices = liqidClient.getDevices(null, null, null);
//        return new LiqidConfiguration(devices, groups, machines, preDevices);
//    }
//
//    public DeviceStatus getDeviceByName(
//        final String deviceName
//    ) {
//        return _devicesByName.get(deviceName);
//    }
//
//    public Collection<String> getDeviceNames() {
//        return _devicesByName.keySet();
//    }
//
//    public Collection<DeviceStatus> getDevices() {
//        return new LinkedList<>(_devicesByName.values());
//    }
//
//    public Collection<DeviceStatus> getGroupAttachedDevices(
//        final Integer groupId
//    ) {
//        LinkedList<DeviceStatus> deviceStatuses = new LinkedList<>();
//        for (PreDevice pd : _preDevices) {
//            if (pd.getGroupId().equals(groupId) && !pd.getMachineId().equals(0)) {
//                DeviceStatus deviceStatus = _devicesByName.get(pd.getDeviceName());
//                deviceStatuses.add(deviceStatus);
//            }
//        }
//        return deviceStatuses;
//    }
//
//    public Collection<DeviceStatus> getGroupFreePoolDevices(
//        final Integer groupId
//    ) {
//        LinkedList<DeviceStatus> deviceStatuses = new LinkedList<>();
//        for (PreDevice pd : _preDevices) {
//            if (pd.getGroupId().equals(groupId) && pd.getMachineId().equals(0)) {
//                DeviceStatus deviceStatus = _devicesByName.get(pd.getDeviceName());
//                deviceStatuses.add(deviceStatus);
//            }
//        }
//        return deviceStatuses;
//    }
//
//    public Collection<PreDevice> getPreDevices() {
//        return new LinkedList<>(_preDevices);
//    }
//
//    /**
//     * Returns the PreDevice object of the COMPUTE type for the given machine (if there is noe) - else null
//     */
//    public PreDevice getComputeDevicesForMachine(
//        final Integer machineId
//    ) {
//        for (PreDevice pd : _preDevices) {
//            if (Objects.equals(pd.getMachineId(), machineId) && (pd.getPreDeviceType() == PreDeviceType.COMPUTE)) {
//                return pd;
//            }
//        }
//        return null;
//    }
//
//    /**
//     * Returns all the PreDevice objects attached to the given machine.
//     */
//    public Collection<PreDevice> getPreDevicesForMachine(
//        final Integer machineId
//    ) {
//        LinkedList<PreDevice> preDevices = new LinkedList<>();
//        for (PreDevice pd : _preDevices) {
//            if (Objects.equals(pd.getMachineId(), machineId)) {
//                preDevices.add(pd);
//            }
//        }
//
//        return preDevices;
//    }
//
//    public Collection<Group> getGroups() {
//        return new LinkedList<>(_groups);
//    }
//
//    public boolean hasGroups() { return !_groups.isEmpty(); }
//
//    public Collection<Machine> getMachines() {
//        return new LinkedList<>(_machines);
//    }
//
//    public void show(
//        final String indent
//    ) {
//        System.out.printf("%sGroups:\n", indent);
//        for (var g : _groups) {
//            System.out.printf("%s%s%s (%d)\n", indent, indent, g.getGroupName(), g.getGroupId());
//        }
//
//        System.out.printf("%sMachines:\n", indent);
//        for (var m : _machines) {
//            System.out.printf("%s%s%s (group %d)\n", indent, indent, m.getMachineName(), m.getGroupId());
//        }
//
//        System.out.printf("%sDevices:\n", indent);
//        for (var devName : _devicesByName.keySet()) {
//            System.out.printf("%s%s%s\n", indent, indent, devName);
//        }
//
//        System.out.printf("%sPreDevices:\n", indent);
//        for (var p : _preDevices) {
//            System.out.printf("%s%s%s\n", indent, indent, p.getDeviceName());
//        }
//    }
}
