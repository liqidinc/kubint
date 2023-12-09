/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.exceptions.ProcessingException;
import com.bearsnake.k8sclient.K8SClient;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;

import java.util.HashMap;
import java.util.Map;

public class ExecutionContext {
//
//    public final K8SClient _k8sClient;
//    public final LiqidClient _liqidClient;
//    public final Logger _logger;
//
//    private Map<String, DeviceStatus> _devicesStatusByName = null;
//    private Group _group = null;
//    private final Map<String, Machine> _machinesByName = new HashMap<>();
//
//    public ExecutionContext(
//        final K8SClient k8sClient,
//        final LiqidClient liqidClient,
//        final Logger logger
//    ) {
//        _k8sClient = k8sClient;
//        _liqidClient = liqidClient;
//        _logger = logger;
//    }
//
//    public void addMachine(
//        final Machine machine
//    ) {
//        _machinesByName.put(machine.getMachineName(), machine);
//    }
//
//    public DeviceStatus getDeviceStatusByName(
//        final String deviceName
//    ) throws LiqidException, ProcessingException {
//        getDevicesStatusByName(false); // load the container
//        if (!_devicesStatusByName.containsKey(deviceName)) {
//            var msg = String.format("Requested device '%s' does not exist", deviceName);
//            var t = new ProcessingException(msg);
//            _logger.throwing(t);
//            throw t;
//        }
//
//        return _devicesStatusByName.get(deviceName);
//    }
//
//    public Map<String, DeviceStatus> getDevicesStatusByName(
//        final boolean refresh
//    ) throws LiqidException {
//        if (refresh || (_devicesStatusByName == null)) {
//            var devStats = _liqidClient.getAllDevicesStatus();
//            _devicesStatusByName = new HashMap<>();
//            devStats.forEach(ds -> _devicesStatusByName.put(ds.getName(), ds));
//        }
//
//        return _devicesStatusByName;
//    }
//
//    public Group getGroup() {
//        return _group;
//    }
//
//    public Machine getMachineByName(
//        final String machineName
//    ) throws ProcessingException {
//        if (!_machinesByName.containsKey(machineName)) {
//            var msg = String.format("Requested machine '%s' does not exist", machineName);
//            var t = new ProcessingException(msg);
//            _logger.throwing(t);
//            throw t;
//        }
//
//        return _machinesByName.get(machineName);
//    }
//
//    public void loadMachines() throws LiqidException {
//        _machinesByName.clear();
//        var machines = _liqidClient.getMachines();
//        for (var mach : machines) {
//            _machinesByName.put(mach.getMachineName(), mach);
//        }
//    }
//
//    public void setGroup(final Group group) { _group = group; }
}
