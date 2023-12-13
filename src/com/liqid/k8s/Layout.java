/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.PreDeviceType;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A layout describes an association of liqid devices to liqid machines.
 * It is generally used as a road-map for creation of a Plan object.
 */
public class Layout {

//    public final Logger _logger;
//    public final Map<String, MachineInfo> _machineInfos = new HashMap<>();
//
//    private Layout(
//        final Logger logger,
//        final Collection<MachineInfo> machineInfos
//    ) {
//        _logger = logger;
//        machineInfos.forEach(mi -> _machineInfos.put(mi.getMachineName(), mi));
//    }
//
//    /**
//     * Creates a resource layout for a particular resource type.
//     * Returns false if we have insufficient resources of this type.
//     */
//    private static boolean createInitialResourceLayout(
//        final ResourceType resourceType,
//        final K8SConfiguration k8sConfiguration,
//        final Collection<String> deviceNames,
//        final Map<String, MachineInfo> machineInfos,
//        final Logger logger
//    ) {
//        var fn = "createInitialResourceLayout";
//        logger.atTrace().log("Entering {} with resType={} k8sCfg={} devNames={} machInfos={}",
//                             fn,
//                             resourceType,
//                             k8sConfiguration,
//                             deviceNames == null ? "<null>" : String.join(",", deviceNames),
//                             String.join(",", machineInfos.values().toString()));
//
//        //  get a temporary list of all the device names for the given type
//        var tempDevNames = new LinkedList<String>();
//        if ((deviceNames != null) && !deviceNames.isEmpty()) {
//            tempDevNames.addAll(deviceNames);
//        }
//        var successful = true;
//
//        //  Allocate devices across nodes which specify a resource count.
//        //  Also build the list of nodes which do not.
//        var nonSpecNodes = new LinkedList<ConfigurationNode>();
//        for (var cfgNode : k8sConfiguration.getNodes()) {
//            var specifiedCounter = cfgNode.getSpecifiedCounter(resourceType);
//            if (cfgNode._isWorker) {
//                if (specifiedCounter != null) {
//                    var machInfo = machineInfos.get(cfgNode._k8sNodeName);
//                    if (tempDevNames.size() < specifiedCounter) {
//                        System.err.printf("Insufficient %s resources for node %s: Remaining=%d, Requested=%d\n",
//                                          resourceType._tag,
//                                          cfgNode._k8sNodeName,
//                                          tempDevNames.size(),
//                                          specifiedCounter);
//                        successful = false;
//                    } else {
//                        for (int dx = 0; dx < specifiedCounter; ++dx) {
//                            machInfo.addDeviceName(tempDevNames.pop());
//                        }
//                    }
//                } else {
//                    nonSpecNodes.add(cfgNode);
//                }
//            }
//        }
//
//        //  Now allocate the remainder of devices (if any) across the nodes which did *not* specify a count
//        while (!tempDevNames.isEmpty() && !nonSpecNodes.isEmpty()) {
//            var devsPer = tempDevNames.size() / nonSpecNodes.size();
//            var devsMod = tempDevNames.size() % nonSpecNodes.size();
//            if (devsMod > 0) {
//                devsPer++;
//            }
//
//            var cfgNode = nonSpecNodes.pop();
//            var machInfo = machineInfos.get(cfgNode._k8sNodeName);
//            for (int dx = 0; dx < devsPer; ++dx) {
//                machInfo.addDeviceName(tempDevNames.pop());
//            }
//        }
//
//        return successful;
//    }
//
//    private static Collection<MachineInfo> createCommon(
//        final K8SConfiguration k8sConfiguration,
//        final Collection<DeviceStatus> devices,
//        final Logger logger
//    ) throws SetupException {
//        var fn = "createCommon";
//        logger.atTrace().log("Entering {} with k8sCfg={} devs={}", fn, k8sConfiguration, devices);
//
//        var devicesByName = new HashMap<String, DeviceStatus>();
//        var deviceNamesByResType = new HashMap<ResourceType, List<String>>();
//        for (var devStat : devices) {
//            devicesByName.put(devStat.getName(), devStat);
//
//            var resType = ResourceType.fromDeviceType(devStat.getDeviceType());
//            if (!deviceNamesByResType.containsKey(resType)) {
//                deviceNamesByResType.put(resType, new LinkedList<>());
//            }
//
//            var subList = deviceNamesByResType.get(resType);
//            subList.add(devStat.getName());
//        }
//
//        var machInfoByK8SName = new HashMap<String, MachineInfo>();
//        for (var cfgNode : k8sConfiguration.getNodes()) {
//            var pcpuName = cfgNode._liqidPCPUName;
//            if (devicesByName.containsKey(pcpuName)) {
//                var machineName = String.format("%s%s", Main.LIQID_MACHINE_NAME_PREFIX, cfgNode._k8sNodeName);
//                machInfoByK8SName.put(cfgNode._k8sNodeName, new MachineInfo(cfgNode._k8sNodeName, machineName, pcpuName));
//            }
//        }
//
//        var resError = false;
//        for (var resourceType : ResourceType.values()) {
//            if (resourceType != ResourceType.RT_CPU) {
//                var resDevNames = deviceNamesByResType.get(resourceType);
//                if (resDevNames != null) {
//                    if (!createInitialResourceLayout(resourceType,
//                                                     k8sConfiguration,
//                                                     resDevNames,
//                                                     machInfoByK8SName,
//                                                     logger)) {
//                        resError = true;
//                    }
//                }
//            }
//        }
//
//        if (resError) {
//            var t = new SetupException("Insufficient resources");
//            logger.throwing(t);
//            throw t;
//        }
//
//        var result = machInfoByK8SName.values();
//        logger.atTrace().log("{} returning {}", fn, result);
//        return result;
//    }
//
//    public static Layout createExisting(
//        final K8SConfiguration k8sConfiguration,
//        final LiqidConfiguration liqidConfiguration,
//        final Integer groupId,
//        final Logger logger
//    ) {
//        var fn = "createExisting";
//        logger.atTrace().log("Entering {} with k8sCfg={} liqCfg={} grp={}",
//                             fn, k8sConfiguration, liqidConfiguration, groupId);
//
//        var machInfos = new LinkedList<MachineInfo>();
//        for (var mach : liqidConfiguration.getMachines()) {
//            if (Objects.equals(mach.getGroupId(), groupId)) {
//                var cpuPreDev = liqidConfiguration.getComputeDevicesForMachine(mach.getMachineId());
//                if (cpuPreDev != null) {
//                    var machName = mach.getMachineName();
//                    var devName = cpuPreDev.getDeviceName();
//                    var k8sName = machName.substring(Main.LIQID_MACHINE_NAME_PREFIX.length());
//                    var machInfo = new MachineInfo(k8sName, machName, devName);
//
//                    var preDevs = liqidConfiguration.getPreDevicesForMachine(mach.getMachineId());
//                    for (var preDev : preDevs) {
//                        if (preDev.getPreDeviceType() != PreDeviceType.COMPUTE) {
//                            machInfo.addDeviceName(preDev.getDeviceName());
//                        }
//                    }
//
//                    machInfos.add(machInfo);
//                }
//            }
//        }
//
//        var result = new Layout(logger, machInfos);
//        logger.atTrace().log("{} returning {}", fn, result);
//        return result;
//    }
//
//    /**
//     * Creates a layout for adding nodes into an existing configuration
//     */
//    public static Layout createForAddingNodes(
//        final K8SConfiguration k8sConfiguration,
//        final LiqidConfiguration liqidConfiguration,
//        final Integer groupId,
//        final Logger logger
//    ) throws SetupException {
//        var fn = "createForAddingNodes";
//        logger.atTrace().log("Entering {} with k8sCfg={} liqCfg={} grp={}",
//                             fn, k8sConfiguration, liqidConfiguration, groupId);
//
//        var devicePool = liqidConfiguration.getGroupFreePoolDevices(groupId);
//
//        var result = new Layout(logger, createCommon(k8sConfiguration, devicePool, logger));
//        result.mergeWith(Layout.createForInitialConfiguration(k8sConfiguration, liqidConfiguration, logger));
//
//        logger.atTrace().log("{} returning {}", fn, result);
//        return result;
//    }
//
//    /**
//     * Creates a Layout for initial configuration
//     */
//    public static Layout createForInitialConfiguration(
//        final K8SConfiguration k8sConfiguration,
//        final LiqidConfiguration liqidConfiguration,
//        final Logger logger
//    ) throws SetupException {
//        var fn = "createForInitialConfiguration";
//        logger.atTrace().log("Entering {} with k8sCfg={} liqCfg={}", fn, k8sConfiguration, liqidConfiguration);
//
//        var devicePool = liqidConfiguration.getDevices();
//
//        var result = new Layout(logger, createCommon(k8sConfiguration, devicePool, logger));
//        logger.atTrace().log("{} returning {}", fn, result);
//        return result;
//    }
//
//    private void mergeWith(
//        final Layout layout2
//    ) {
//        _machineInfos.putAll(layout2._machineInfos);
//    }
//
////    private void doResourceAllocation(
////        final ResourceType resourceType,
////        final Collection<DeviceStatus> resources
////    ) throws ConfigFileException, ProcessingException {
////        var fn = "doResourceAllocation";
////        _logger.atTrace().log(String.format("Entering %s with resType=%s resources=%s", fn, resourceType, resources));
////
////        var tempResources = new LinkedList<>(resources);
////
////        // do specific resource requests first while
////        // collecting a list of workers for which this resource is unspecified
////        var workers_unspecified = new LinkedList<OldMachineInfo>();
////        for (var machInfo : _machineInfos) {
////            var reqCount = machInfo.getCounterSpecification(resourceType);
////            if (reqCount != null) {
////                if (reqCount > tempResources.size()) {
////                    var msg = String.format("Insufficient %s resources for %s - have %d remaining, wanted %d",
////                                            resourceType._tag,
////                                            machInfo.getMachineName(),
////                                            resources.size(),
////                                            reqCount);
////                    var t = new ConfigFileException(msg);
////                    _logger.throwing(t);
////                    throw t;
////                }
////
////                for (int subCount = 0; subCount < reqCount; subCount++) {
////                    machInfo.addResource(tempResources.pop());
////                }
////            } else if (machInfo.getConfigurationNode()._isWorker) {
////                workers_unspecified.add(machInfo);
////            }
////        }
////
////        // now spread what's left over the unspecified nodes
////        while (!workers_unspecified.isEmpty()) {
////            var machineCount = workers_unspecified.size();
////            var resourceCount = tempResources.size();
////            var count = resourceCount / machineCount;
////            if (resourceCount % machineCount > 0) {
////                count++;
////            }
////
////            var machInfo = workers_unspecified.pop();
////            while (count > 0) {
////                machInfo.addResource(tempResources.pop());
////                count--;
////            }
////        }
////
////        // if there are any remaining resources, make a note
////        if (!tempResources.isEmpty()) {
////            System.out.printf("NOTE: %d %s resources remain unassigned\n",
////                              tempResources.size(),
////                              resourceType._tag);
////        }
////
////        _logger.atTrace().log(String.format("%s returning", fn));
////    }
//
//    public MachineInfo getMachineInfo(
//        final String machineName
//    ) {
//        return _machineInfos.get(machineName);
//    }
//
//    public void show(
//        final String indent
//    ) {
//        for (var mi : _machineInfos.values()) {
//            System.out.printf("%s%s\n", indent, mi.toString());
//        }
//    }
//
//    @Override
//    public String toString() {
//        var sb = new StringBuilder();
//        sb.append("{machineInfos=[");
//        var first = true;
//        for (var machInfo : _machineInfos.values()) {
//            if (!first) sb.append(", ");
//            sb.append(machInfo.toString());
//            first = false;
//        }
//        sb.append("]");
//        return sb.toString();
//    }
}
