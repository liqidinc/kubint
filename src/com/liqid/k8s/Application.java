/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.commands.CommandType;
import com.liqid.k8s.commands.InitializeCommand;
import com.liqid.k8s.commands.LinkCommand;
import com.liqid.k8s.commands.NodesCommand;
import com.liqid.k8s.commands.ResetCommand;
import com.liqid.k8s.commands.ResourcesCommand;
import com.liqid.k8s.commands.UnlinkCommand;
import com.liqid.k8s.exceptions.ScriptException;
import com.liqid.sdk.LiqidException;

import java.util.Collection;

public class Application {

    private CommandType _commandType;
    private int _timeoutInSeconds = 300;

    private Boolean _allocate;
    private Boolean _force;
    private String _liqidAddress;
    private String _liqidGroupName;
    private String _liqidPassword;
    private String _liqidUsername;
    private Logger _logger;
    private Boolean _noUpdate;
    private Collection<String> _processorSpecs;
    private String _proxyURL;
    private Collection<String> _resourceSpecs;

    Application setAllocate(final Boolean value) { _allocate = value; return this; }
    Application setCommandType(final CommandType value) { _commandType = value; return this; }
    Application setForce(final Boolean value) { _force = value; return this; }
    Application setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    Application setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    Application setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    Application setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    Application setLogger(final Logger value) { _logger = value; return this; }
    Application setNoUpdate(final boolean flag) { _noUpdate = flag; return this; }
    Application setProxyURL(final String value) { _proxyURL = value; return this; }
    Application setProcessorSpecs(final Collection<String> list) { _processorSpecs = list; return this; }
    Application setResourceSpecs(final Collection<String> list) {_resourceSpecs = list; return this; }
    Application setTimeoutInSeconds(final int value) { _timeoutInSeconds = value; return this; }

    // ------------------------------------------------------------------------
    // helper functions
    // ------------------------------------------------------------------------

//    private void addDevicesToMachine(
//        final Integer groupId,
//        final Integer machId,
//        final ResourceType resType,
//        final Integer count
//    ) throws ProcessingException, LiqidException {
//        var fn = "addDevicesToMachine";
//        _logger.atTrace().log("Entering {} groupId={} machId={} resType={} count={}",
//                              fn, groupId, machId, resType, count);
//
//        var preDevs = _liqidClient.getUnattachedDevicesForGroup(resType._queryType, groupId);
//        if (preDevs.size() < count) {
//            var msg = String.format("Internal error - we have %d %s devices but we need %d",
//                                    preDevs.size(), resType._tag, count);
//            var t = new ProcessingException(msg);
//            _logger.throwing(t);
//            throw t;
//        }
//
//        for (int dx = 0; dx < count; ++dx) {
//            var preDev = preDevs.pop();
//            var dev = _ContextForUpdate._allDevicesStatusByName.get(preDev.getDeviceName());
//            _liqidClient.addDeviceToMachine(dev.getDeviceId(), groupId, machId);
//        }
//
//        _logger.atTrace().log("{} returning", fn);
//    }

//    /**
//     * Checks aggregate variances (all variances added together per resource type)
//     * comparing them to the number of free pool or group free pool devices to see
//     * if we are over-committed.
//     * @param groupId Identifier of the kubernetes Liqid Cluster Group
//     * @return true if we are okay, else false.
//     */
//    private boolean checkAvailability(
//        final Integer groupId,
//        final Map<String, Map<ResourceType, Integer>> resourceVariances
//    ) throws LiqidException {
//        var fn = "checkAvailability";
//        _logger.atTrace().log("Entering {} groupId={} resVars={}", fn, groupId, resourceVariances);
//
//        // aggregateVariances is a collection mapping each resource type to the aggregate
//        // variance across all machines in the machines-by-name collection.
//        var aggregateVariances = Arrays.stream(ResourceType.values())
//                                       .collect(Collectors.toMap(et -> et, et -> 0, (a, b) -> b, HashMap::new));
//
//        for (var entry : resourceVariances.entrySet()) {
//            var subMap = entry.getValue();
//            for (var subEntry : subMap.entrySet()) {
//                var resType = subEntry.getKey();
//                var resVar = subEntry.getValue();
//                var newValue = aggregateVariances.get(resType) + resVar;
//                aggregateVariances.put(resType, newValue);
//            }
//        }
//
//        // availableResources is a collection mapping each resource type to the total number
//        // of available resources of that type, where 'available' means the device is
//        // either in the system pool or in the k8s group free pool.
//        var availableResources =
//            Arrays.stream(ResourceType.values())
//                  .collect(Collectors.toMap(resType -> resType, resType -> 0, (a, b) -> b, HashMap::new));
//
//        var freeGroupDevs = _liqidClient.getUnattachedDevicesForGroup(null, groupId);
//        for (var freeDev : freeGroupDevs) {
//            var resType = preDeviceTypeToResourceType(freeDev.getPreDeviceType());
//            var newValue = availableResources.get(resType) + 1;
//            availableResources.put(resType, newValue);
//        }
//
//        availableResources.put(ResourceType.RT_FPGA,
//                               availableResources.get(ResourceType.RT_FPGA) + _liqidClient.getFreeFPGADevicesStatus().size());
//        availableResources.put(ResourceType.RT_GPU,
//                               availableResources.get(ResourceType.RT_GPU) + _liqidClient.getFreeGPUDevicesStatus().size());
//        availableResources.put(ResourceType.RT_MEMORY,
//                               availableResources.get(ResourceType.RT_MEMORY) + _liqidClient.getFreeMemoryDevicesStatus().size());
//        availableResources.put(ResourceType.RT_LINK,
//                               availableResources.get(ResourceType.RT_LINK) + _liqidClient.getFreeNetworkDevicesStatus().size());
//        availableResources.put(ResourceType.RT_TARGET,
//                               availableResources.get(ResourceType.RT_TARGET) + _liqidClient.getFreeStorageDevicesStatus().size());
//
//        var result = true;
//        for (var var : aggregateVariances.entrySet()) {
//            var resType = var.getKey();
//            var wanted = var.getValue();
//            if (wanted > 0) {
//                var have = availableResources.get(resType);
//                if (wanted > have) {
//                    var msg = String.format("Insufficient %s devices - want %d, have %d", resType._tag, wanted, have);
//                    System.out.println(msg);
//                    result = false;
//                }
//            }
//        }
//
//        _logger.atTrace().log("{} returning {}", fn, result);
//        return result;
//    }

//    /**
//     * Deploys the given layout to the Liqid Client.
//     * For an initial config, the given groupId is null and we create a group.
//     * For an expansion, the existing groupId is provided as a parameter.
//     */
//    private void deployLayout(
//        final Layout layout,
//        final Integer existingGroupId
//    ) throws ProcessingException, LiqidException {
//        var fn = "deployLayout";
//        _logger.trace(String.format("Entering %s layout=%s groupId=%d", fn, layout, existingGroupId));
//
//        Integer groupId;
//        if (existingGroupId == null) {
//            // Create kubernetes group
//            var liqidGroup = _liqidClient.createGroup(LIQID_K8S_GROUP_NAME);
//            groupId = liqidGroup.getGroupId();
//        } else {
//            groupId = existingGroupId;
//        }
//
//        // Move everything we're going to attach to the eventual machines, to the new group.
//        try {
//            _liqidClient.groupPoolEdit(groupId);
//            for (var machInfo : layout._machineInfos) {
//                for (var devStat : machInfo.getAllResources()) {
//                    System.out.printf("Adding device %s to group %d...\n", devStat.getName(), groupId);
//                    _liqidClient.addDeviceToGroup(devStat.getDeviceId(), groupId);
//                }
//            }
//            _liqidClient.groupPoolDone(groupId);
//        } catch (LiqidException lex) {
//            try {
//                _liqidClient.cancelGroupPoolEdit(groupId);
//            } catch (LiqidException lex2) {
//                // can't do anything about it.
//            }
//            throw lex;
//        }
//
//        // Create the various machines and move resources to them.
//        for (var machInfo : layout._machineInfos) {
//            var editing = false;
//            var machineId = 0;
//            try {
//                var liqidMachine = _liqidClient.createMachine(groupId, machInfo.getMachineName());
//                machineId = liqidMachine.getMachineId();
//                _liqidClient.editFabric(machineId);
//                editing = true;
//                for (var devStat : machInfo.getAllResources()) {
//                    System.out.printf("Adding device %s to machine %s...\n", devStat.getName(), machInfo.getMachineName());
//                    _liqidClient.addDeviceToMachine(devStat.getDeviceId(), groupId, machineId);
//                }
//
//                _liqidClient.reprogramFabric(machineId);
//                if (machInfo.getResources(ResourceType.RT_GPU).size() > 1) {
//                    System.out.printf("Enabling P2P for machine %s...\n", machInfo.getMachineName());
//                    _liqidClient.setP2PEnabled(machineId, P2PType.ON);
//                }
//            } catch (LiqidException lex) {
//                if (editing) {
//                    try {
//                        _liqidClient.cancelEditFabric(machineId);
//                    } catch (LiqidException lex2) {
//                        // give up
//                    }
//                }
//                throw lex;
//            }
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }
//
//    /**
//     * Produces a map of machine names to maps of resource types to the variance in resource counts
//     * for that resource type, for that machine.
//     * i.e., value['Machine-Node2'][RT_GPU] == variance of GPUs for Machine-Node2.
//     * If the variance is positive, we need to add {n} GPUs to the machine, where {n} == |variance|.
//     * If negative, we need to remove {n} GPUs from the machine, where {n} == |variance|.
//     * If any particular machine has no variances, it will not have an entry in the map.
//     */
//    private Map<String, Map<ResourceType, Integer>> getResourceVariances(
//        final K8SConfiguration configuration
//    ) throws ProcessingException {
//        var fn = "getResourceVariances";
//        _logger.trace(String.format("Entering %s config=%s", fn, configuration));
//
//        var result = new HashMap<String, Map<ResourceType, Integer>>();
//        for (var configNode : configuration.getNodes()) {
//            // Create a ResourceType-keyed map which contains the specified counts
//            // of the resources which the config file wants assigned to each k8s node.
//            // A null value indicates that the count is not specified.
//            var requestedCounts = new HashMap<ResourceType, Integer>();
//            requestedCounts.put(ResourceType.RT_FPGA, configNode._specifiedFPGACount);
//            requestedCounts.put(ResourceType.RT_GPU, configNode._specifiedGPUCount);
//            requestedCounts.put(ResourceType.RT_MEMORY, configNode._specifiedMemoryCount);
//            requestedCounts.put(ResourceType.RT_LINK, configNode._specifiedLinkCount);
//            requestedCounts.put(ResourceType.RT_TARGET, configNode._specifiedSSDCount);
//
//            // Find the Liqid Machine object which corresponds to this node,
//            // then get the list of devices (aka resources) currently assigned to this machine.
//            var machineName = String.format("%s-%s", LIQID_K8S_GROUP_NAME, configNode._k8sNodeName);
//
//            // For this machine, determine the device count variance per resource type.
//            // variance > 0 means we need more resources; variance < 0 means we need fewer.
//            var actualCounts = Arrays.stream(ResourceType.values())
//                                     .collect(Collectors.toMap(et -> et, et -> 0, (a, b) -> b, HashMap::new));
//            for (var dev : _ContextForUpdate._preDevicesByMachineName.get(machineName)) {
//                var preDevType = dev.getPreDeviceType();
//                var resType = preDeviceTypeToResourceType(preDevType);
//                actualCounts.put(resType, actualCounts.get(resType) + 1);
//            }
//
//            // build map of resource types -> variances for the machine,
//            // but only post it to the result if there is at least one non-zero variance.
//            var hasVariance = false;
//            var machineVariances = new HashMap<ResourceType, Integer>();
//            for (var resType : ResourceType.values()) {
//                var wanted = requestedCounts.get(resType);
//                var actual = actualCounts.get(resType);
//                if (wanted != null) {
//                    // wanted count is specified; variance is wanted - actual
//                    machineVariances.put(resType, wanted - actual);
//                    hasVariance = true;
//                } else if (actual > 0) {
//                    // wanted count is *not* specified, but the machine has resources -
//                    // therefore, variance is (0 - resource count).
//                    // Note that if wanted is unspecified and the machine has no resources,
//                    // then it does not have a variance for this resource type.
//                    // This rule is in place so that we can count this device's resources as available for
//                    // re-allocation in order for the reconfigure results to match the initial config results
//                    // for the current Liqid Cluster inventory given the same config file for either case.
//                    machineVariances.put(resType, -actual);
//                    hasVariance = true;
//                }
//            }
//
//            if (hasVariance) {
//                result.put(machineName, machineVariances);
//            }
//        }
//
//        _logger.trace(String.format("%s returning %s", fn, result));
//        return result;
//    }

//    /**
//     * Creates a LiqidClient object, configures it for logging, and logs in.
//     */
//    private void makeLiqidClient() throws LiqidException {
//        var fn = "makeLiqidClient";
//        _logger.atTrace().log("Entering {}", fn);
//
//        _liqidClient = new LiqidClientBuilder().setHostAddress(_directorAddress)
//                                               .setTimeoutInSeconds(_timeoutInSeconds)
//                                               .build();
//        if (_logging) {
//            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//            Configuration config = ctx.getConfiguration();
//            LoggerConfig loggerConfig = config.getLoggerConfig(_liqidClient.getLogger().getName());
//            loggerConfig.setLevel(Level.TRACE);
//            ctx.updateLoggers();
//        }
//
//        _liqidClient.login("LiqidK8SDeploy", _directorUsername, _directorPassword);
//        _logger.atTrace().log("{} returning", fn);
//    }

//    /**
//     * Creates a k8s client and configures it for logging.
//     */
//    private void makeK8SClient() {
//        _k8sClient = new K8SClient(_proxyURL);
//        _k8sClient.enableLogging(_logging);
//    }

//    /**
//     * Converts a LiqidSDK pre-device type to a ResourceType
//     */
//    private static ResourceType preDeviceTypeToResourceType(
//        final PreDeviceType preDeviceType
//    ) {
//        return switch (preDeviceType) {
//            case GPU -> ResourceType.RT_GPU;
//            case TARGET -> ResourceType.RT_TARGET;
//            case LINK, FIBER_CHANNEL, INFINIBAND -> ResourceType.RT_LINK;
//            case FPGA -> ResourceType.RT_FPGA;
//            case COMPUTE -> ResourceType.RT_CPU;
//            case MEMORY -> ResourceType.RT_MEMORY;
//            case FABRIC_CHIP -> null; // should not get this one
//        };
//    }
//
//    /**
//     * Performs the actual reconfiguration
//     */
//    private void reconfigure(
//        final Integer groupId,
//        final Map<String, Map<ResourceType, Integer>> resourceVariances
//    ) throws ProcessingException, LiqidException, K8SException {
//        var fn = "reconfigure";
//        _logger.trace(String.format("Entering %s groupId=%d resVars=%s", fn, groupId, resourceVariances));
//
//        var kubeClient = new K8SClient(_proxyURL);
//        kubeClient.enableLogging(true);
//
//        var group1 = new LinkedList<String>();
//        var group2 = new LinkedList<String>();
//
//        // Move all system free resources to the group free pool, even if we end up not assigning all of them.
//        // It's... easier this way.
//        List<DeviceStatus> devs = new LinkedList<>();
//        devs.addAll(_liqidClient.getFreeFPGADevicesStatus());
//        devs.addAll(_liqidClient.getFreeGPUDevicesStatus());
//        devs.addAll(_liqidClient.getFreeMemoryDevicesStatus());
//        devs.addAll(_liqidClient.getFreeNetworkDevicesStatus());
//        devs.addAll(_liqidClient.getFreeStorageDevicesStatus());
//        if (devs.size() > 0) {
//            System.out.printf("Moving %d device(s) to the k8s Liqid Cluster Croup...\n", devs.size());
//            _liqidClient.groupPoolEdit(groupId);
//            try {
//                for (var dev : devs) {
//                    _liqidClient.addDeviceToGroup(dev.getDeviceId(), groupId);
//                }
//            } finally {
//                _liqidClient.groupPoolDone(groupId);
//            }
//        }
//
//        // Create two lists - group1 contains machine names of all machines which are losing resources
//        // of at least one resource type. group2 contains machine names of all machines which are gaining
//        // resources of at least one resource type. Note that any particular machine can be in either,
//        // both, or neither groups.
//        for (var entry : resourceVariances.entrySet()) {
//            var machName = entry.getKey();
//            var machVariances = entry.getValue();
//            var needsMore = false;
//            var needsFewer = false;
//            for (var varEntry : machVariances.entrySet()) {
//                var resVariance = varEntry.getValue();
//                if (resVariance > 0) {
//                    needsMore = true;
//                } else if (resVariance < 0) {
//                    needsFewer = true;
//                }
//            }
//
//            if (needsFewer) {
//                group1.add(machName);
//            }
//            if (needsMore) {
//                group2.add(machName);
//            }
//        }
//
//        // Remove resources from machines as appropriate. We do this first so that we
//        // have adequate free resources to add to machines in the second step.
//        for (var machName : group1) {
//            var nodeName = _ContextForUpdate._nodeNamesByMachineName.get(machName);
//            System.out.printf("Draining %s...\n", nodeName);
//            kubeClient.cordonNode(nodeName);
//            kubeClient.evictPodsForNode(nodeName, true);
//
//            var machId = _ContextForUpdate._liqidMachinesByName.get(machName).getMachineId();
//            _liqidClient.editFabric(machId);
//            var resMap = resourceVariances.get(machName);
//            for (var entry : resMap.entrySet()) {
//                var resType = entry.getKey();
//                var variance = entry.getValue();
//                try {
//                    if (variance < 0) {
//                        var count = Math.abs(variance);
//                        System.out.printf("Removing %d %s device(s) from machine %s", count, resType._tag, machName);
//                        removeDevicesFromMachine(groupId, machId, resType, count);
//                    }
//                } catch (LiqidException lex) {
//                    try {
//                        _liqidClient.cancelReprogramFabric(machId);
//                    } catch (LiqidException lex2) {
//                        // can't do anything about it
//                    }
//                    throw lex;
//                }
//            }
//            _liqidClient.reprogramFabric(machId);
//
//            System.out.printf("Uncordoning %s...\n", nodeName);
//            kubeClient.uncordonNode(nodeName);
//        }
//
//        // Add resources from machines as appropriate.
//        for (var machName : group2) {
//            var nodeName = _ContextForUpdate._nodeNamesByMachineName.get(machName);
//            System.out.printf("Draining %s...\n", nodeName);
//            kubeClient.cordonNode(nodeName);
//            kubeClient.evictPodsForNode(nodeName, true);
//
//            var machId = _ContextForUpdate._liqidMachinesByName.get(machName).getMachineId();
//            _liqidClient.editFabric(machId);
//            var resMap = resourceVariances.get(machName);
//            for (var entry : resMap.entrySet()) {
//                var resType = entry.getKey();
//                var variance = entry.getValue();
//                try {
//                    if (variance > 0) {
//                        System.out.printf("Attaching %d %s device(s) from machine %s", variance, resType._tag, machName);
//                        addDevicesToMachine(groupId, machId, resType, variance);
//                    }
//                } catch (LiqidException lex) {
//                    try {
//                        _liqidClient.cancelReprogramFabric(machId);
//                    } catch (LiqidException lex2) {
//                        // can't do anything about it
//                    }
//                    throw lex;
//                }
//            }
//            _liqidClient.reprogramFabric(machId);
//            var gpus = _liqidClient.getDevices(DeviceQueryType.GPU, groupId, machId);
//            if (gpus.size() > 1) {
//                System.out.printf("Enabling P2P for machine %s...\n", machName);
//                _liqidClient.setP2PEnabled(machId, P2PType.ON);
//            }
//
//            System.out.printf("Uncordoning %s...\n", nodeName);
//            kubeClient.uncordonNode(nodeName);
//        }
//
//        // Move whatever resources remain in the group free pool, back out to the system.
//        var allDevs = _liqidClient.getAllDevicesStatus();
//        var devStatsByName = allDevs.stream().collect(Collectors.toMap(DeviceStatus::getName,
//                                                                       devStat -> devStat,
//                                                                       (a, b) -> b,
//                                                                       HashMap::new));
//
//        var preDevices = _liqidClient.getUnattachedDevicesForGroup(null, groupId);
//        if (preDevices.size() > 0) {
//            _liqidClient.groupPoolEdit(groupId);
//            for (var dev : preDevices) {
//                try {
//                    var devStat = devStatsByName.get(dev.getDeviceName());
//                    System.out.printf("Removing %s device %s from group...\n",
//                                      devStat.getDeviceType(),
//                                      devStat.getName());
//                    _liqidClient.removeDeviceFromGroup(devStat.getDeviceId(), groupId);
//                } catch (Exception ex) {
//                    // nothing to be done, just keep going
//                }
//            }
//            _liqidClient.groupPoolDone(groupId);
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }

//    private void removeDevicesFromMachine(
//        final Integer groupId,
//        final Integer machId,
//        final ResourceType resType,
//        final Integer count
//    ) throws ProcessingException, LiqidException {
//        var fn = "removeDevicesFromMachine";
//        _logger.trace(String.format("Entering %s groupId=%d machId=%d resType=%s count=%d",
//                                    fn, groupId, machId, resType, count));
//
//        var preDevs = _liqidClient.getDevices(resType._queryType, groupId, machId);
//        if (preDevs.size() < count) {
//            var msg = String.format("Internal error - we have %d %s devices but we need %d",
//                                    preDevs.size(), resType._tag, count);
//            var t = new ProcessingException(msg);
//            _logger.throwing(t);
//            throw t;
//        }
//
//        for (int dx = 0; dx < count; ++dx) {
//            var preDev = preDevs.pop();
//            var dev = _ContextForUpdate._allDevicesStatusByName.get(preDev.getDeviceName());
//            _liqidClient.removeDeviceFromMachine(dev.getDeviceId(), groupId, machId);
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }
//
//    /**
//     * Compares results of the wanted configuration with the existing layout
//     * to determine if there are any variances between the general node naming
//     * and the host deployments.
//     * Returns a list of user-readable strings detailing the variances found...
//     * if there are none, then the resulting list is empty.
//     */
//    private List<String> resolveCPUsAndMachines(
//        final K8SConfiguration configuration,
//        final Layout existingLayout
//    ) {
//        var fn = "resolveCPUsAndMachines";
//        _logger.trace(String.format("Entering %s config=%s, layout=%s", fn, configuration, existingLayout));
//
//        var result = new LinkedList<String>();
//
//        var pcpuNameToNodeName = new HashMap<String, String>();
//        var nodeNameToPcpuName = new HashMap<String, String>();
//        for (var configNode : configuration.getNodes()) {
//            var pcpuName = configNode._liqidPCPUName;
//            var nodeName = configNode._k8sNodeName;
//            pcpuNameToNodeName.put(pcpuName, nodeName);
//            nodeNameToPcpuName.put(nodeName, pcpuName);
//        }
//
//        var machineNameToCPUStatus = new HashMap<String, DeviceStatus>();
//        for (var machInfo : existingLayout._machineInfos) {
//            machineNameToCPUStatus.put(machInfo.getMachineName(), machInfo.getComputeDeviceStatus());
//        }
//
//        var machineNamePrefix = LIQID_K8S_GROUP_NAME + "-";
//        for (var entry : pcpuNameToNodeName.entrySet()) {
//            var pcpuName = entry.getKey();
//            var nodeName = entry.getValue();
//            var expectedMachineName = machineNamePrefix + nodeName;
//            if (!machineNameToCPUStatus.containsKey(expectedMachineName)) {
//                var msg = String.format("Machine name '%s' composed from information in config is not found in layout.",
//                                        expectedMachineName);
//                result.add(msg);
//            } else {
//                var cpuStat = machineNameToCPUStatus.get(expectedMachineName);
//                if (!pcpuName.equals(cpuStat.getName())) {
//                    var msg = String.format("PCPU name '%s' from config does not match machine '%s' pcpu name '%s'.",
//                                            pcpuName, expectedMachineName, cpuStat.getName());
//                    result.add(msg);
//                }
//            }
//        }
//
//        for (var entry : machineNameToCPUStatus.entrySet()) {
//            var machName = entry.getKey();
//            var cpuStat = entry.getValue();
//            if (!machName.startsWith(machineNamePrefix)) {
//                var msg = String.format("Machine name '%s' from Liqid config does not start with '%s'.",
//                                        machName,
//                                        LIQID_K8S_GROUP_NAME);
//                result.add(msg);
//                continue;
//            }
//
//            var expectedPCPUName = cpuStat.getName();
//            var expectedNodeName = machName.substring(LIQID_K8S_GROUP_NAME.length() + 1);
//            if (!pcpuNameToNodeName.containsKey(expectedPCPUName)) {
//                var msg = String.format("For machine '%s', node name '%s' si not in the requested configuration.",
//                                        machName,
//                                        expectedPCPUName);
//                result.add(msg);
//            } else {
//                if (!nodeNameToPcpuName.containsKey(expectedNodeName)
//                    || !nodeNameToPcpuName.get(expectedNodeName).equals(expectedPCPUName)) {
//                    var msg = String.format("For machine '%s', node '%s' is not associated with port '%s'.",
//                                            machName,
//                                            expectedNodeName,
//                                            expectedPCPUName);
//                    result.add(msg);
//                }
//            }
//
//            if (!nodeNameToPcpuName.containsKey(expectedNodeName)) {
//                var msg = String.format("For machine '%s', node name '%s' is not in the requested configuration.",
//                                        machName,
//                                        expectedNodeName);
//                result.add(msg);
//            } else {
//                if (!pcpuNameToNodeName.containsKey(expectedPCPUName)
//                    || !pcpuNameToNodeName.get(expectedPCPUName).equals(expectedNodeName)) {
//                    var msg = String.format("For machine '%s', port '%s' is not associated with node '%s'.",
//                                            machName,
//                                            expectedPCPUName,
//                                            expectedNodeName);
//                    result.add(msg);
//                }
//            }
//        }
//
//        _logger.trace(String.format("%s returning %s", fn, result));
//        return result;
//    }

    void process() throws K8SException, LiqidException, ScriptException {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        var command = switch (_commandType) {
//            case COMPOSE ->
//                new ComposeCommand(_logger, _proxyURL, _timeoutInSeconds);
            case INITIALIZE ->
                new InitializeCommand(_logger, _force, _timeoutInSeconds)
                    .setAllocate(_allocate)
                    .setLiqidAddress(_liqidAddress)
                    .setLiqidGroupName(_liqidGroupName)
                    .setLiqidPassword(_liqidPassword)
                    .setLiqidUsername(_liqidUsername)
                    .setProcessorSpecs(_processorSpecs)
                    .setProxyURL(_proxyURL)
                    .setResourceSpecs(_resourceSpecs);
            case LINK ->
                new LinkCommand(_logger, _force, _timeoutInSeconds)
                    .setLiqidAddress(_liqidAddress)
                    .setLiqidGroupName(_liqidGroupName)
                    .setLiqidPassword(_liqidPassword)
                    .setLiqidUsername(_liqidUsername)
                    .setProxyURL(_proxyURL);
            case NODES ->
                new NodesCommand(_logger, _force, _timeoutInSeconds)
                    .setProxyURL(_proxyURL);
//            case RECONFIGURE ->
//                new ReconfigureCommand(_logger, _proxyURL, _timeoutInSeconds);
            case RESET ->
                new ResetCommand(_logger, _force, _timeoutInSeconds)
                    .setLiqidAddress(_liqidAddress)
                    .setLiqidPassword(_liqidPassword)
                    .setLiqidUsername(_liqidUsername)
                    .setProxyURL(_proxyURL);
            case RESOURCES ->
                new ResourcesCommand(_logger, _force, _timeoutInSeconds)
                    .setLiqidAddress(_liqidAddress)
                    .setLiqidPassword(_liqidPassword)
                    .setLiqidUsername(_liqidUsername);
            case UNLINK ->
                new UnlinkCommand(_logger, _force, _timeoutInSeconds)
                    .setProxyURL(_proxyURL);
//            case VALIDATE ->
//                new ValidateCommand(_logger, _proxyURL, _timeoutInSeconds);
        };

        var plan = command.process();
        if (plan != null) {
            // commands which do not update anything may not create a plan
            plan.show();
            if (!_noUpdate) {
                plan.execute(command.getK8SClient(), command.getLiqidClient(), _logger);
            }
        }

        if ((command.getLiqidClient() != null) && (command.getLiqidClient().isLoggedIn())) {
            try {
                command.getLiqidClient().logout();
            } catch (LiqidException lex) {
                _logger.catching(lex);
            }
        }

        var noUpStr = _noUpdate ? "with no update " : "";
        System.out.printf("--- %s command completed successfully %s---\n", _commandType.getToken(), noUpStr);
        _logger.trace("Exiting %s", fn);
    }
}
