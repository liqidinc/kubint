/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.*;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.layout.GeneralType;
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.liqid.k8s.Constants.*;

/**
 * Abstract class for all command handlers.
 * Most of the really useful functionality exists in this class, with the subclasses simply calling back
 * if and as necessary.
 */
public abstract class Command {

    protected static final Map<GeneralType, String> ANNOTATION_KEY_FOR_DEVICE_TYPE = new HashMap<>();
    static {
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.FPGA, K8S_ANNOTATION_FPGA_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.GPU, K8S_ANNOTATION_GPU_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.MEMORY, K8S_ANNOTATION_MEMORY_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.LINK, K8S_ANNOTATION_LINK_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.SSD, K8S_ANNOTATION_SSD_ENTRY);
    }

    protected final Logger _logger;
    protected final Boolean _force;
    protected final Integer _timeoutInSeconds;

    protected String _liqidAddress;
    protected String _liqidGroupName;
    protected String _liqidPassword;
    protected String _liqidUsername;
    protected String _proxyURL;

    protected K8SClient _k8sClient;
    protected LiqidClient _liqidClient;
    protected LiqidInventory _liqidInventory;

    protected Command(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        _logger = logger;
        _force = force;
        _timeoutInSeconds = timeoutInSeconds;
    }

    /**
     * Instructs the subclass to process itself given sufficient information
     * @return a Plan if processing was able to produce one
     * @throws ConfigurationException Indicates an inconsistency in the general configuration of the Liqid Cluster
     *                                  or the Kubernetes Cluster
     * @throws ConfigurationDataException Indicates a problem in the actual liqid-supplied data stored in the
     *                                      Liqid Cluster or the Kubernetes Cluster
     * @throws InternalErrorException Indicates that some error has been detected which is likely caused by an error
     *                                  in programming
     * @throws K8SHTTPError Indicates that the Kubernetes API returned an HTTP status which we did not expect
     * @throws K8SJSONError Indicates that the Kubernetes API returned information in a format which we did not expect
     * @throws K8SRequestError Indicates some problem was encountered by the Kubernetes API library - this could be
     *                          a programming problem, or it could be something wrong in the Kubernetes API.
     * @throws LiqidException Indicates that a general error occurred while interacting with the Liqid Director API.
     */
    public abstract Plan process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException;

    public K8SClient getK8SClient() { return _k8sClient; }
    public LiqidClient getLiqidClient() { return _liqidClient; }

//    /**
//     * Creates steps to allocate resources as equally as possible among the known worker nodes.
//     * We assume that all relevant resources are in the containing group, and *not* already assigned to any machines.
//     * The practical result is that we add actions to the given plan which
//     *      annotate worker nodes to indicate the generic resource counts for each node
//     *      (optionally) attaches corresponding resources to the machines which contain the worker node cpus
//     * @param inventory the inventory to be used - in some cases we may wish to use some inventory which is not
//     *                  the official as-existing inventory.
//     * @param computeDevices container which indicates the compute devices of interest, along with their nodes
//     * @param resourceDevices container which indicates the resource devices of interest
//     * @param createAssignActions true if we should create actions to assign the devices, false for only annotations
//     * @param plan where we store Action objects to implement our plan
//     */
//    protected void allocateEqually(
//        final LiqidInventory inventory,
//        final Map<DeviceItem, Node> computeDevices,
//        final Collection<DeviceItem> resourceDevices,
//        final boolean createAssignActions,
//        final Plan plan
//    ) {
//        var fn = "allocateEqually";
//        _logger.trace("Entering %s with inv=%s compDevs=%s resDevs=%s asgActs=%s",
//                      fn, inventory, computeDevices, resourceDevices, createAssignActions);
//
//        var groupId = inventory.getGroupId(_liqidGroupName);
//        var devsByType = LiqidInventory.segregateByType(inventory.getDevicesInGroup(groupId));
//        if (computeDevices.isEmpty() || resourceDevices.isEmpty()) {
//            _logger.trace("Exiting %s with nothing to do", fn);
//            return;
//        }
//
//        //  We do this by machine, because it is much more efficient to attach all the devices for the machine
//        //  in one step. We need to iterate over the computeDevices to get there, though.
//        var remainingMachineCount = computeDevices.size();
//        for (var entry : computeDevices.entrySet()) {
//            var compItem = entry.getKey();
//            var node = entry.getValue();
//            var machId = inventory.getMachineIdForDevice(compItem.getStatus().getDeviceId());
//            var machine = inventory.getMachine(machId);
//
//            //  annotations and device names for this machine
//            var annoAction = new AnnotateNode().setNodeName(node.getName());
//            var assignAction = new AssignToMachine().setMachineName(machine.getMachineName())
//                                                    .setNodeName(node.getName());
//
//            //  loop over each type of device - for each type we have all the still-to-be-assigned
//            //  devices of that type.
//            for (var subEntry : devsByType.entrySet()) {
//                var genType = subEntry.getKey();
//                var devItems = subEntry.getValue();
//
//                //  how many for this machine?
//                var resCount = devItems.size() / remainingMachineCount;
//                if (devItems.size() % remainingMachineCount > 0) {
//                    resCount++;
//                }
//
//                //  Create annotation for the indicated count
//                var annoKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType);
//                var annoValue = String.format("%d", resCount);
//                annoAction.addAnnotation(annoKey, annoValue);
//
//                //  copy names to the devNames set, and remove the corresponding devItem objects
//                var iter = devItems.iterator();
//                while (iter.hasNext() && resCount > 0) {
//                    var di = iter.next();
//                    assignAction.addDeviceName(di.getStatus().getName());
//                    iter.remove();
//                    resCount--;
//                }
//            }
//
//            plan.addAction(annoAction);
//            if (createAssignActions) {
//                plan.addAction(assignAction);
//            }
//        }
//
//        _logger.trace("Exiting %s", fn);
//    }
//
//    /**
//     * Checks the current Liqid and Kubernetes configuration to see if there is anything which would prevent
//     * us from executing the requested command. Some problems can be warnings if _force is set.
//     * In any case where processing cannot continue, we will throw an exception.
//     * This is common code for the Initialize and Adopt commands.
//     * @param computeDevices a map of DeviceItem objects representing compute resources, to the corresponding
//     *                       Node objects representing k8s worker nodes.
//     * @return true if no errors were detected
//     */
//    protected boolean checkConfiguration(
//        final Map<DeviceItem, Node> computeDevices
//    ) {
//        var fn = "checkConfiguration";
//        _logger.trace("Entering %s", fn);
//
//        // Are there any compute device descriptions which contradict the node names?
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//        for (var entry : computeDevices.entrySet()) {
//            var devItem = entry.getKey();
//            var node = entry.getValue();
//            var desc = devItem.getInfo().getUserDescription();
//            if (!(desc.equals("n/a") || desc.equals(node.getName()))) {
//                System.err.printf("%s:Description for resource %s conflicts with node name %s",
//                                  errPrefix,
//                                  devItem.getStatus().getName(),
//                                  node.getName());
//                errors = true;
//            }
//        }
//
//        var result = !errors;
//        _logger.trace("Exiting %s with %s", fn, result);
//        return result;
//    }
//
//    /**
//     * Helpful wrapper to create a full annotation key.
//     * @param keySuffix the definitive portion of the key
//     * @return the full key, with the company prefix applied to the front of the suffix
//     */
//    protected String createAnnotationKeyFor(
//        final String keySuffix
//    ) {
//        return String.format("%s/%s", K8S_ANNOTATION_PREFIX, keySuffix);
//    }
//
//    /**
//     * Helpful wrapper to create a full annotation key for a device-specific resource count.
//     * @param genType the general type of interest
//     * @return the full key for this type of resource counter (with the company prefix applied)
//     */
//    protected String createAnnotationKeyForDeviceType(
//        final LiqidGeneralType genType
//    ) {
//        return createAnnotationKeyFor(ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType));
//    }
//
//    /**
//     * Creates a machine name for a combination of the compute device name and the k8s node name.
//     * This is somewhat arbitrary, but it should be used whenever a machine name needs to be associated with
//     * a particular compute device and worker node combination.
//     * We ensure (to a reasonable extend) that the generated name is valid for a Liqid Cluster Machine.
//     * @param devStat SDK DeviceStatus object representing a compute device
//     * @param node Node object representing the k8w worker node corresponding to the compute device
//     * @return name to be used for the Liqid Cluster machine.
//     */
//    protected String createMachineName(
//        final DeviceStatus devStat,
//        final Node node
//    ) {
//        var devName = devStat.getName();
//        var nodeName = node.getName();
//        var machName = String.format("%s-%s", devName, nodeName);
//        if (machName.length() > 22) {
//            machName = machName.substring(0, 22);
//        }
//
//        return machName;
//    }
//
//    /**
//     * Add actions to a plan which effect the creation of Liqid Cluster machines.
//     * These actions create the machine, add the appropriate compute device to the machine,
//     * and set the compute device user description to the appropriate k8s worker node name.
//     * We assume the compute devices have already been added to the targeted group.
//     * @param plan the plan which we populate
//     * @param computeDevices a map of DeviceItem objects representing compute resources, to the corresponding
//     *                       Node objects representing k8s worker nodes.
//     */
//    protected void createMachines(
//        final Plan plan,
//        final Map<DeviceItem, Node> computeDevices
//    ) {
//        // We're going to do it in order by pcpu{n} name, just because it is cleaner.
//        var orderedMap = new TreeMap<Integer, DeviceItem>();
//        for (var devItem : computeDevices.keySet()) {
//            Integer key = Integer.parseInt(devItem.getStatus().getName().substring(4));
//            orderedMap.put(key, devItem);
//        }
//
//        for (var entry : orderedMap.entrySet()) {
//            var devItem = entry.getValue();
//            var node = computeDevices.get(devItem);
//            var devName = devItem.getStatus().getName();
//            var nodeName = node.getName();
//            var machineName = createMachineName(devItem.getStatus(), node);
//
//            plan.addAction(new CreateMachine().setMachineName(machineName).setGroupName(_liqidGroupName));
//            plan.addAction(new AssignToMachine().setMachineName(machineName).addDeviceName(devName));
//            plan.addAction(new SetUserDescription().setDeviceName(devName).setDescription(nodeName));
//            plan.addAction(new AnnotateNode().setNodeName(nodeName)
//                                             .addAnnotation(Constants.K8S_ANNOTATION_MACHINE_NAME, machineName));
//        }
//    }

    /**
     * Creates a new Logger based on our current logger, which does NOT log to stdout or stderr.
     * Use for initializing lower-level libraries which you do not want to engage in the same verbosity
     * as the main code.
     * @param name name for the new logger
     * @return new Logger object
     */
    private Logger createSubLogger(
        final String name
    ) {
        var newLogger = new Logger(name, _logger);
        for (var w : newLogger.getWriters()) {
            if ((w instanceof StdOutWriter) || (w instanceof StdErrWriter)) {
                newLogger.removeWriter(w);
            }
        }
        return newLogger;
    }

//    /**
//     * Given a particular inventory, we create a map which contains a set of DeviceItem keys for all the compute
//     * devices in the inventory which have user descriptions, pointing to values of those descriptions which
//     * are taken to be node names of k8s worker nodes.
//     * @return
//     */
//    protected boolean developComputeList(
//        final LiqidInventory inventory
//    ) {
//
//    }
//
//    /**
//     * Based on processor specifications, we populate containers of compute device information.
//     * @param inventory LiqidInventory object we use as a reference
//     * @param processorSpecs list of processor specifications which tie compute resources to k8s nodes
//     *                       format is {deviceName} ':' {nodeName}
//     * @param computeDevices a map of DeviceItem objects which we create based on the processorSpecs praameter,
//     *                       which represents compute resources with their corresponding k8s Node objects.
//     * @return true if successful, else false
//     */
//    protected boolean developComputeList(
//        final LiqidInventory inventory,
//        final Collection<String> processorSpecs,
//        final Map<DeviceItem, Node> computeDevices
//    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
//        var fn = "developDeviceList";
//        _logger.trace("Entering %s with processorSpecs=%s", fn, processorSpecs);
//
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//
//        for (var spec : processorSpecs) {
//            var split = spec.split(":");
//            if (split.length != 2) {
//                System.err.printf("ERROR:Invalid format for spec '%s'\n", spec);
//                errors = true;
//            }
//
//            var devName = split[0];
//            var nodeName = split[1];
//
//            var devItem = inventory.getDevice(devName);
//            if (devItem == null) {
//                System.err.printf("%s:Compute resource '%s' is not in the Liqid Cluster\n", errPrefix, devName);
//                errors = true;
//            }
//
//            Node node = null;
//            try {
//                node = _k8sClient.getNode(nodeName);
//            } catch (K8SHTTPError ex) {
//                if (ex.getResponseCode() == 404) {
//                    System.err.printf("%s:Worker node '%s' is not in the Kubernetes Cluster\n", errPrefix, nodeName);
//                    errors = true;
//                } else {
//                    throw ex;
//                }
//            }
//
//            if ((devItem != null) && (node != null)) {
//                computeDevices.put(devItem, node);
//            }
//        }
//
//        var result = !errors;
//        _logger.trace("Exiting %s with %s, computeDevices=%s", fn, errors, computeDevices);
//        return result;
//    }
//
//    /**
//     * Based on resource specifications (i.e., device names) we develop a collection of corresponding
//     * DeviceItem objects based on the provided LiqidInventory object
//     * @param inventory LiqidInventory object we use as a reference
//     * @param resourceSpecs list of resource (device) names - does NOT include compute resources
//     * @param resourceDevices collection which we populate with DeviceItem objects
//     * @return true if successful, else false
//     */
//    protected boolean developDeviceList(
//        final LiqidInventory inventory,
//        final Collection<String> resourceSpecs,
//        final Collection<DeviceItem> resourceDevices
//    ) {
//        var fn = "developDeviceList";
//        _logger.trace("Entering %s with resourceSpecs=%s", fn, resourceSpecs);
//
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//
//        for (var spec : resourceSpecs) {
//            var devItem = inventory.getDevice(spec);
//            if (devItem == null) {
//                System.err.printf("%s:Resource '%s' is not in the Liqid Cluster\n", errPrefix, spec);
//                errors = true;
//            } else {
//                resourceDevices.add(devItem);
//            }
//        }
//
//        var result = !errors;
//        _logger.trace("Exiting %s with %s, resourceDevices=%s", fn, errors, resourceDevices);
//        return result;
//    }
//
//    /**
//     * Extracts the device names from a list of DeviceItem objects.
//     * There is no guarantee that the names will be in any particular order, particularly compared with the
//     * input collection.
//     * @param items collection of DeviceItem objects
//     * @return collection of strings
//     */
//    protected Collection<String> getDeviceNames(
//        final Collection<DeviceItem> items
//    ) {
//        return items.stream()
//                    .map(i -> i.getStatus().getName())
//                    .collect(Collectors.toCollection(HashSet::new));
//    }
//
//    /**
//     * Retrieves a short string to be used in error/warning messages.
//     * Anomalies are often warnings if -force is set, but errors otherwise
//     * @return message prefix
//     */
//    protected String getErrorPrefix() {
//        return _force ? "WARNING" : "ERROR";
//    }
//
//    /**
//     * Given two sets of the same type, we populate a third set of that type with
//     * only those items which are contained in both of the original sets.
//     * @param set1 first contributing set
//     * @param set2 second contributing set
//     * @param intersection result set.
//     * @param <T> item type
//     */
//    protected <T> void getIntersection(
//        final Collection<T> set1,
//        final Collection<T> set2,
//        final Collection<T> intersection
//    ) {
//        intersection.clear();
//        set1.stream().filter(set2::contains).forEach(intersection::add);
//    }
//
//    /**
//     * This code solicits the information we need to interact with the Liqid Cluster from the k8s database.
//     * It presumes that the k8s cluster is suitably linked to a Liqid Cluster.
//     * Such linkage exists in the form of a ConfigMap and an optional Secret.
//     * The specific bits of information returned include:
//     *      IP address of the Liqid Cluster (actually, of the director)
//     *      Group name of the Liqid Cluster group to which all relevant resources do, or should, belong.
//     *      Username credential if basic authentication is enabled for the Liqid cluster
//     *      Password credential if basic authentication is enabled for the Liqid cluster,
//     *          although we do account for the possibility of a null password for a sadly unprotected username.
//     * @throws ConfigurationDataException Indicates something is wrong in the actual bits of information stored in
//     *                                      the configmap or the secret.
//     * @throws K8SHTTPError If any unexpected HTTP responses are received. Generally, we expect only 200s.
//     * @throws K8SJSONError If information received from k8s cannot be converted from JSON into the expected data
//     *                          structs. This generally indicates a programming error on our part, but it could also
//     *                          result from gratuitous changes in k8s, which does unfortunately occur.
//     * @throws K8SRequestError Indicates some other error during processing, from within the k8sClient module.
//     */
//    protected void getLiqidLinkage(
//    ) throws ConfigurationDataException, K8SHTTPError, K8SJSONError, K8SRequestError {
//        var fn = "getLiqidLinkage";
//        _logger.trace("Entering %s", fn);
//
//        var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
//        _liqidAddress = cfgMap.data.get(K8S_CONFIG_MAP_IP_ADDRESS_KEY);
//        _liqidGroupName = cfgMap.data.get(K8S_CONFIG_MAP_GROUP_NAME_KEY);
//
//        _liqidUsername = null;
//        _liqidPassword = null;
//        try {
//            var secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
//            var creds = new CredentialMangler(secret.data.get(K8S_SECRET_CREDENTIALS_KEY));
//            _liqidUsername = creds.getUsername();
//            _liqidPassword = creds.getPassword();
//        } catch (K8SHTTPError kex) {
//            // a 404 is okay - there might not be any credentials. Anything else gets rethrown.
//            if (kex.getResponseCode() != 404) {
//                throw kex;
//            }
//        }
//
//        _logger.trace("Exiting %s", fn);
//    }

    protected boolean hasAnnotations() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "hasAnnotations";
        _logger.trace("Entering %s", fn);

        var nodeEntities = _k8sClient.getNodes();
        for (var node : nodeEntities) {
            var annos = node.metadata.annotations;
            for (var key : annos.keySet()) {
                if (key.startsWith(K8S_ANNOTATION_PREFIX)) {
                    _logger.trace("Exiting %s with true", fn);
                    return true;
                }
            }
        }

        _logger.trace("Exiting %s with false", fn);
        return false;
    }

    /**
     * Checks to see whether linkage (either or both of configMap and secret) exists
     * @return true if linkage exists, else false
     */
    protected boolean hasLinkage() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "hasLinkage";
        _logger.trace("Entering %s", fn);

        var result = false;
        try {
            var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            if (cfgMap != null) {
                result = true;
            }
        } catch (K8SHTTPError ex) {
            //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        if (!result) {
            try {
                var secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
                if (secret != null) {
                    result = true;
                }
            } catch (K8SHTTPError ex) {
                //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
                if (ex.getResponseCode() != 404) {
                    throw ex;
                }
            }
        }

        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    /**
     * Initializes a k8sClient object and stores the reference in our local attribute for the client.
     */
    protected void initK8sClient() throws K8SException {
        var fn = "initK8sClient";
        _logger.trace("Entering %s", fn);

        try {
            _k8sClient = new K8SClient(_proxyURL, createSubLogger(_logger.getName()));
        } catch (IOException ex) {
            _logger.catching(ex);
            var ex2 = new K8SException("Caught:" + ex.getMessage());
            _logger.throwing(ex2);
            throw ex2;
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Initializes a LiqidClient object and stores the reference in our local attribute for the client.
     * Address and credentials must be set ahead of time, either from command line parameters
     * or from linkage information.
     */
    protected void initLiqidClient() throws InternalErrorException, LiqidException {
        var fn = "initLiqidClient";
        _logger.trace("Entering %s", fn);

        try {
            _liqidClient = new LiqidClientBuilder().setHostAddress(_liqidAddress)
                                                   .setTimeoutInSeconds(_timeoutInSeconds)
                                                   .build();
        } catch (LiqidException ex) {
            _logger.catching(ex);
            var ex2 = new InternalErrorException("Caught:" + ex.getMessage());
            _logger.throwing(ex2);
            throw ex2;
        }

        _liqidClient.setLogger(createSubLogger("LiqidSDK"));
        if (_liqidUsername != null) {
            _liqidClient.login(LIQID_SDK_LABEL, _liqidUsername, _liqidPassword);
        }

        _liqidInventory = LiqidInventory.createLiqidInventory(_liqidClient);
        _logger.trace("Exiting %s", fn);
    }

//    /**
//     * Adds actions to the given plan to efficiently release all indicated devices from their containing groups.
//     * Does NOT actually do anything else.
//     * @param inventory the LiqidInventory which we reference
//     * @param devices collection of DeviceItem objects to be removed from their containing groups
//     * @param plan plan which we populate
//     */
//    protected void releaseDevicesFromGroups(
//        final LiqidInventory inventory,
//        final Collection<DeviceItem> devices,
//        final Plan plan
//    ) {
//        var fn = "releaseDevicesFromGroups";
//        _logger.trace("Entering %s with inventory=%s devices=%s", fn, inventory, devices);
//
//        // Iterate over the groups so that we can do multiple devices per group.
//        // In the case where we're removing all the devices for a group, just delete the group.
//        for (var group : inventory.getGroups()) {
//                var grpDevs = inventory.getDevicesInGroup(group.getGroupId());
//                Set<DeviceItem> devsToRemove = new HashSet<>();
//                getIntersection(devices, grpDevs, devsToRemove);
//
//                if (!devsToRemove.isEmpty()) {
//                    if (devsToRemove.size() == grpDevs.size()) {
//                        plan.addAction(new DeleteGroup().setGroupName(group.getGroupName()));
//                    } else {
//                        var names = getDeviceNames(devsToRemove);
//                        plan.addAction(new RemoveFromGroup().setGroupName(group.getGroupName())
//                                                            .setDeviceNames(names));
//                    }
//                }
//        }
//
//        _logger.trace("Exiting %s with plan=%s", fn, plan);
//    }
//
//    /**
//     * Adds actions to the given plan to efficiently release all indicated devices from their containing machines.
//     * Does NOT actually do anything else.
//     * @param inventory the LiqidInventory which we reference
//     * @param devices collection of DeviceItem objects to be removed from their containing machines
//     * @param plan plan which we populate
//     */
//    protected void releaseDevicesFromMachines(
//        final LiqidInventory inventory,
//        final Collection<DeviceItem> devices,
//        final Plan plan
//    ) {
//        var fn = "releaseDevicesFromMachines";
//        _logger.trace("Entering %s with inventory=%s devices=%s", fn, inventory, devices);
//
//        // Iterate over the machines so that we can do multiple devices per machine.
//        // In the case where we're removing all the devices for a machine, just delete the machine.
//        for (var mach : inventory.getMachines()) {
//            var machDevs = inventory.getDevicesInMachine(mach.getMachineId());
//            Set<DeviceItem> devsToRemove = new HashSet<>();
//            getIntersection(devices, machDevs, devsToRemove);
//
//            if (!devsToRemove.isEmpty()) {
//                if (devsToRemove.size() == machDevs.size()) {
//                    plan.addAction(new DeleteMachine().setMachineName(mach.getMachineName()));
//                } else {
//                    var names = getDeviceNames(devsToRemove);
//                    plan.addAction(new RemoveFromMachine().setMachineName(mach.getMachineName())
//                                                          .setDeviceNames(names));
//                }
//            }
//        }
//
//        _logger.trace("Exiting %s with plan=%s", fn, plan);
//    }
}
