/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.*;
import com.liqid.k8s.CredentialMangler;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.layout.*;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.*;
import com.liqid.sdk.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
    protected Boolean _force;
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

    /**
     * Check for conflicts in the current Liqid / K8S configurations
     * @return true if we are okay, false if errors exist
     */
    protected boolean checkForContradictions(
        final Map<DeviceItem, Node> computeDevices,
        final Collection<DeviceItem> resourceDevices
    ) {
        var fn = "checkForContradictions";
        _logger.trace("Entering %s compDevs=%s resDevs=%s", fn, computeDevices, resourceDevices);

        var errors = false;
        var errPrefix = getErrorPrefix();

        // Are there any compute device descriptions which contradict the node names?
        for (var entry : computeDevices.entrySet()) {
            var devItem = entry.getKey();
            var node = entry.getValue();
            var desc = devItem.getDeviceInfo().getUserDescription();
            if ((desc != null) && (!desc.equals("n/a")) && !desc.equals(node.getName())) {
                System.err.printf("%s:User description for device '%s' is not set to the corresponding node name '%s'\n",
                                  errPrefix,
                                  devItem.getDeviceName(),
                                  node.getName());
            }

            var machineAnnoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machineName = node.metadata.annotations.get(machineAnnoKey);
            if (machineName != null) {
                var machine = _liqidInventory.getMachine(machineName);
                if (machine == null) {
                    System.err.printf("%s:node name '%s' has an incorrect annotation referencing non-existant machine name '%s'\n",
                                      errPrefix,
                                      node.getName(),
                                      machineName);
                    errors = true;
                } else {
                    var computeName = machine.getComputeName();
                    if (computeName == null) {
                        System.err.printf("%s:node name '%s' refers to machine '%s' which has no compute resource\n",
                                          errPrefix,
                                          node.getName(),
                                          machine);
                        errors = true;
                    } else if (!computeName.equals(devItem.getDeviceName())) {
                        System.err.printf("%s:node name '%s' refers to machine '%s' which has compute resource '%s instead of '%s'\n",
                                          errPrefix,
                                          node.getName(),
                                          machine,
                                          computeName,
                                          devItem.getDeviceName());
                        errors = true;
                    }
                }
            }
        }

        var result = !errors;
        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * Creates a map of allocations based on the given allocators.
     * --[ This is the point where we convert types/vendors/models/counts into actual device identifiers. ]--
     * The allocators describe, in order of ResourceModel specificity, a number of allocators which describe, per resModel
     * per machine, the number of requested devices (which could be zero) which are described by the resModel for that machine,
     * along with a list of all the potential candidate identifiers in order of preference, for satisfying that request.
     * Our job is to choose the best device identifiers based on the preference, for each allocator, producing an allocation
     * of device identifiers per machine. At this point, order of preference or resource model is no longer relevant, so we
     * simply return a map of machine name to an Allocation object for that machine.
     * @param allocators ordered map of allocators
     * @return unordered map of machines -> allocations if successful, null if errors are detected and we are not forcing
     */
    protected Collection<Allocation> createAllocations(
        final Map<ResourceModel, Collection<Allocator>> allocators
    ) {
        var fn = "createAllocations";
        _logger.trace("Entering %s with allocators=%s", fn, allocators);

        var errors = false;
        var errPrefix = getErrorPrefix();

        // key is machine name
        var allocations = new HashMap<String, Allocation>();
        var chosenIds = new HashSet<Integer>();
        for (var entry : allocators.entrySet()) {
            // per res model
            var resModel = entry.getKey();
            var allocs = entry.getValue();
            for (var alloc : allocs) {
                // per machine
                var newDeviceIds = new HashSet<Integer>();
                var machineName = alloc.getMachineName();
                var count = alloc.getCount();
                var selectionSet = alloc.getDeviceIdentifiers();
                while (count > 0) {
                    if (selectionSet.isEmpty()) {
                        System.out.printf("%s:Out of potential device identifiers for machine %s resmodel %s\n",
                                          errPrefix, machineName, resModel);
                        errors = true;
                        break;
                    }

                    var id = selectionSet.removeFirst();
                    if (!chosenIds.contains(id)) {
                        newDeviceIds.add(id);
                        chosenIds.add(id);
                        count--;
                    }
                }

                if (!allocations.containsKey(machineName)) {
                    allocations.put(machineName, new Allocation(machineName));
                }
                allocations.get(machineName).appendDeviceIdentifiers(newDeviceIds);
            }
        }

        var result = (errors && !_force) ? null : allocations.values();
        _logger.trace("%s returning with %s", fn, result);
        return result;
    }

    /**
     * Given an inventory of the current Liqid configuration and a desired layout, we populate our allocations list
     * with Allocation objects describing the potential devices for each ResourceModel entry indicated in the
     * desired layout.
     * This is auto-magically sorted such that the most specific resource models are first in line, followed
     * by the less-restrictive, and finishing with the least-restrictive. (Auto-magically, because our content
     * is sorted by ResourceModel, which has a compareTo() method implementing this sorting).
     * @param inventory LiqidInventory which sources the devices we consider
     * @param desiredLayout ClusterLayout which describes the layout wanted by the user
     * @return ordered map (ordered by ResourceModel ordering, which prioritizes specific allocations, then vendor, then generic)
     * which, for each unique ResourceModel, presents a set of Allocator objects relevant to that model, for a particular machine.
     */
    public Map<ResourceModel, Collection<Allocator>> createAllocators(
        final LiqidInventory inventory,
        final ClusterLayout desiredLayout
    ) {
        var fn = "createAllocators";
        _logger.trace("Entering %s with inventory=%s desiredLayout=%s", fn, inventory, desiredLayout);

        var result = new HashMap<ResourceModel, Collection<Allocator>>();

        // iterate over the machine profiles in the desired layout.
        for (var machineProfile : desiredLayout.getMachineProfiles()) {
            var machineName = machineProfile.getMachineName();
            var resModels = machineProfile.getResourceModels();

            // Find restrictive resource models (those with a value of zero)
            var restrictions = new HashSet<ResourceModel>();
            for (var rm : resModels) {
                var devCount = machineProfile.getCount(rm);
                if (devCount == 0) {
                    restrictions.add(rm);
                }
            }

            for (var rm : resModels) {
                var devCount = machineProfile.getCount(rm);
                if (devCount > 0) {
                    var devIds = getOrderedDeviceIdentifiers(inventory, rm, restrictions, machineName);
                    result.computeIfAbsent(rm, k -> new LinkedList<>());
                    result.get(rm).add(new Allocator(machineProfile.getMachineName(), devCount, devIds));
                }
            }
        }

        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * Helpful wrapper to create a full annotation key.
     * @param keySuffix the definitive portion of the key
     * @return the full key, with the company prefix applied to the front of the suffix
     */
    protected static String createAnnotationKeyFor(
        final String keySuffix
    ) {
        return String.format("%s/%s", K8S_ANNOTATION_PREFIX, keySuffix);
    }

    /**
     * Creates a string used as an annotation for a particular resource model and a count of resources
     * for that model.
     * @param resourceModel model of interest
     * @param count number of resources
     * @return string to be used in a specification annotation value
     */
    protected String createAnnotationForModelAndCount(
        final ResourceModel resourceModel,
        final Integer count
    ) throws InternalErrorException {
        if (resourceModel instanceof SpecificResourceModel) {
            return String.format("%s:%s:%d", resourceModel.getVendorName(), resourceModel.getModelName(), count);
        } else if (resourceModel instanceof VendorResourceModel) {
            return String.format("%s:%d", resourceModel.getVendorName(), count);
        } else if (resourceModel instanceof GenericResourceModel) {
            return String.format("%d", count);
        } else {
            throw new InternalErrorException("Unrecognized resource model");
        }
    }

    /**
     * Creates actions to clear out all resource annotations from all known nodes,
     * then repopulate the annotations of the nodes based on the given ClusterLayout.
     * All the nodes referenced in the layout must be in the nodes collection, but the nodes collection
     * may contain nodes which are not in the layout.
     * We do expect each node's machine name annotation to be in place.
     * @param nodes collection of all the non-compute nodes of interest
     * @param layout ClusterLayout containing the desired resource layout
     * @param plan plan which will be populated with actions
     * @return true if no errors were detected along the way
     */
    protected boolean createAnnotationsFromClusterLayout(
        final Collection<Node> nodes,
        final ClusterLayout layout,
        final Plan plan
    ) throws InternalErrorException {
        var fn = "createAnnotationsFromClusterLayout";
        _logger.trace("Entering %s with nodes=%s layout=%s plan=%s", fn, nodes, layout, plan);

        var errors = false;
        var errPrefix = getErrorPrefix();

        // This is done by establishing removal annotations for all general types, for all nodes,
        // then overwriting the appopropriate removals with replacements per the cluster layout.
        for (var node : nodes) {
            var newAnnos = new HashMap<GeneralType, String>();

            if (node.metadata.annotations != null) {
                for (var genType : GeneralType.values()) {
                    var annoKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType);
                    if (node.metadata.annotations.containsKey(annoKey)) {
                        newAnnos.put(genType, null);
                    }
                }
            }

            var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machineName = node.metadata.annotations.get(annoKey);
            if (machineName == null) {
                System.err.printf("%s:Incorrectly-annotated node '%s' - no machine name\n",
                                  errPrefix, node.getName());
                errors = true;
            } else {
                var profile = layout.getMachineProfile(machineName);
                if (profile != null) {
                    for (var resModel : profile.getResourceModels()) {
                        var genType = resModel.getGeneralType();
                        var count = profile.getCount(resModel);
                        var spec = createAnnotationForModelAndCount(resModel, count);
                        if (newAnnos.get(genType) != null) {
                            var sb = new StringBuilder();
                            sb.append(newAnnos.get(genType)).append(",").append(spec);
                            newAnnos.put(genType, sb.toString());
                        } else {
                            newAnnos.put(genType, spec);
                        }
                    }
                }
            }

            var action = new AnnotateNodeAction().setNodeName(node.getName());
            for (var entry : newAnnos.entrySet()) {
                var genType = entry.getKey();
                var spec = entry.getValue();
                action.addAnnotation(ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType), spec);
            }

            if (!action.getAnnotations().isEmpty()) {
                plan.addAction(action);
            }
        }

        var result = !errors;
        _logger.trace("%s returning %s with plan=%s", fn, result, plan);
        return result;
    }

    /**
     * Creates a populates a ClusterLayout based on the Liqid annotations for the given worker nodes
     * @param nodes collection of worker nodes
     * @return populated ClusterLayout if successful, or null if errors exist and _force is not set
     */
    protected ClusterLayout createClusterLayoutFromAnnotations(
        final Collection<Node> nodes
    ) {
        var fn = "createClusterLayoutFromAnnotations";
        _logger.trace("Entering %s", fn);

        var errors = false;
        var errPrefix = getErrorPrefix();

        var layout = new ClusterLayout();
        for (var node : nodes) {
            var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machineName = node.metadata.annotations.get(annoKey);
            if (machineName == null) {
                System.err.printf("%s:Node '%s' is not annotated with a valid machine name\n",
                                  errPrefix, node.getName());
                errors = true;
                continue;
            }

            var machProfile = new MachineProfile(machineName);
            for (var anno : getLiqidAnnotations(node).entrySet()) {
                var split = anno.getKey().split("/");
                GeneralType genType = null;
                if (split.length == 2) {
                    genType = switch (split[1]) {
                        case K8S_ANNOTATION_FPGA_ENTRY -> GeneralType.FPGA;
                        case K8S_ANNOTATION_GPU_ENTRY -> GeneralType.GPU;
                        case K8S_ANNOTATION_LINK_ENTRY -> GeneralType.LINK;
                        case K8S_ANNOTATION_MEMORY_ENTRY -> GeneralType.MEMORY;
                        case K8S_ANNOTATION_SSD_ENTRY -> GeneralType.SSD;
                        default -> null;
                    };
                }

                if (genType != null) {
                    var specs = anno.getValue().split(",");
                    for (var spec : specs) {
                        split = spec.split(":");
                        try {
                            ResourceModel resModel = null;
                            String vendor;
                            String model;
                            Integer count = null;
                            switch (split.length) {
                                case 1:
                                    count = Integer.parseInt(split[0]);
                                    resModel = new GenericResourceModel(genType);
                                    break;
                                case 2:
                                    vendor = split[0];
                                    count = Integer.parseInt(split[1]);
                                    resModel = new VendorResourceModel(genType, vendor);
                                    break;
                                case 3:
                                    vendor = split[0];
                                    model = split[1];
                                    count = Integer.parseInt(split[2]);
                                    resModel = new SpecificResourceModel(genType, vendor, model);
                                    break;
                                default:
                                    System.out.printf("%s:Annotation for node '%s' -> %s is invalid\n",
                                                      errPrefix, node.getName(), anno.getValue());
                                    errors = true;
                            }
                            machProfile.injectCount(resModel, count);
                        } catch (NumberFormatException ex) {
                            System.err.printf("%s:Annotation for node '%s' -> %s contains invalid resource count\n",
                                              errPrefix, node.getName(), anno.getValue());
                            errors = true;
                        }
                    }
                }
            }

            if (!machProfile.getResourceModels().isEmpty()) {
                layout.addMachineProfile(machProfile);
            }
        }

        var result = errors ? null : layout;
        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * As below, but using the current _liqidInventory for sourcing the configuration
     * @param nodes collection of worker nodes
     * @return ClusterLayout object corresponding to the actions added to the given Plan
     */
    protected ClusterLayout createEvenlyAllocatedClusterLayout(
        final Collection<Node> nodes
    ) throws K8SRequestError, K8SJSONError, K8SHTTPError {
        var fn = "allocateEqually";
        _logger.trace("Entering %s", fn);

        var computeDeviceItems = new HashMap<DeviceItem, Node>();
        var resourceDeviceItems = new LinkedList<DeviceItem>();

        var nodeLookup = nodes.stream()
                              .collect(Collectors.toMap(Node::getName, node -> node, (a, b) -> b, HashMap::new));
        for (var devItem : _liqidInventory.getDeviceItems()) {
            if (devItem.getGeneralType() == GeneralType.CPU) {
                var nodeName = devItem.getDeviceInfo().getUserDescription();
                computeDeviceItems.put(devItem, nodeLookup.get(nodeName));
            } else {
                resourceDeviceItems.add(devItem);
            }
        }

        var layout = createEvenlyAllocatedClusterLayout(computeDeviceItems, resourceDeviceItems);

        _logger.trace("%s returning %s", fn, layout);
        return layout;
    }

    /**
     * Creates steps to create annotations which will allocate resources as equally as possible
     * among the k8s worker nodes. Does NOT require anything from the existing _liqidClient nor from _k8sClient.
     * This logic is here because we really need to tie nodes to machines (sometimes in the context of NOT
     * having the annotation for the node->machine name in place), and we need to know how many devices of each
     * generic type we have, which we could get many places... but all of that in one place is best provided
     * in the .commands package, which is where we are at.
     * @param computeDeviceItems container which indicates the compute devices of interest, along with their nodes
     * @param resourceDeviceItems container which indicates the resource devices of interest
     * @return ClusterLayout object corresponding to the actions added to the given Plan
     */
    protected ClusterLayout createEvenlyAllocatedClusterLayout(
        final Map<DeviceItem, Node> computeDeviceItems,
        final Collection<DeviceItem> resourceDeviceItems
    ) {
        var fn = "allocateEqually";
        _logger.trace("Entering %s with compDevs=%s resDevs=%s",
                      fn, computeDeviceItems, resourceDeviceItems);

        var layout = new ClusterLayout();
        var devsByType = LiqidInventory.segregateDeviceItemsByType(resourceDeviceItems);
        if (computeDeviceItems.isEmpty() || resourceDeviceItems.isEmpty()) {
            _logger.trace("Exiting %s with nothing to do", fn);
            return layout;
        }

        //  We do this by machine, because it is much more efficient to attach all the devices for the machine
        //  in one step. We need to iterate over the computeDeviceItems to get there, though.
        var remainingMachineCount = computeDeviceItems.size();
        for (var entry : computeDeviceItems.entrySet()) {
            var node = entry.getValue();
            var machineName = createMachineName(entry.getKey().getDeviceStatus(), node);
            var machineProfile = new MachineProfile(machineName);

            //  loop over each type of device - for each type we have all the still-to-be-assigned
            //  devices of that type.
            for (var subEntry : devsByType.entrySet()) {
                var genType = subEntry.getKey();
                var devItems = subEntry.getValue();

                //  how many for this machine?
                var resCount = devItems.size() / remainingMachineCount;
                if (devItems.size() % remainingMachineCount > 0) {
                    resCount++;
                }

                //  Create resource profile and add it to machine profile with the resource count
                if (resCount > 0) {
                    var resModel = new GenericResourceModel(genType);
                    machineProfile.injectCount(resModel, resCount);

                    for (int rx = 0; rx < resCount; rx++) {
                        devItems.removeFirst();
                    }
                }
            }

            remainingMachineCount--;

            if (!machineProfile.getResourceModels().isEmpty()) {
                layout.addMachineProfile(machineProfile);
            }
        }

        _logger.trace("%s returning %s", fn, layout);
        return layout;
    }

    /**
     * Creates a machine name for a combination of the compute device name and the k8s node name.
     * This is somewhat arbitrary, but it should be used whenever a machine name needs to be associated with
     * a particular compute device and worker node combination.
     * We ensure (to a reasonable extend) that the generated name is valid for a Liqid Cluster Machine.
     * @param devStat SDK DeviceStatus object representing a compute device
     * @param node Node object representing the k8w worker node corresponding to the compute device
     * @return name to be used for the Liqid Cluster machine.
     */
    protected String createMachineName(
        final DeviceStatus devStat,
        final Node node
    ) {
        var devName = devStat.getName();
        var nodeName = node.getName();
        var machName = String.format("%s-%s", devName, nodeName);
        if (machName.length() > 22) {
            machName = machName.substring(0, 22);
        }

        return machName;
    }

    /**
     * Add actions to a plan which effect the creation of Liqid Cluster machines.
     * These actions create the machine, add the appropriate compute device to the machine,
     * and set the compute device user description to the appropriate k8s worker node name.
     * We assume the compute devices have already been added to the targeted group.
     * One side-effect which might be useful to the caller, is that we do the following link-up in the
     * computeDevices parameter:
     *  Each DeviceItem will be updated so that the user description field contains the corresponding node name
     *  Each Node item will be updated to contain an annotation tying the node to the corresponding Liqid machine
     * These changes are not persisted, but they may be used by the caller for any subsequent necessary processing.
     * @param computeDevices a map of DeviceItem objects representing compute resources, to the corresponding
     *                       Node objects representing k8s worker nodes.
     * @param plan the plan which we populate
     */
    protected void createMachines(
        final Map<DeviceItem, Node> computeDevices,
        final Plan plan
    ) {
        var fn = "createMachines";
        _logger.trace("Entering %s with compDevs=%s plan=%s", fn, computeDevices, plan);

        // We're going to do it in order by pcpu{n} name, just because it is cleaner.
        var orderedMap = new TreeMap<Integer, DeviceItem>();
        for (var devItem : computeDevices.keySet()) {
            Integer key = Integer.parseInt(devItem.getDeviceName().substring(4));
            orderedMap.put(key, devItem);
        }

        for (var entry : orderedMap.entrySet()) {
            var devItem = entry.getValue();
            var node = computeDevices.get(devItem);
            var devName = devItem.getDeviceName();
            var nodeName = node.getName();
            var machineName = createMachineName(devItem.getDeviceStatus(), node);

            plan.addAction(new CreateMachineAction().setMachineName(machineName).setGroupName(_liqidGroupName));
            plan.addAction(new AssignToMachineAction().setMachineName(machineName).addDeviceName(devName));
            plan.addAction(new SetUserDescriptionAction().setDeviceName(devName).setDescription(nodeName));
            plan.addAction(new AnnotateNodeAction().setNodeName(nodeName)
                                                   .addAnnotation(K8S_ANNOTATION_MACHINE_NAME, machineName));

            devItem.getDeviceInfo().setUserDescription(nodeName);
            if (computeDevices.get(devItem).metadata.annotations == null) {
                computeDevices.get(devItem).metadata.annotations = new HashMap<>();
            }
            computeDevices.get(devItem).metadata.annotations.put(createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME),
                                                                 machineName);
        }

        _logger.trace("%s returning with plan=%s", fn, plan);
    }

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

    /**
     * Based on processor specifications, we populate containers of compute device information.
     * @param processorSpecs list of processor specifications which tie compute resources to k8s nodes
     *                       format is {deviceName} ':' {nodeName}
     * @param computeDevices a map of DeviceItem objects which we create based on the processorSpecs praameter,
     *                       which represents compute resources with their corresponding k8s Node objects.
     * @return true if successful, else false
     */
    protected boolean developComputeListFromSpecifications(
        final Collection<String> processorSpecs,
        final Map<DeviceItem, Node> computeDevices
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "developDeviceList";
        _logger.trace("Entering %s with processorSpecs=%s", fn, processorSpecs);

        var errors = false;
        var errPrefix = getErrorPrefix();

        for (var spec : processorSpecs) {
            var split = spec.split(":");
            if (split.length != 2) {
                System.err.printf("ERROR:Invalid format for spec '%s'\n", spec);
                errors = true;
            }

            var devName = split[0];
            var nodeName = split[1];

            var devItem = _liqidInventory.getDeviceItem(devName);
            if (devItem == null) {
                System.err.printf("%s:Compute resource '%s' is not in the Liqid Cluster\n", errPrefix, devName);
                errors = true;
            }

            Node node = null;
            try {
                node = _k8sClient.getNode(nodeName);
            } catch (K8SHTTPError ex) {
                if (ex.getResponseCode() == 404) {
                    System.err.printf("%s:Worker node '%s' is not in the Kubernetes Cluster\n", errPrefix, nodeName);
                    errors = true;
                } else {
                    throw ex;
                }
            }

            if ((devItem != null) && (node != null)) {
                computeDevices.put(devItem, node);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, computeDevices=%s", fn, errors, computeDevices);
        return result;
    }

    /**
     * Based on resource specifications (i.e., device names) we develop a collection of corresponding
     * DeviceItem objects based on the provided LiqidInventory object
     * @param resourceSpecs list of resource (device) names - does NOT include compute resources
     * @param resourceDevices collection which we populate with DeviceItem objects
     * @return true if successful, else false
     */
    protected boolean developDeviceListFromSpecifications(
        final Collection<String> resourceSpecs,
        final Collection<DeviceItem> resourceDevices
    ) {
        var fn = "developDeviceList";
        _logger.trace("Entering %s with resourceSpecs=%s", fn, resourceSpecs);

        var errors = false;
        var errPrefix = getErrorPrefix();

        for (var spec : resourceSpecs) {
            var devItem = _liqidInventory.getDeviceItem(spec);
            if (devItem == null) {
                System.err.printf("%s:Resource '%s' is not in the Liqid Cluster\n", errPrefix, spec);
                errors = true;
            } else {
                resourceDevices.add(devItem);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, resourceDevices=%s", fn, errors, resourceDevices);
        return result;
    }

    /**
     * Retrieves a short string to be used in error/warning messages.
     * Anomalies are often warnings if -force is set, but errors otherwise
     * @return message prefix
     */
    protected String getErrorPrefix() {
        return _force ? "WARNING" : "ERROR";
    }

    /**
     * Given two collections of the same type, we populate a third collection of that type with
     * only those items which are contained in both of the original collections.
     * The basic type should have a meaningful equals operation.
     * For unordered collections, the resulting order will be that of collection1.
     * @param collection1 first contributing collection
     * @param collection2 second contributing collection
     * @param intersection resulting collection.
     * @param <T> item type
     */
    protected <T> void getIntersection(
        final Collection<T> collection1,
        final Collection<T> collection2,
        final Collection<T> intersection
    ) {
        intersection.clear();
        var temp2 = new LinkedList<>(collection2);
        for (var item : collection1) {
            if (temp2.contains(item)) {
                temp2.remove(item);
                intersection.add(item);
            }
        }
    }

    /**
     * Retrieves all the liqid annotations for a particular node from the k8s cluster
     * @return map containing liqid annotations
     */
    protected Map<String, String> getLiqidAnnotations(
        final Node node
    ) {
        var fn = "getLiqidAnnotations";
        _logger.trace("Entering %s with node=%s", fn, node);

        var result = new HashMap<String, String>();
        for (var anno : node.metadata.annotations.entrySet()) {
            if (anno.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
                result.put(anno.getKey(), anno.getValue());
            }
        }

        _logger.trace("%s returning %s", result);
        return result;
    }

    /**
     * This code solicits the information we need to interact with the Liqid Cluster from the k8s database.
     * It presumes that the k8s cluster is suitably linked to a Liqid Cluster.
     * Such linkage exists in the form of a ConfigMap and an optional Secret.
     * The specific bits of information returned include:
     *      IP address of the Liqid Cluster (actually, of the director)
     *      Group name of the Liqid Cluster group to which all relevant resources do, or should, belong.
     *      Username credential if basic authentication is enabled for the Liqid cluster
     *      Password credential if basic authentication is enabled for the Liqid cluster,
     *          although we do account for the possibility of a null password for a sadly unprotected username.
     * @throws ConfigurationDataException Indicates something is wrong in the actual bits of information stored in
     *                                      the configmap or the secret.
     * @throws K8SHTTPError If any unexpected HTTP responses are received. Generally, we expect only 200s.
     * @throws K8SJSONError If information received from k8s cannot be converted from JSON into the expected data
     *                          structs. This generally indicates a programming error on our part, but it could also
     *                          result from gratuitous changes in k8s, which does unfortunately occur.
     * @throws K8SRequestError Indicates some other error during processing, from within the k8sClient module.
     */
    protected void getLiqidLinkage(
    ) throws ConfigurationDataException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "getLiqidLinkage";
        _logger.trace("Entering %s", fn);

        var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        _liqidAddress = cfgMap.data.get(K8S_CONFIG_MAP_IP_ADDRESS_KEY);
        _liqidGroupName = cfgMap.data.get(K8S_CONFIG_MAP_GROUP_NAME_KEY);

        _liqidUsername = null;
        _liqidPassword = null;
        try {
            var secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
            var creds = new CredentialMangler(secret.data.get(K8S_SECRET_CREDENTIALS_KEY));
            _liqidUsername = creds.getUsername();
            _liqidPassword = creds.getPassword();
        } catch (K8SHTTPError kex) {
            // a 404 is okay - there might not be any credentials. Anything else gets rethrown.
            if (kex.getResponseCode() != 404) {
                throw kex;
            }
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Creates a list of device identifiers from the inventory which are accepted by the given ResourceModel
     * object (i.e., match the general type, and the vendor (if relevant) and model (if relevant).
     * The ordering of the resulting list is important to the caller, and is as follows:
     *  Firstly, the devices owned by the given machine
     *  Secondly, the devices which are not owned by any machine
     *  Finally, the devices which are owned by other machines
     * Each segregated category is segregated to facility unit tests.
     * Protected to facilitate unit tests.
     * @param inventory LiqidInventory from which we get the list of devices
     * @param resourceModel ResourceModel limiting the devices which we are allowed to consider
     * @param disallowedModels a possibly empty collection of ResourceModel objects which we are *not* allowed to
     *                         consider - the caller might want all GPUs (for example) *excepting* those from ACME,
     *                         or models T1 and T2 from SKY-NET.
     * @param machineName machine name of the machine to which this list applies
     * @return sorted list of device identifiers
     */
    protected LinkedList<Integer> getOrderedDeviceIdentifiers(
        final LiqidInventory inventory,
        final ResourceModel resourceModel,
        final Collection<ResourceModel> disallowedModels,
        final String machineName
    ) {
        var fn = "getOrderedDeviceIdentifiers";
        _logger.trace("Entering %s with inventory=%s resModel=%s disallowedModesl=%s machName=%s",
                      fn, inventory, resourceModel, disallowedModels, machineName);

        var thisMachineList = new TreeSet<Integer>();
        var otherMachineList = new TreeSet<Integer>();
        var freeList = new TreeSet<Integer>();

        for (var devItem : inventory.getDeviceItems()) {
            if (resourceModel.accepts(devItem.getDeviceInfo())) {
                var ignore = false;
                for (var rm : disallowedModels) {
                    if (rm.accepts(devItem.getDeviceInfo())) {
                        ignore = true;
                        break;
                    }
                }

                if (!ignore) {
                    var devId = devItem.getDeviceId();
                    if (devItem.isAssignedToMachine()) {
                        var attachedMachine = inventory.getMachine(devItem.getMachineId());
                        if (attachedMachine.getMachineName().equals(machineName)) {
                            thisMachineList.add(devId);
                        } else {
                            otherMachineList.add(devId);
                        }
                    } else {
                        freeList.add(devId);
                    }
                }
            }
        }

        var result = new LinkedList<>(thisMachineList);
        result.addAll(freeList);
        result.addAll(otherMachineList);

        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * Checks the indicated collection of nodes to see whether any of them are annotated
     * with Liqid annotations
     */
    protected boolean hasAnnotations(
        final Collection<Node> nodes
    ) {
        var fn = "hasAnnotations";
        _logger.trace("Entering %s with nodes=%s", fn, nodes);

        for (var node : nodes) {
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

    /**
     * Processes a VarianceSet against the content of the given LiqidInventory, populating the given Plan object
     * with actions which will effect the changes required to do so.
     * @param varianceSet the variance set to be processed
     * @param plan the plan to be updated
     * @throws InternalErrorException if something goes quite wrong
     */
    protected void processVarianceSet(
        final Set<Integer> unassignedDeviceIds,
        final VarianceSet varianceSet,
        final Plan plan
    ) throws InternalErrorException {
        var fn = "processVarianceSet";
        _logger.trace("Entering %s with varSet=%s plan=%s", fn, varianceSet, plan);

        var working = new HashSet<>(unassignedDeviceIds);
        while (!varianceSet.isEmpty()) {
            var action = varianceSet.getAction(_liqidInventory, working);
            plan.addAction(action);
        }

        _logger.trace("%s returning with plan=%s", fn, plan);
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing groups.
     * Does NOT actually do anything else.
     * @param devices collection of DeviceItem objects to be removed from their containing groups
     * @param plan plan which we populate
     */
    protected void releaseDevicesFromGroups(
        final Collection<DeviceItem> devices,
        final Plan plan
    ) {
        var fn = "releaseDevicesFromGroups";
        _logger.trace("Entering %s with devices=%s", fn, devices);

        // Iterate over the groups so that we can do multiple devices per group.
        // In the case where we're removing all the devices for a group, just delete the group.
        for (var group : _liqidInventory.getGroups()) {
                var grpDevs = _liqidInventory.getDeviceItemsForGroup(group.getGroupId());
                Set<DeviceItem> devsToRemove = new HashSet<>();
                getIntersection(devices, grpDevs, devsToRemove);

                if (!devsToRemove.isEmpty()) {
                    if (devsToRemove.size() == grpDevs.size()) {
                        plan.addAction(new DeleteGroupAction().setGroupName(group.getGroupName()));
                    } else {
                        var names = LiqidInventory.getDeviceNamesFromItems(devsToRemove);
                        plan.addAction(new RemoveFromGroupAction().setGroupName(group.getGroupName())
                                                                  .setDeviceNames(names));
                    }
                }
        }

        _logger.trace("Exiting %s with plan=%s", fn, plan);
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing machines.
     * Does NOT actually do anything else.
     * @param devices collection of DeviceItem objects to be removed from their containing machines
     * @param plan plan which we populate
     */
    protected void releaseDevicesFromMachines(
        final Collection<DeviceItem> devices,
        final Plan plan
    ) {
        var fn = "releaseDevicesFromMachines";
        _logger.trace("Entering %s with devices=%s", fn, devices);

        // Iterate over the machines so that we can do multiple devices per machine.
        // In the case where we're removing all the devices for a machine, just delete the machine.
        for (var mach : _liqidInventory.getMachines()) {
            var machDevs = _liqidInventory.getDeviceItemsForMachine(mach.getMachineId());
            Set<DeviceItem> devsToRemove = new HashSet<>();
            getIntersection(devices, machDevs, devsToRemove);

            if (!devsToRemove.isEmpty()) {
                if (devsToRemove.size() == machDevs.size()) {
                    plan.addAction(new DeleteMachineAction().setMachineName(mach.getMachineName()));
                } else {
                    var names = LiqidInventory.getDeviceNamesFromItems(devsToRemove);
                    plan.addAction(new RemoveFromMachineAction().setMachineName(mach.getMachineName())
                                                                .setDeviceNames(names));
                }
            }
        }

        _logger.trace("Exiting %s with plan=%s", fn, plan);
    }
}
