/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.LiqidGeneralType;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AnnotateNode;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.DeviceType;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.stream.Collectors;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;

public class AnnotateCommand extends Command {

    private Boolean _automatic;
    private Boolean _clear;
    private Collection<String> _fpgaSpecs;
    private Collection<String> _gpuSpecs;
    private Collection<String> _linkSpecs;
    private String _machineName;
    private Collection<String> _memorySpecs;
    private String _nodeName;
    private Collection<String> _ssdSpecs;

    public AnnotateCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public AnnotateCommand setAutomatic(final Boolean value) { _automatic = value; return this; }
    public AnnotateCommand setClear(final Boolean value) { _clear = value; return this; }
    public AnnotateCommand setFPGASpecifications(final Collection<String> list) { _fpgaSpecs = list; return this; }
    public AnnotateCommand setGPUSpecifications(final Collection<String> list) { _gpuSpecs = list; return this; }
    public AnnotateCommand setLinkSpecifications(final Collection<String> list) { _linkSpecs = list; return this; }
    public AnnotateCommand setMachineName(final String value) { _machineName = value; return this; }
    public AnnotateCommand setMemorySpecifications(final Collection<String> list) { _memorySpecs = list; return this; }
    public AnnotateCommand setNodeName(final String value) { _nodeName = value; return this; }
    public AnnotateCommand setProxyURL(final String value) { _proxyURL = value; return this; }
    public AnnotateCommand setSSDSpecifications(final Collection<String> list) { _ssdSpecs = list; return this; }

    private boolean processAutomatic(
        final Plan plan
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError, ProcessingException {
        var fn = "processAutomatic";
        _logger.trace("Entering %s");

        var errPrefix = getErrorPrefix();
        var errors = false;

        var computeResources = new HashMap<DeviceStatus, Node>();
        for (var node : _k8sClient.getNodes()) {
            var machineName = node.metadata.annotations.get(createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME));
            if (machineName != null) {
                var machine = _liqidInventory._machinesByName.get(machineName);
                if (machine == null) {
                    System.out.printf("%s:Node '%s' refers to Liqid machine '%s' which is not in the Liqid Cluster\n",
                                      errPrefix, node.getName(), machineName);
                    errors = true;
                } else {
                    var machDevs = _liqidInventory._deviceStatusByMachineId.get(machine.getMachineId());
                    DeviceStatus compDevStat = null;
                    for (var ds : machDevs) {
                        if (ds.getDeviceType() == DeviceType.COMPUTE) {
                            compDevStat = ds;
                            break;
                        }
                    }
                    if (compDevStat == null) {
                        System.out.printf("%s:Machine '%s' referenced by node '%s' does not contain a compute node\n",
                                          errPrefix, machineName, node.getName());
                    } else {
                        computeResources.put(compDevStat, node);
                    }
                }
            }
        }

        var group = _liqidInventory._groupsByName.get(_liqidGroupName);
        var resDevices = _liqidInventory._deviceStatusByGroupId.get(group.getGroupId())
                                                               .stream()
                                                               .filter(ds -> ds.getDeviceType() != DeviceType.COMPUTE)
                                                               .collect(Collectors.toCollection(LinkedList::new));

        allocateEqually(plan, computeResources, resDevices);

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private boolean processClear(
        final Plan plan
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "processClear";
        _logger.trace("Entering %s");

        var errors = false;

        try {
            _k8sClient.getNode(_nodeName);
        } catch (K8SHTTPError kex) {
            if (kex.getResponseCode() == 404) {
                System.err.printf("ERROR:Node '%s' does not exist in the Kubernetes Cluster\n", _nodeName);
                errors = true;
            } else {
                throw kex;
            }
        }

        var action = new AnnotateNode().setNodeName(_nodeName);
        for (var genType : LiqidGeneralType.values()) {
            if (genType != LiqidGeneralType.CPU) {
                var annoKey = createAnnotationKeyForDeviceType(genType);
                action.addAnnotation(annoKey, null);
            }
        }
        plan.addAction(action);

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private boolean processManual(
        final Plan plan
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "processManual";
        _logger.trace("Entering %s", fn);

        var errPrefix = getErrorPrefix();
        var errors = false;

        try {
            _k8sClient.getNode(_nodeName);
        } catch (K8SHTTPError kex) {
            if (kex.getResponseCode() == 404) {
                System.err.printf("ERROR:Node '%s' does not exist in the Kubernetes Cluster\n", _nodeName);
                errors = true;
            } else {
                _logger.throwing(kex);
                throw kex;
            }
        }

        var group = _liqidInventory._groupsByName.get(_liqidGroupName);
        if (group == null) {
            System.err.printf("%s:Group '%s' does not exist in the Liqid Cluster\n", errPrefix, _liqidGroupName);
            if (!_force) {
                errors = true;
            }
        }

        var mach = _liqidInventory._machinesByName.get(_machineName);
        if (mach == null) {
            System.err.printf("%s:Machine '%s' does not exist in the Liqid Cluster\n", errPrefix, _machineName);
            if (!_force) {
                errors = true;
            }
        } else if ((group != null) && (!mach.getGroupId().equals(group.getGroupId()))) {
            System.err.printf("%s:Machine '%s' is not in group '%s'\n", errPrefix, _machineName, _liqidGroupName);
            if (!_force) {
                errors = true;
            }
        }

        var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
        plan.addAction(new AnnotateNode().setNodeName(_nodeName).addAnnotation(annoKey, _machineName));

        if (_fpgaSpecs != null) {
            if (!processManualType(LiqidGeneralType.FPGA, _fpgaSpecs, plan)) {
                errors = true;
            }
        }

        if (_gpuSpecs != null) {
            if (!processManualType(LiqidGeneralType.GPU, _gpuSpecs, plan)) {
                errors = true;
            }
        }

        if (_linkSpecs != null) {
            if (!processManualType(LiqidGeneralType.LINK, _linkSpecs, plan)) {
                errors = true;
            }
        }

        if (_memorySpecs != null) {
            if (!processManualType(LiqidGeneralType.MEMORY, _memorySpecs, plan)) {
                errors = true;
            }
        }

        if (_ssdSpecs != null) {
            if (!processManualType(LiqidGeneralType.SSD, _ssdSpecs, plan)) {
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private boolean processManualType(
        final LiqidGeneralType genType,
        final Collection<String> specifications,
        final Plan plan
    ) {
        var fn = "processType";
        _logger.trace("Entering %s genType=%s specifications=%s annotations=%s",
                      fn,
                      genType,
                      specifications);

        var errors = false;
        var errPrefix = getErrorPrefix();

        var vendorAndModel = new LinkedHashMap<String, Integer>();
        var modelOnly = new LinkedHashMap<String, Integer>();
        Integer noSpecificity = null;

        for (var spec : specifications) {
            String vendor;
            String model;
            int resCount;

            var split = spec.split(":");
            if (split.length > 3) {
                System.err.printf("ERROR:Spec '%s' is invalid\n", spec);
                errors = true;
                continue;
            }

            try {
                resCount = Integer.parseInt(split[split.length - 1]);
                if (resCount < 0) {
                    throw new NumberFormatException("");
                }
            } catch (NumberFormatException ex) {
                System.err.printf("ERROR:Spec '%s' contains an invalid resource count\n", spec);
                errors = true;
                continue;
            }

            if (split.length == 3) {
                vendor = split[0];
                model = split[1];

                if (!_liqidInventory.hasDevice(split[0], split[1])) {
                    System.err.printf("%s:Spec '%s' refers to a vendor/model which is not present in the Liqid Cluster\n",
                                      errPrefix, spec);
                    if (!_force) {
                        errors = true;
                    }
                }

                var key = vendor + ":" + model;
                if (vendorAndModel.containsKey(key)) {
                    System.err.printf("ERROR:Spec '%s' overlays a previous specification\n", spec);
                    errors = true;
                } else {
                    vendorAndModel.put(key, resCount);
                }
            } else if (split.length == 2) {
                model = split[0];

                if (!_liqidInventory.hasDevice(split[0])) {
                    System.err.printf("%s:Spec '%s' refers to a model which is not present in the Liqid Cluster\n",
                                      errPrefix, spec);
                    if (!_force) {
                        errors = true;
                    }
                }

                if (modelOnly.containsKey(model)) {
                    System.err.printf("ERROR:Spec '%s' overlays a previous specification\n", spec);
                    errors = true;
                } else {
                    modelOnly.put(model, resCount);
                }
            } else {
                if (noSpecificity != null) {
                    System.err.printf("ERROR:Spec '%s' overlays a previous specification\n", spec);
                    errors = true;
                } else {
                    noSpecificity = resCount;
                }
            }
        }

        if (errors) {
            _logger.trace("Exiting %s with false", fn);
            return false;
        }

        var annoKey = createAnnotationKeyForDeviceType(genType);
        if (vendorAndModel.isEmpty() && modelOnly.isEmpty() && (noSpecificity != null) && (noSpecificity == 0)) {
            System.out.printf("Any existing annotation for type %s will be removed\n", genType);
            plan.addAction(new AnnotateNode().setNodeName(_nodeName).addAnnotation(annoKey, null));
        } else {
            var newSpecStrings = new LinkedList<String>();
            for (var entry : vendorAndModel.entrySet()) {
                if (entry.getValue() > 0) {
                    System.out.printf("Will allocate %d %s devices from vendor:model %s\n",
                                      entry.getValue(), genType, entry.getKey());
                    newSpecStrings.add(String.format("%s:%d", entry.getKey(), entry.getValue()));
                }
            }

            for (var entry : modelOnly.entrySet()) {
                if (entry.getValue() > 0) {
                    System.out.printf("Will allocate %s%d %s devices of model %s from any vendor\n",
                                      vendorAndModel.isEmpty() ? "" : "an additional ",
                                      entry.getValue(),
                                      genType,
                                      entry.getKey());
                    newSpecStrings.add(String.format("%s:%d", entry.getKey(), entry.getValue()));
                }
            }

            if ((noSpecificity != null) && (noSpecificity != 0)) {
                System.out.printf("Will allocate %s%d %s devices of any model from any vendor\n",
                                  vendorAndModel.isEmpty() ? "" : "an additional ",
                                  noSpecificity,
                                  genType);
                newSpecStrings.add(String.format("%d", noSpecificity));
            }

            var newSpecString = String.join(",", newSpecStrings);
            plan.addAction(new AnnotateNode().setNodeName(_nodeName).addAnnotation(annoKey, newSpecString));
        }

        _logger.trace("Exiting %s with true", fn);
        return true;
    }

    @Override
    public Plan process(
    ) throws ConfigurationDataException,
             ConfigurationException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();

        // If there is no linkage, tell the user and stop
        if (!hasLinkage()) {
            throw new ConfigurationException("No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
        }

        getLiqidLinkage();
        initLiqidClient();
        getLiqidInventory();

        var plan = new Plan();
        var success = false;
        if (_automatic) {
            success = processAutomatic(plan);
        } else if (_clear) {
            success = processClear(plan);
        } else {
            success = processManual(plan);
        }

        if (!success) {
            System.err.println("Errors prevent further processing.");
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        // All done
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
