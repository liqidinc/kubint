/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.commands.Command;
import com.liqid.k8s.LiqidGeneralType;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AnnotateNode;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;

//class LabelCommand extends Command {
//
//    private Collection<String> _fpgaSpecs;
//    private Collection<String> _gpuSpecs;
//    private Collection<String> _linkSpecs;
//    private String _machineName;
//    private Collection<String> _memorySpecs;
//    private String _nodeName;
//    private Boolean _noUpdate = false;
//    private Collection<String> _ssdSpecs;
//
//    LabelCommand(
//        final Logger logger,
//        final String proxyURL,
//        final Boolean force,
//        final Integer timeoutInSeconds
//    ) {
//        super(logger, proxyURL, force, timeoutInSeconds);
//    }
//
//    LabelCommand setFPGASpecifications(final Collection<String> list) { _fpgaSpecs = list; return this; }
//    LabelCommand setGPUSpecifications(final Collection<String> list) { _gpuSpecs = list; return this; }
//    LabelCommand setLinkSpecifications(final Collection<String> list) { _linkSpecs = list; return this; }
//    LabelCommand setMachineName(final String value) { _machineName = value; return this; }
//    LabelCommand setMemorySpecifications(final Collection<String> list) { _memorySpecs = list; return this; }
//    LabelCommand setNodeName(final String value) { _nodeName = value; return this; }
//    LabelCommand setNoUpdate(final Boolean value) { _noUpdate = value; return this; }
//    LabelCommand setSSDSpecifications(final Collection<String> list) { _ssdSpecs = list; return this; }
//
//    private boolean processType(
//        final LiqidGeneralType genType,
//        final Collection<String> specifications,
//        final Plan plan
//    ) {
//        var fn = "processType";
//        _logger.trace("Entering %s genType=%s specifications=%s annotations=%s",
//                      fn,
//                      genType,
//                      specifications);
//
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//
//        var vendorAndModel = new LinkedHashMap<String, Integer>();
//        var modelOnly = new LinkedHashMap<String, Integer>();
//        Integer noSpecificity = null;
//
//        for (var spec : specifications) {
//            String vendor = null;
//            String model = null;
//            Integer resCount = null;
//
//            var split = spec.split(":");
//            if (split.length > 3) {
//                System.err.printf("ERROR:Spec '%s' is invalid\n", spec);
//                errors = true;
//                continue;
//            }
//
//            try {
//                resCount = Integer.parseInt(split[split.length - 1]);
//                if (resCount < 0) {
//                    throw new NumberFormatException("");
//                }
//            } catch (NumberFormatException ex) {
//                System.err.printf("ERROR:Spec '%s' contains an invalid resource count\n", spec);
//                errors = true;
//                continue;
//            }
//
//            if (split.length == 3) {
//                vendor = split[0];
//                model = split[1];
//
//                if (!_liqidInventory.hasDevice(split[0], split[1])) {
//                    System.err.printf("%s:Spec '%s' refers to a vendor/model which is not present in the Liqid Cluster\n",
//                                      errPrefix, spec);
//                    if (!_force) {
//                        errors = true;
//                    }
//                }
//
//                var key = vendor + ":" + model;
//                if (vendorAndModel.containsKey(key)) {
//                    System.err.printf("ERROR:Spec '%s' overlays a previous specification\n", spec);
//                    errors = true;
//                } else {
//                    vendorAndModel.put(key, resCount);
//                }
//            } else if (split.length == 2) {
//                model = split[0];
//
//                if (!_liqidInventory.hasDevice(split[0])) {
//                    System.err.printf("%s:Spec '%s' refers to a model which is not present in the Liqid Cluster\n",
//                                      errPrefix, spec);
//                    if (!_force) {
//                        errors = true;
//                    }
//                }
//
//                if (modelOnly.containsKey(model)) {
//                    System.err.printf("ERROR:Spec '%s' overlays a previous specification\n", spec);
//                    errors = true;
//                } else {
//                    modelOnly.put(model, resCount);
//                }
//            } else {
//                if (noSpecificity != null) {
//                    System.err.printf("ERROR:Spec '%s' overlays a previous specification\n", spec);
//                    errors = true;
//                } else {
//                    noSpecificity = resCount;
//                }
//            }
//        }
//
//        if (errors) {
//            _logger.trace("Exiting %s with false", fn);
//            return false;
//        }
//
//        var annoKey = createAnnotationKeyForDeviceType(genType);
//        if (vendorAndModel.isEmpty() && modelOnly.isEmpty() && (noSpecificity != null) && (noSpecificity == 0)) {
//            System.out.printf("Any existing annotation for type %s will be removed\n", genType);
//            plan.addAction(new AnnotateNode().setNodeName(_nodeName).addAnnotation(annoKey, null));
//        } else {
//            var newSpecStrings = new LinkedList<String>();
//            for (var entry : vendorAndModel.entrySet()) {
//                if (entry.getValue() > 0) {
//                    System.out.printf("Will allocate %d %s devices from vendor:model %s\n",
//                                      entry.getValue(), genType, entry.getKey());
//                    newSpecStrings.add(String.format("%s:%d", entry.getKey(), entry.getValue()));
//                }
//            }
//
//            for (var entry : modelOnly.entrySet()) {
//                if (entry.getValue() > 0) {
//                    System.out.printf("Will allocate %s%d %s devices of model %s from any vendor\n",
//                                      vendorAndModel.isEmpty() ? "" : "an additional ",
//                                      entry.getValue(),
//                                      genType,
//                                      entry.getKey());
//                    newSpecStrings.add(String.format("%s:%d", entry.getKey(), entry.getValue()));
//                }
//            }
//
//            if ((noSpecificity != null) && (noSpecificity != 0)) {
//                System.out.printf("Will allocate %s%d %s devices of any model from any vendor\n",
//                                  vendorAndModel.isEmpty() ? "" : "an additional ",
//                                  noSpecificity,
//                                  genType);
//                newSpecStrings.add(String.format("%d", noSpecificity));
//            }
//
//            var newSpecString = String.join(",", newSpecStrings);
//            plan.addAction(new AnnotateNode().setNodeName(_nodeName).addAnnotation(annoKey, newSpecString));
//        }
//
//        _logger.trace("Exiting %s with true", fn);
//        return true;
//    }
//
//    @Override
//    public Plan process(
//    ) throws ConfigurationDataException,
//             ConfigurationException,
//             InternalErrorException,
//             K8SException,
//             LiqidException,
//             ProcessingException {
//        var fn = this.getClass().getName() + ":process";
//        _logger.trace("Entering %s", fn);
//
//        initK8sClient();
//
//        Node node;
//        try {
//            node = _k8sClient.getNode(_nodeName);
//        } catch (K8SHTTPError ex) {
//            if (ex.getResponseCode() == 404) {
//                throw new ProcessingException(String.format("Worker node '%s' does not exist", _nodeName));
//            } else {
//                throw ex;
//            }
//        }
//
//        getLiqidLinkage();
//        initLiqidClient();
//        getLiqidInventory();
//
//        var errPrefix = getErrorPrefix();
//        var errors = false;
//        var annotations = new HashMap<String, String>();
//
//        var group = _liqidInventory._groupsByName.get(_liqidGroupName);
//        if (group == null) {
//            System.err.printf("%s:Group '%s' does not exist in the Liqid Cluster\n", errPrefix, _liqidGroupName);
//            if (!_force) {
//                errors = true;
//            }
//        }
//
//        var mach = _liqidInventory._machinesByName.get(_machineName);
//        if (mach == null) {
//            System.err.printf("%s:Machine '%s' does not exist in the Liqid Cluster\n", errPrefix, _machineName);
//            if (!_force) {
//                errors = true;
//            }
//        } else if ((group != null) && (!mach.getGroupId().equals(group.getGroupId()))) {
//            System.err.printf("%s:Machine '%s' is not in group '%s'\n", errPrefix, _machineName, _liqidGroupName);
//            if (!_force) {
//                errors = true;
//            }
//        }
//
//        var plan = new Plan();
//        var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
//        plan.addAction(new AnnotateNode().setNodeName(_nodeName).addAnnotation(annoKey, _machineName));
//
//        if (_fpgaSpecs != null) {
//            if (!processType(LiqidGeneralType.FPGA, _fpgaSpecs, plan)) {
//                errors = true;
//            }
//        }
//
//        if (_gpuSpecs != null) {
//            if (!processType(LiqidGeneralType.GPU, _gpuSpecs, plan)) {
//                errors = true;
//            }
//        }
//
//        if (_linkSpecs != null) {
//            if (!processType(LiqidGeneralType.LINK, _linkSpecs, plan)) {
//                errors = true;
//            }
//        }
//
//        if (_memorySpecs != null) {
//            if (!processType(LiqidGeneralType.MEMORY, _memorySpecs, plan)) {
//                errors = true;
//            }
//        }
//
//        if (_ssdSpecs != null) {
//            if (!processType(LiqidGeneralType.SSD, _ssdSpecs, plan)) {
//                errors = true;
//            }
//        }
//
//        if (errors) {
//            System.err.println("Errors prevent further processing.");
//            _logger.trace("Exiting %s with null", fn);
//            return null;
//        }
//
//        // All done
//        _logger.trace("Exiting %s with %s", fn, plan);
//        return plan;
//    }
//}
