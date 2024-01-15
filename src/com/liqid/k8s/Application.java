/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.commands.*;
import com.liqid.k8s.exceptions.ScriptException;
import com.liqid.sdk.LiqidException;

import java.util.Collection;

public class Application {

    private CommandType _commandType;
    private int _timeoutInSeconds = 300;

    private Boolean _allocate;
    private Boolean _automatic;
    private Boolean _clear;
    private Boolean _force;
    private Collection<String> _fpgaSpecs;
    private Collection<String> _gpuSpecs;
    private String _liqidAddress;
    private String _liqidGroupName;
    private String _liqidPassword;
    private String _liqidUsername;
    private Collection<String> _linkSpecs;
    private Logger _logger;
    private String _machineName;
    private Collection<String> _memorySpecs;
    private String _nodeName;
    private Boolean _noUpdate;
    private Collection<String> _processorSpecs;
    private String _proxyURL;
    private Collection<String> _resourceSpecs;
    private Collection<String> _ssdSpecs;

    Application setAllocate(final Boolean value) { _allocate = value; return this; }
    Application setAutomatic(final Boolean value) { _automatic = value; return this; }
    Application setClear(final Boolean value) { _clear = value; return this; }
    Application setCommandType(final CommandType value) { _commandType = value; return this; }
    Application setForce(final Boolean value) { _force = value; return this; }
    Application setFPGASpecs(final Collection<String> list) { _fpgaSpecs = list; return this; }
    Application setGPUSpecs(final Collection<String> list) { _gpuSpecs = list; return this; }
    Application setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    Application setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    Application setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    Application setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    Application setLinkSpecs(final Collection<String> list) { _linkSpecs = list; return this; }
    Application setLogger(final Logger value) { _logger = value; return this; }
    Application setMachineName(final String value) { _machineName = value; return this; }
    Application setMemorySpecs(final Collection<String> list) { _memorySpecs = list; return this; }
    Application setNodeName(final String value) { _nodeName = value; return this; }
    Application setNoUpdate(final boolean flag) { _noUpdate = flag; return this; }
    Application setProxyURL(final String value) { _proxyURL = value; return this; }
    Application setProcessorSpecs(final Collection<String> list) { _processorSpecs = list; return this; }
    Application setResourceSpecs(final Collection<String> list) {_resourceSpecs = list; return this; }
    Application setSSDSpecs(final Collection<String> list) { _ssdSpecs = list; return this; }
    Application setTimeoutInSeconds(final int value) { _timeoutInSeconds = value; return this; }

    void process() throws K8SException, LiqidException, ScriptException {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        var command = switch (_commandType) {
            case ADOPT ->
                new AdoptCommand(_logger, _force, _timeoutInSeconds)
                    .setProcessorSpecs(_processorSpecs)
                    .setProxyURL(_proxyURL)
                    .setResourceSpecs(_resourceSpecs);
            case ANNOTATE ->
                new AnnotateCommand(_logger, _force, _timeoutInSeconds)
                    .setAutomatic(_automatic)
                    .setClear(_clear)
                    .setMachineName(_machineName)
                    .setNodeName(_nodeName)
                    .setFPGASpecifications(_fpgaSpecs)
                    .setGPUSpecifications(_gpuSpecs)
                    .setLinkSpecifications(_linkSpecs)
                    .setMemorySpecifications(_memorySpecs)
                    .setSSDSpecifications(_ssdSpecs)
                    .setProxyURL(_proxyURL);
            case COMPOSE ->
                new ComposeCommand(_logger, _force, _timeoutInSeconds)
                    .setProxyURL(_proxyURL);
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
            case RELEASE ->
                new ReleaseCommand(_logger, _force, _timeoutInSeconds)
                    .setProxyURL(_proxyURL)
                    .setResourceSpecs(_resourceSpecs);
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
