/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.ScriptException;
import com.liqid.sdk.LiqidException;

import java.util.Collection;

public class Application {

    private CommandType _commandType;
    private int _timeoutInSeconds = 300;

    private String _liqidAddress;
    private String _liqidGroupName;
    private String _liqidMachineName;
    private String _liqidPassword;
    private String _liqidUsername;

    private Collection<String> _liqidFPGASpecs;
    private Collection<String> _liqidGPUSpecs;
    private Collection<String> _liqidLinkSpecs;
    private Collection<String> _liqidMemorySpecs;
    private Collection<String> _liqidSSDSpecs;

    private String _k8sNodeName;

    private boolean _all = false;
    private boolean _force = false;
    private Logger _logger;
    private boolean _noUpdate = false;
    private String _proxyURL;

    Application() {}

    Application setAll(final boolean flag) { _all = flag; return this; }
    Application setCommandType(final CommandType value) { _commandType = value; return this; }
    Application setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    Application setLiqidFPGASpecifications(final Collection<String> list) { _liqidFPGASpecs = list; return this; }
    Application setLiqidGPUSpecifications(final Collection<String> list) { _liqidGPUSpecs = list; return this; }
    Application setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    Application setLiqidLinkSpecifications(final Collection<String> list) { _liqidLinkSpecs = list; return this; }
    Application setLiqidMachineName(final String value) { _liqidMachineName = value; return this; }
    Application setLiqidMemorySpecifications(final Collection<String> list) { _liqidMemorySpecs = list; return this; }
    Application setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    Application setLiqidSSDSpecifications(final Collection<String> list) { _liqidSSDSpecs = list; return this; }
    Application setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    Application setForce(final boolean value) { _force = value; return this; }
    Application setLogger(final Logger value) { _logger = value; return this; }
    Application setNoUpdate(final boolean flag) { _noUpdate = flag; return this; }
    Application setK8SNodeName(final String value) { _k8sNodeName = value; return this; }
    Application setProxyURL(final String value) { _proxyURL = value; return this; }
    Application setTimeoutInSeconds(final int value) { _timeoutInSeconds = value; return this; }

    void process() throws K8SException, LiqidException, ScriptException {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        var command = switch (_commandType) {
            case AUTO -> new AutoCommand(_logger, _proxyURL, _force, _timeoutInSeconds);
            case LABEL -> new LabelCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                .setFPGASpecifications(_liqidFPGASpecs)
                .setGPUSpecifications(_liqidGPUSpecs)
                .setLinkSpecifications(_liqidLinkSpecs)
                .setMachineName(_liqidMachineName)
                .setMemorySpecifications(_liqidMemorySpecs)
                .setNodeName(_k8sNodeName)
                .setNoUpdate(_noUpdate)
                .setSSDSpecifications(_liqidSSDSpecs);
            case LINK -> new LinkCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                .setLiqidAddress(_liqidAddress)
                .setLiqidGroupName(_liqidGroupName)
                .setLiqidPassword(_liqidPassword)
                .setLiqidUsername(_liqidUsername);
            case NODES -> new NodesCommand(_logger, _proxyURL, _force, _timeoutInSeconds);
            case RESOURCES -> new ResourcesCommand(_logger, _proxyURL, _force, _timeoutInSeconds).setAll(_all);
            case UNLABEL -> new UnlabelCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                .setNodeName(_k8sNodeName)
                .setAll(_all);
            case UNLINK -> new UnlinkCommand(_logger, _proxyURL, _force, _timeoutInSeconds);
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
