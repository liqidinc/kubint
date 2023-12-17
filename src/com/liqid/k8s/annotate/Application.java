/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.FileWriter;
import com.bearsnake.klog.Level;
import com.bearsnake.klog.LevelMask;
import com.bearsnake.klog.Logger;
import com.bearsnake.klog.PrefixEntity;
import com.bearsnake.klog.StdOutWriter;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.sdk.LiqidException;

import java.io.IOException;
import java.util.Collection;

public class Application {

    public static final String LOGGER_NAME = "Annotate";
    public static final String LOG_FILE_NAME = "liq-annotate.log";

    // If _logging is true, we do extensive logging to a log file. If false, only errors to stdout.
    private boolean _logging = false;

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
    Application setLogging(final boolean flag) { _logging = flag; return this; }
    Application setNoUpdate(final boolean flag) { _noUpdate = flag; return this; }
    Application setK8SNodeName(final String value) { _k8sNodeName = value; return this; }
    Application setProxyURL(final String value) { _proxyURL = value; return this; }
    Application setTimeoutInSeconds(final int value) { _timeoutInSeconds = value; return this; }

    private void initLogging() throws InternalErrorException {
        try {
            var level = _logging ? Level.TRACE : Level.ERROR;
            _logger = new Logger(LOGGER_NAME);
            _logger.setLevel(level);

            _logger.addWriter(new StdOutWriter(Level.ERROR));
            if (_logging) {
                var fw = new FileWriter(new LevelMask(level), LOG_FILE_NAME, false);
                fw.addPrefixEntity(PrefixEntity.SOURCE_CLASS);
                fw.addPrefixEntity(PrefixEntity.SOURCE_METHOD);
                fw.addPrefixEntity(PrefixEntity.SOURCE_LINE_NUMBER);
                _logger.addWriter(fw);
            }
        } catch (IOException ex) {
            throw new InternalErrorException(ex.toString());
        }
    }

    void process() {
        var fn = "process";
        boolean result = false;
        try {
            initLogging();
            _logger.trace("Entering %s", fn);

            result = switch (_commandType) {
                case AUTO ->
                    new AutoCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setNoUpdate(_noUpdate)
                        .process();
                case LABEL ->
                    new LabelCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setFPGASpecifications(_liqidFPGASpecs)
                        .setGPUSpecifications(_liqidGPUSpecs)
                        .setLinkSpecifications(_liqidLinkSpecs)
                        .setMachineName(_liqidMachineName)
                        .setMemorySpecifications(_liqidMemorySpecs)
                        .setNodeName(_k8sNodeName)
                        .setNoUpdate(_noUpdate)
                        .setSSDSpecifications(_liqidSSDSpecs)
                        .process();
                case LINK ->
                    new LinkCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setLiqidAddress(_liqidAddress)
                        .setLiqidGroupName(_liqidGroupName)
                        .setLiqidPassword(_liqidPassword)
                        .setLiqidUsername(_liqidUsername)
                        .process();
                case NODES ->
                    new NodesCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
                case RESOURCES ->
                    new ResourcesCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setAll(_all)
                        .process();
                case UNLABEL ->
                    new UnlabelCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setNodeName(_k8sNodeName)
                        .process();
                case UNLINK ->
                    new UnlinkCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
            };
        } catch (ConfigurationDataException ex) {
            _logger.catching(ex);
            System.err.println("Configuration Data inconsistency(ies) prevent further processing.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (ConfigurationException ex) {
            _logger.catching(ex);
            System.err.println("Configuration inconsistency(ies) prevent further processing.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (InternalErrorException ex) {
            _logger.catching(ex);
            System.err.println("An internal error has been detected in the application.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (K8SJSONError ex) {
            _logger.catching(ex);
            System.err.println("Something went wrong while parsing JSON data from the Kubernetes cluster.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (K8SHTTPError ex) {
            _logger.catching(ex);
            var code = ex.getResponseCode();
            System.err.printf("Received unexpected %d HTTP response from the Kubernetes API server.\n", code);
            System.err.println("Please verify that you have provided the correct IP address and port information,");
            System.err.println("and that the API server (or proxy server) is up and running.");
        } catch (K8SRequestError ex) {
            _logger.catching(ex);
            System.err.println("Could not complete the request to the Kubernetes API server.");
            System.err.println("Error: " + ex.getMessage());
            System.err.println("Please verify that you have provided the correct IP address and port information,");
            System.err.println("and that the API server (or proxy server) is up and running.");
        } catch (LiqidException ex) {
            _logger.catching(ex);
            System.err.println("Could not complete the request due to an error communicating with the Liqid Cluster.");
            System.err.println("Error: " + ex.getMessage());
            System.err.println("Please verify that you have provided the correct IP address and port information,");
            System.err.println("and that the API server (or proxy server) is up and running.");
        }

        if (result) {
            System.out.printf("--- %s command completed successfully ---\n", _commandType.getToken());
        } else {
            System.err.printf("--- %s command failed ---\n", _commandType.getToken());
        }
        _logger.trace("Exiting %s", fn);
    }
}
