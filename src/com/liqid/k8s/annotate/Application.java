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

public class Application {

    static final String AUTO_COMMAND = "auto";
    static final String LABEL_COMMAND = "label";
    static final String LINK_COMMAND = "link";
    static final String NODES_COMMAND = "nodes";
    static final String RESOURCES_COMMAND = "resources";
    static final String UNLABEL_COMMAND = "unlabel";
    static final String UNLINK_COMMAND = "unlink";
    public static final String LOGGER_NAME = "Annotate";
    public static final String LOG_FILE_NAME = "liq-annotation.log";

    // If _logging is true, we do extensive logging to a log file. If false, only errors to stdout.
    private boolean _logging = false;

    private String _command;
    private int _timeoutInSeconds = 300;

    private String _liqidAddress;
    private String _liqidGroupName;
    private String _liqidPassword;
    private String _liqidUsername;
    private String _k8sNodeName;
    private String _proxyURL;
    private boolean _force = false;
    private Logger _logger;

    Application() {}

    Application setCommand(final String value) { _command = value; return this; }
    Application setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    Application setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    Application setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    Application setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    Application setForce(final boolean value) { _force = value; return this; }
    Application setLogging(final boolean flag) { _logging = flag; return this; }
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
        try {
            initLogging();
            _logger.trace("Entering %s", fn);

            switch (_command) {
                case AUTO_COMMAND ->
                    new AutoCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
                case LABEL_COMMAND ->
                    new LabelCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
                case LINK_COMMAND ->
                    new LinkCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setLiqidAddress(_liqidAddress)
                        .setLiqidGroupName(_liqidGroupName)
                        .setLiqidPassword(_liqidPassword)
                        .setLiqidUsername(_liqidUsername)
                        .process();
                case NODES_COMMAND ->
                    new NodesCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
                case RESOURCES_COMMAND ->
                    new ResourcesCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
                case UNLABEL_COMMAND ->
                    new UnlabelCommand(_logger, _proxyURL, _force, _timeoutInSeconds)
                        .setNodeName(_k8sNodeName)
                        .process();
                case UNLINK_COMMAND ->
                    new UnlinkCommand(_logger, _proxyURL, _force, _timeoutInSeconds).process();
            }
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

        _logger.trace("Exiting %s", fn);
    }
}
