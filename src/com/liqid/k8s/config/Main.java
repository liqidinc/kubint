/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.komando.*;
import com.bearsnake.komando.exceptions.*;
import com.bearsnake.komando.values.CommandValue;
import com.bearsnake.komando.values.FixedPointValue;
import com.bearsnake.komando.values.StringValue;
import com.bearsnake.komando.values.Value;
import com.bearsnake.komando.values.ValueType;
import com.liqid.k8s.Constants;

import java.util.List;

import static com.liqid.k8s.config.CommandType.CLEANUP;
import static com.liqid.k8s.config.CommandType.EXECUTE;
import static com.liqid.k8s.config.CommandType.PLAN;
import static com.liqid.k8s.config.CommandType.VALIDATE;

public class Main {

    /*
    config cleanup
        -px,--proxy-url={proxy_url}

    config execute
        -px,--proxy-url={proxy_url}

    config plan
        -px,--proxy-url={proxy_url}

    config validate
        -px,--proxy-url={proxy_url}
    */

    private static final CommandValue CV_CLEANUP = new CommandValue(CLEANUP.getToken());
    private static final CommandValue CV_EXECUTE = new CommandValue(CommandType.EXECUTE.getToken());
    private static final CommandValue CV_PLAN = new CommandValue(PLAN.getToken());
    private static final CommandValue CV_VALIDATE = new CommandValue(CommandType.VALIDATE.getToken());

    private static final Switch K8S_PROXY_URL_SWITCH;
    private static final Switch LOGGING_SWITCH;
    private static final Switch TIMEOUT_SWITCH;
    private static final CommandArgument COMMAND_ARG;

    static {
        try {
            K8S_PROXY_URL_SWITCH =
                new ArgumentSwitch.Builder().setShortName("px")
                                            .setLongName("proxy-url")
                                            .setIsRequired(true)
                                            .setValueName("k8x_proxy_url")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the kubectl proxy server.")
                                            .build();
            LOGGING_SWITCH =
                new SimpleSwitch.Builder().setShortName("l")
                                          .setLongName("logging")
                                          .addDescription("Enables logging for error diagnostics.")
                                          .addDescription("Information will be written to " + com.liqid.k8s.annotate.Application.LOG_FILE_NAME)
                                          .build();
            TIMEOUT_SWITCH =
                new ArgumentSwitch.Builder().setShortName("t")
                                            .setLongName("timeout")
                                            .setIsRequired(false)
                                            .setValueName("seconds")
                                            .setValueType(ValueType.FIXED_POINT)
                                            .addDescription("Timeout value for back-end network communication in seconds.")
                                            .build();
            COMMAND_ARG =
                new CommandArgument.Builder().addDescription(CLEANUP.getToken())
                                             .addDescription("  Cleans up the Liqid Configuration.")
                                             .addDescription("  Detaches all resources from the various nodes, then deletes all the machines in the ")
                                             .addDescription("  configured Kubernetes group, returning all resources to the group free pool.")
                                             .addDescription(EXECUTE.getToken())
                                             .addDescription("  Consults the various Kubernetes node annotations, develops a plan to achieve the requested")
                                             .addDescription("  resource layout, then executes the plan.")
                                             .addDescription(PLAN.getToken())
                                             .addDescription("  Consults the various Kubernetes node annotations, develops a plan to achieve the requested")
                                             .addDescription("  resource layout, displays the plan, but does not execute it.")
                                             .addDescription(VALIDATE.getToken())
                                             .addDescription("  Ensures the validity of the Liqid Cluster and Kubernetes Cluster configurations")
                                             .addDescription("    in comparison to the various Kubernetes node annotations.")
                                             .addCommandValue(CV_CLEANUP)
                                             .addCommandValue(CV_EXECUTE)
                                             .addCommandValue(CV_PLAN)
                                             .addCommandValue(CV_VALIDATE)
                                             .build();
        } catch (KomandoException e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------------
    // helper functions
    // ------------------------------------------------------------------------

    // Takes the *first* Value object which caller promises is a StringValue, and returns the extracted String.
    // If the value is not a StringValue, we return null.
    private static String getSingleString(
        final List<Value> valueList
    ) {
        String result = null;
        if ((valueList != null) && !valueList.isEmpty()) {
            result = ((StringValue)valueList.get(0)).getValue();
        }
        return result;
    }

    /**
     * Converts command line nonsense into configuration values which make sense.
     */
    private static boolean configureApplication(
        final Application application,
        final String[] args
    ) {
        try {
            CommandLineHandler clh = new CommandLineHandler();
            clh.addCanonicalHelpSwitch()
               .addCanonicalVersionSwitch()
               .addSwitch(LOGGING_SWITCH)
               .addSwitch(K8S_PROXY_URL_SWITCH)
               .addSwitch(TIMEOUT_SWITCH)
               .addCommandArgument(COMMAND_ARG);

            var result = clh.processCommandLine(args);
            if (result.hasErrors() || result.hasWarnings()) {
                for (var msg : result._messages) {
                    System.err.println(msg);
                }
                System.err.println("Use --help for usage assistance");
                if (result.hasErrors()) {
                    return false;
                }
            } else if (result.isHelpRequested()) {
                clh.displayUsage("");
                return false;
            } else if (result.isVersionRequested()) {
                System.out.println("k8sIntegration Version " + Constants.VERSION);
                return false;
            }

            application.setLogging(result._switchSpecifications.containsKey(LOGGING_SWITCH))
                       .setCommandType(CommandType.get(result._commandValue.getValue()))
                       .setProxyURL(getSingleString(result._switchSpecifications.get(K8S_PROXY_URL_SWITCH)));

            var values = result._switchSpecifications.get(TIMEOUT_SWITCH);
            if ((values != null) && !values.isEmpty()) {
                application.setTimeoutInSeconds((int) (long) ((FixedPointValue) values.get(0)).getValue());
            }

            return true;
        } catch (KomandoException ex) {
            System.out.println("Internal error:" + ex.getMessage());
            return false;
        }
    }

    // ------------------------------------------------------------------------
    // program entry point
    // ------------------------------------------------------------------------

    public static void main(
        final String[] args
    ) {
        var app = new Application();
        if (configureApplication(app, args)) {
            try {
                app.process();
            } catch (Exception ex) {
                System.out.println("Caught " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }
}
