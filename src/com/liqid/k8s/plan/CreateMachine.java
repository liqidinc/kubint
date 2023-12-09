/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.sdk.LiqidException;

/**
 * Creates a machine with the specified name, as part of a specified group.
 */
public class CreateMachine extends Step {
//
//    private final String _machineName;
//
//    public CreateMachine(
//        final String machineName
//    ) {
//        super(Action.CREATE_MACHINE);
//        _machineName = machineName;
//    }
//
//    @Override
//    public void perform(
//        final ExecutionContext context
//    ) throws ProcessingException {
//        var fn = "perform";
//        context._logger.trace("Entering %s", fn);
//
//        try {
//            var groupId = context.getGroup().getGroupId();
//            var mach = context._liqidClient.createMachine(groupId, _machineName);
//            context.addMachine(mach);
//        } catch (LiqidException lex) {
//            context._logger.catching(lex);
//            var t = new ProcessingException(lex);
//            context._logger.throwing(t);
//            throw t;
//        }
//
//        context._logger.trace("%s returning", fn);
//    }
//
//    @Override
//    public String toString() {
//        return "Create Machine " + _machineName;
//    }
}
