/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.sdk.LiqidException;

public class ClearConfiguration extends Step {

//    public ClearConfiguration() {
//        super(Action.CLEAR_CONFIGURATION);
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
//            context._liqidClient.clearGroups();
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
//        return "Clear Configuration";
//    }
}
