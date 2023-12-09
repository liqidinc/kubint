/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.sdk.LiqidException;

/**
 * Creates a group with the specified name.
 */
public class CreateGroup extends Step {

//    private final String _groupName;
//
//    public CreateGroup(
//        final String groupName
//    ) {
//        super(Action.CREATE_GROUP);
//        _groupName = groupName;
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
//            context.setGroup(context._liqidClient.createGroup(_groupName));
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
//        return "Create Group " + _groupName;
//    }
}
