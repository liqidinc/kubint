/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.LinkedList;

public class AssignToGroup extends Step {
//
//    private final String _groupName;
//    private final Collection<String> _deviceNames = new LinkedList<>();
//
//    public AssignToGroup(
//        final String groupName,
//        final Collection<String> deviceNames
//    ) {
//        super(Action.ASSIGN_RESOURCES_TO_GROUP);
//        _groupName = groupName;
//        _deviceNames.addAll(deviceNames);
//    }
//
//    @Override
//    public void perform(
//        final ExecutionContext context
//    ) throws ProcessingException {
//        var fn = "perform";
//        context._logger.trace("Entering %s", fn);
//
//        boolean editInProgress = false;
//        Integer groupId = null;
//        try {
//            groupId = context._liqidClient.getGroupIdByName(_groupName);
//            context._liqidClient.groupPoolEdit(groupId);
//            editInProgress = true;
//            for (var devName : _deviceNames) {
//                var devStat = context.getDeviceStatusByName(devName);
//                context._liqidClient.addDeviceToGroup(devStat.getDeviceId(), groupId);
//            }
//            context._liqidClient.groupPoolDone(groupId);
//            editInProgress = false;
//        } catch (LiqidException lex) {
//            context._logger.catching(lex);
//            var t = new ProcessingException(lex);
//            context._logger.throwing(t);
//            throw t;
//        } finally {
//            if (editInProgress) {
//                try {
//                    context._liqidClient.cancelGroupPoolEdit(groupId);
//                } catch (LiqidException lex2) {
//                    //  nothing can be done here
//                    context._logger.catching(lex2);
//                }
//            }
//
//            context._logger.trace("%s returning", fn);
//        }
//    }
//
//    @Override
//    public String toString() {
//        return String.format("Assign to Group %s: %s", _groupName, String.join(", ", _deviceNames));
//    }
}
