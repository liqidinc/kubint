/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.bearsnake.k8sclient.K8SClient;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.actions.Action;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

public class Plan {

    private final ArrayList<Action> _actions = new ArrayList<>();

    public Plan addAction(final Action action) { _actions.add(action); return this; }

    public void execute(
        final K8SClient k8SClient,
        final LiqidClient liqidClient,
        final Logger logger
    ) throws InternalErrorException, K8SException, LiqidException, ProcessingException {
        for (var action : _actions) {
            action.checkParameters();
        }

        var context = new ExecutionContext().setK8SClient(k8SClient)
                                            .setLiqidClient(liqidClient)
                                            .setLiqidInventory(LiqidInventory.createLiqidInventory(liqidClient))
                                            .setLogger(logger);

        for (int sx = 0; sx < _actions.size(); ++sx) {
            var step = _actions.get(sx);
            System.out.printf("---| Executing Step %d: %s...\n", sx + 1, step.toString());
            step.perform(context);
        }
    }

    public Collection<Action> getActions() { return new LinkedList<>(_actions); }

    public void show() {
        System.out.println();
        System.out.println("Plan----------------------------------");
        if (_actions.isEmpty()) {
            System.out.println("Nothing to be done");
        } else {
            for (int sx = 0; sx < _actions.size(); ++sx) {
                System.out.printf("| Step %d: %s\n", sx + 1, _actions.get(sx).toString());
            }
        }
        System.out.println("--------------------------------------");
    }
}
