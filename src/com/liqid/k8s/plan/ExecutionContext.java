/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.bearsnake.klog.Logger;
import com.liqid.k8s.LiqidInventory;
import com.bearsnake.k8sclient.K8SClient;
import com.liqid.sdk.LiqidClient;

public class ExecutionContext {

    private K8SClient _k8sClient;
    private LiqidClient _liqidClient;
    private LiqidInventory _liqidInventory;
    private Logger _logger;

    public K8SClient getK8SClient() { return _k8sClient; }
    public LiqidClient getLiqidClient() { return _liqidClient; }
    public LiqidInventory getLiqidInventory() { return _liqidInventory; }
    public Logger getLogger() { return _logger; }

    public ExecutionContext setK8SClient(final K8SClient value) { _k8sClient = value; return this; }
    public ExecutionContext setLiqidClient(final LiqidClient value) { _liqidClient = value; return this; }
    public ExecutionContext setLiqidInventory(final LiqidInventory value) { _liqidInventory = value; return this; }
    public ExecutionContext setLogger(final Logger value) { _logger = value; return this; }
}
