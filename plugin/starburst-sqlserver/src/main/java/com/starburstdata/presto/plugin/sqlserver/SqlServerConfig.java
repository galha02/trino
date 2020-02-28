/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import io.airlift.configuration.Config;

public class SqlServerConfig
{
    private boolean impersonationEnabled;

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("sqlserver.impersonation.enabled")
    public SqlServerConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }
}
