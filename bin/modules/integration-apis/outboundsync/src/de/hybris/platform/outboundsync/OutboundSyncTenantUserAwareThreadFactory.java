/*
 * [y] hybris Platform
 *
 * Copyright (c) 2019 SAP SE or an SAP affiliate company.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.outboundsync;

import de.hybris.platform.core.Tenant;
import de.hybris.platform.core.TenantAwareThreadFactory;
import de.hybris.platform.jalo.JaloSession;

/**
 * A {@link java.util.concurrent.ThreadFactory} that is aware of the tenant and user for the
 * {@link JaloSession}. Currently, the user is set to the Admin user.
 */
public class OutboundSyncTenantUserAwareThreadFactory extends TenantAwareThreadFactory
{
	public OutboundSyncTenantUserAwareThreadFactory(final Tenant tenant)
	{
		super(tenant);
	}

	public OutboundSyncTenantUserAwareThreadFactory(final Tenant tenant, final JaloSession session)
	{
		super(tenant, session);
	}

	@Override
	protected void afterPrepareThread()
	{
		super.afterPrepareThread();
		final JaloSession session = JaloSession.getCurrentSession();
		session.getSessionContext().setUser(session.getUserManager().getAdminEmployee());
	}
}
