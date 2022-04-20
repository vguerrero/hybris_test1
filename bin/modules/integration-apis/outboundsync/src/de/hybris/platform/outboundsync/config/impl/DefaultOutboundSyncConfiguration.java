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

package de.hybris.platform.outboundsync.config.impl;

import de.hybris.platform.integrationservices.config.BaseIntegrationServicesConfiguration;

/**
 * Provides access methods to configurations related to the Inbound Services
 */
public class DefaultOutboundSyncConfiguration extends BaseIntegrationServicesConfiguration implements OutboundSyncConfiguration
{
	private static final String OUTBOUNDSYNC_MAX_RETRIES = "outboundsync.max.retries";
	private static final String ITEM_GROUP_SIZE_MAX = "outboundsync.item.group.size.max";
	private static final String ITEM_GROUPING_TIMEOUT = "outboundsync.item.grouping.timeout";

	@Override
	public int getMaxOutboundSyncRetries()
	{
		return getIntegerProperty(OUTBOUNDSYNC_MAX_RETRIES, 0);
	}

	@Override
	public int getItemGroupSizeMax()
	{
		return getIntegerProperty(ITEM_GROUP_SIZE_MAX, 0);
	}

	@Override
	public int getItemGroupingTimeout()
	{
		return getIntegerProperty(ITEM_GROUPING_TIMEOUT, 0);
	}
}
