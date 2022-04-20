/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.outboundsync.job;

import de.hybris.platform.outboundsync.dto.OutboundItemDTO;

public interface ItemPKPopulator
{
	/**
	 * Populates the pk with the root item pk for the integrationObject under concern
	 *
	 * @param itemDTO A DTO with the information about the changes in an item.
	 */
	void populatePK(final OutboundItemDTO itemDTO);


}
