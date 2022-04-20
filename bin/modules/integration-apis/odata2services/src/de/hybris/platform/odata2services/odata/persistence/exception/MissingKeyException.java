/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */

package de.hybris.platform.odata2services.odata.persistence.exception;

import de.hybris.platform.odata2services.odata.persistence.InvalidDataException;

/**
 * Exception thrown when the entityType has no valid key defined
 *
 * Will result in HttpStatus 400
 */
public class MissingKeyException extends InvalidDataException
{
	/**
	 * Constructor to create MissingKeyException
	 *
	 * @param entityType entity type
	 */
	public MissingKeyException(final String entityType)
	{
		super(String.format("There is no valid integration key defined for the current entityType [%s].", entityType), "invalid_key_definition", entityType);
	}
}
