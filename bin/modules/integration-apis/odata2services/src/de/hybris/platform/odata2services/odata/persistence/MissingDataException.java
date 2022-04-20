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
package de.hybris.platform.odata2services.odata.persistence;

import de.hybris.platform.odata2services.odata.OData2ServicesException;

import org.apache.olingo.odata2.api.commons.HttpStatusCodes;
import org.apache.olingo.odata2.api.edm.EdmEntityType;

/**
 * Exception to thrown when a required property or navigationProperty is not supplied in the request.
 *
 * Will result in HttpStatus 400
 */
public class MissingDataException extends OData2ServicesException
{
	private static final HttpStatusCodes STATUS_CODE = HttpStatusCodes.BAD_REQUEST;
	private final String entityType;
	private final String propertyName;

	/**
	 * Constructor to create MissingDataException
	 *
	 * @param message error message
	 * @param errorCode error code
	 * @param propertyName name of the property or navigationProperty
	 * @param entityType {@link EdmEntityType} name
	 */
	public MissingDataException(final String message, final String errorCode, final String propertyName, final String entityType)
	{
		super(message, STATUS_CODE, errorCode);
		this.entityType = entityType;
		this.propertyName = propertyName;
	}

	public String getEntityType()
	{
		return entityType;
	}

	public String getPropertyName()
	{
		return propertyName;
	}
}
