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
package de.hybris.platform.odata2services.odata.persistence.lookup;

import org.apache.olingo.odata2.api.edm.EdmEntityType;

/**
 * Exception for cases when a navigation property does not exist in the DB during lookup.
 */
public class ItemNotFoundForKeyNavigationPropertyException extends RuntimeException
{
	private final String propertyName;
	private final String entityTypeName;

	/**
	 * Constructor to create ItemNotFoundForKeyNavigationPropertyException
	 *
	 * @param entityTypeName {@link EdmEntityType} name
	 * @param propertyName the name of the
	 */
	public ItemNotFoundForKeyNavigationPropertyException(final String entityTypeName, final String propertyName)
	{
		this.entityTypeName = entityTypeName;
		this.propertyName = propertyName;
	}

	public String getPropertyName()
	{
		return propertyName;
	}

	public String getEntityTypeName()
	{
		return entityTypeName;
	}
}
