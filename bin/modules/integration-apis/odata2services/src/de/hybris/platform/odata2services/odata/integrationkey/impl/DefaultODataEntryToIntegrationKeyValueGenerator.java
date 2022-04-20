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
package de.hybris.platform.odata2services.odata.integrationkey.impl;

import static de.hybris.platform.odata2services.odata.EdmAnnotationUtils.getAliasTextIfPresent;

import de.hybris.platform.integrationservices.integrationkey.IntegrationKeyCalculationException;
import de.hybris.platform.integrationservices.integrationkey.impl.AbstractIntegrationKeyValueGenerator;
import de.hybris.platform.integrationservices.integrationkey.impl.IntegrationKeyAlias;
import de.hybris.platform.integrationservices.integrationkey.impl.IntegrationKeyValue;

import java.util.Calendar;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import com.google.common.base.Preconditions;

public class DefaultODataEntryToIntegrationKeyValueGenerator extends AbstractIntegrationKeyValueGenerator<EdmEntitySet, ODataEntry>
{
	@Override
	public String generate(final EdmEntitySet entitySet, final ODataEntry oDataEntry)
	{
		try
		{
			Preconditions.checkArgument(entitySet != null, "Cannot calculate integration key value for null edm entity set");
			Preconditions.checkArgument(oDataEntry != null, "Cannot calculate integration key value for null oDataEntry");
			Preconditions.checkArgument(entitySet.getEntityType() != null, "Cannot calculate integration key value for null entity type");

			final String integrationKeyAlias = getAliasTextIfPresent(entitySet.getEntityType());

			return (StringUtils.isBlank(integrationKeyAlias)) ?
				StringUtils.EMPTY :
				integrationKeyValueFrom(entitySet, oDataEntry, integrationKeyAlias);
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	protected String integrationKeyValueFrom(final EdmEntitySet entitySet, final ODataEntry entry, final String integrationKeyAlias)
	{
		final IntegrationKeyAlias alias = new IntegrationKeyAlias(integrationKeyAlias);
		final IntegrationKeyValue integrationKeyValue = new IntegrationKeyValue();

		calcIntegrationKeyRecursively(entitySet, entry, alias, integrationKeyValue);
		return buildIntegrationKeyValueMatchingAliasOrder(integrationKeyValue, alias);
	}

	protected void calcIntegrationKeyRecursively(final EdmEntitySet type, final ODataEntry entry,
			final IntegrationKeyAlias alias, final IntegrationKeyValue keyValue)
	{

		for (final String aliasType : alias.getTypes())
		{
			final Optional<String> navPropNameOptional = findMatchingNavigationPropertyIn(type, aliasType);
			if (navPropNameOptional.isPresent())
			{
				populateKeyValueFromNavigationProperty(type, entry, alias, keyValue, navPropNameOptional.get());
			}
			else
			{
				if (isAliasTypeMatchesCurrentEntityType(type, aliasType))
				{
					populateKeyValueFromSimpleProperty(type, entry, keyValue, alias.getProperties(aliasType));
				}
			}
		}
	}

	private boolean isAliasTypeMatchesCurrentEntityType(final EdmEntitySet type, final String aliasType)
	{
		return aliasType.equals(getTypeCode(type));
	}

	protected void populateKeyValueFromSimpleProperty(final EdmEntitySet type,
			final ODataEntry entry,
			final IntegrationKeyValue keyValue,
			final List<String> propertyNames)
	{
		propertyNames.forEach(propertyName -> setPropertyValueInIntegrationKey(keyValue, type, entry, propertyName));
	}

	protected void setPropertyValueInIntegrationKey(final IntegrationKeyValue keyValue, final EdmEntitySet type, final ODataEntry entry, final String propertyName)
	{
		if (entry.getProperties().containsKey(propertyName))
		{
			final Object attributeValue = getProperty(entry, propertyName);
			keyValue.addProperty(getTypeCode(type), transformValueToString(attributeValue));
		}
	}

	protected void populateKeyValueFromNavigationProperty(final EdmEntitySet entitySet, final ODataEntry entry,
			final IntegrationKeyAlias aliasComponents, final IntegrationKeyValue integrationKeyValue, final String propertyName)
	{
		try
		{
			final EdmEntityType entityType = entitySet.getEntityType();
			final Object value = entry.getProperties().get(propertyName);
			final EdmTyped type = entityType.getProperty(propertyName);
			if (value instanceof ODataEntry && type instanceof EdmNavigationProperty)
			{
				calcIntegrationKeyRecursively(
						entitySet.getRelatedEntitySet((EdmNavigationProperty) type),
						(ODataEntry) value,
						aliasComponents,
						integrationKeyValue
				);
			}
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	protected Optional<String> findMatchingNavigationPropertyIn(final EdmEntitySet entitySet, final String entityName)
	{
		try
		{
			final EdmEntityType entityType = entitySet.getEntityType();
			return entityType.getNavigationPropertyNames()
					.stream()
					.map(name -> getNavigationProperty(entityType, name))
					.filter(prop -> getNavigationPropertyToRole(prop).equalsIgnoreCase(entityName))
					.map(this::getNavigationPropertyName)
					.findFirst();
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	protected EdmNavigationProperty getNavigationProperty(final EdmEntityType entityType, final String name)
	{
		try
		{
			return (EdmNavigationProperty) entityType.getProperty(name);
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	protected String getNavigationPropertyToRole(final EdmNavigationProperty property)
	{
		try
		{
			return property.getToRole();
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	protected String getNavigationPropertyName(final EdmNavigationProperty property)
	{
		try
		{
			return property.getName();
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	@Override
	protected String getTypeCode(final EdmEntitySet entitySet)
	{
		try
		{
			return entitySet.getEntityType().getName();
		}
		catch (final EdmException e)
		{
			throw new IntegrationKeyCalculationException(e);
		}
	}

	@Override
	protected Object getProperty(final ODataEntry entry, final String propertyName)
	{
		return entry.getProperties().get(propertyName);
	}

	@Override
	protected String transformValueToString(final Object attributeValue)
	{
		return attributeValue instanceof Calendar
				? String.valueOf(((Calendar) attributeValue).getTimeInMillis())
				: String.valueOf(attributeValue);
	}
}
