/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.odata2services.odata.persistence.populator.processor;

import static de.hybris.platform.odata2services.odata.EdmAnnotationUtils.isKeyProperty;

import de.hybris.platform.core.model.ItemModel;
import de.hybris.platform.core.model.type.AttributeDescriptorModel;
import de.hybris.platform.integrationservices.model.TypeAttributeDescriptor;
import de.hybris.platform.odata2services.odata.persistence.ItemConversionRequest;
import de.hybris.platform.odata2services.odata.persistence.StorageRequest;
import de.hybris.platform.odata2services.odata.persistence.exception.InvalidPropertyValueException;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimitivePropertyProcessor extends AbstractPropertyProcessor
{
	private static final Logger LOG = LoggerFactory.getLogger(PrimitivePropertyProcessor.class);
	private static final Calendar END_OF_9999 = new Calendar.Builder()
					.setDate(9999, Calendar.DECEMBER, 31)
					.setTimeOfDay(23, 59, 59)
					.build();

	@Override
	protected  boolean isApplicable(final TypeAttributeDescriptor typeAttributeDescriptor)
	{
		return typeAttributeDescriptor.isPrimitive() && !typeAttributeDescriptor.isCollection();
	}

	@Override
	protected void processItemInternal(final ItemModel item, final String entryPropertyName, final Object value,
			final StorageRequest request) throws EdmException
	{
		if (!getModelService().isNew(item)
				&& isKeyProperty(request.getEntityType().getProperty(entryPropertyName)))
		{
			return;
		}

		final String entityName = request.getEntityType().getName();
		LOG.debug("{}.{} set to '{}'", entityName, entryPropertyName, value);

		final AttributeDescriptorModel attributeDescriptor = getAttributeDescriptor(item, entryPropertyName, request);
		final String itemPropertyName = attributeDescriptor.getQualifier();

		if (value instanceof Calendar) // ECP does not handle Calendar
		{
			getModelService().setAttributeValue(item, itemPropertyName, adjustDateIfGreaterThanYear9999(value));
		}
		else if (attributeDescriptor.getLocalized())
		{
			getModelService().setAttributeValue(item, itemPropertyName, Collections.singletonMap(request.getContentLocale(), value));
		}
		else if (isCharacterType(attributeDescriptor))
		{
			getModelService().setAttributeValue(item, itemPropertyName, asCharacter(attributeDescriptor, value));
		}
		else
		{
			getModelService().setAttributeValue(item, itemPropertyName, value);
		}
	}

	private Character asCharacter(final AttributeDescriptorModel descriptor, final Object value)
	{
		if (value instanceof Character || value == null)
		{
			return (Character) value;
		}
		final String str = String.valueOf(value);
		if (str.length() == 1)
		{
			return str.charAt(0);
		}
		throw new InvalidPropertyValueException("Invalid value '" + value + "' for Character attribute " + descriptor.getQualifier());
	}

	private boolean isCharacterType(final AttributeDescriptorModel descriptor)
	{
		return Character.class.getName().equals(descriptor.getAttributeType().getCode());
	}

	private Date adjustDateIfGreaterThanYear9999(final Object value)
	{
		final Calendar payloadCalendar = (Calendar) value;
		return payloadCalendar.compareTo(END_OF_9999) > 0 ?
				END_OF_9999.getTime() :
				payloadCalendar.getTime();
	}

	@Override
	protected void processEntityInternal(final ODataEntry oDataEntry, final String propertyName, final Object value,
			final ItemConversionRequest request)
	{
		final Object propertyValue;
		if( value instanceof Date )
		{
			propertyValue = DateUtils.toCalendar((Date) value);
		}
		else
		{
			propertyValue = value;
		}
		oDataEntry.getProperties().putIfAbsent(propertyName, propertyValue);
	}

	@Override
	protected boolean shouldPropertyBeConverted(final ItemConversionRequest conversionRequest, final String propertyName)
	{
		return true;
	}
}