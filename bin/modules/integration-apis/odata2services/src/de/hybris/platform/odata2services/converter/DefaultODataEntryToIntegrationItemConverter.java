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

package de.hybris.platform.odata2services.converter;

import de.hybris.platform.integrationservices.item.DefaultIntegrationItem;
import de.hybris.platform.integrationservices.item.IntegrationItem;
import de.hybris.platform.integrationservices.model.TypeAttributeDescriptor;
import de.hybris.platform.integrationservices.model.TypeDescriptor;
import de.hybris.platform.integrationservices.service.ItemTypeDescriptorService;
import de.hybris.platform.odata2services.constants.Odata2servicesConstants;
import de.hybris.platform.odata2services.odata.persistence.ModelEntityService;
import de.hybris.platform.odata2services.odata.persistence.exception.MissingPropertyException;
import de.hybris.platform.odata2services.odata.processor.ServiceNameExtractor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.olingo.odata2.api.edm.EdmAssociation;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.apache.olingo.odata2.api.processor.ODataContext;
import org.springframework.beans.factory.annotation.Required;

import com.google.common.base.Preconditions;

public class DefaultODataEntryToIntegrationItemConverter implements ODataEntryToIntegrationItemConverter
{
	private static final AttributeValueHandler DEFAULT_ATTRIBUTE_VALUE_HANDLER = IntegrationItem::setAttribute;
	private ModelEntityService modelService;
	private ItemTypeDescriptorService itemTypeDescriptorService;
	private ServiceNameExtractor serviceNameExtractor;

	@Override
	public IntegrationItem convert(
			final ODataContext context,
			@NotNull final EdmEntitySet entitySet,
			@NotNull final ODataEntry entry)
			throws EdmException
	{
		Preconditions.checkArgument(entitySet != null, "Entity set is required for the entry conversion");
		Preconditions.checkArgument(entry != null, "Entry is required for the conversion");

		final String typeCode = entitySet.getEntityType().getName();
		final String objectCode = getServiceNameExtractor().extract(context);
		final TypeDescriptor typeDescriptor = getItemTypeDescriptorService().getTypeDescriptor(objectCode, typeCode)
				.orElseThrow(() -> new IntegrationObjectItemNotFoundException(objectCode, typeCode));
		return convert(typeDescriptor, entitySet, entry);
	}

	private IntegrationItem convert(final TypeDescriptor typeDesc, final EdmEntitySet entitySet, final ODataEntry entry) throws EdmException
	{
		final DefaultIntegrationItem item = createIntegrationItem(typeDesc, entitySet, entry);

		for (final Map.Entry<String, Object> ent : entry.getProperties().entrySet())
		{
			final Object value = convertedValue(typeDesc, entitySet, ent.getKey(), ent.getValue());
			getAttributeValueHandler(value).setItemAttribute(item, ent.getKey(), value);
		}
		return item;
	}

	/**
	 * Instantiates an integration item.
	 * @param typeDesc type descriptor to specify what type the item should have
	 * @param entitySet an ODataSet to which the OData entry is being posted
	 * @param entry payload of the request
	 * @return an integration item instance corresponding to the parameters specified
	 */
	protected DefaultIntegrationItem createIntegrationItem(final TypeDescriptor typeDesc, final EdmEntitySet entitySet, final ODataEntry entry)
	{
		// we need to replace this method with modelService.addIntegrationKeyToODataEntry(TypeDescriptor, ODataEntry)
		final String integrationKey = getModelEntityService().addIntegrationKeyToODataEntry(entitySet, entry);
		return new DefaultIntegrationItem(typeDesc, integrationKey);
	}

	private Object convertedValue(final TypeDescriptor type, final EdmEntitySet entitySet, final String attrName, final Object attrValue) throws EdmException
	{
		if (attrValue instanceof ODataEntry)
		{
			return convertedODataEntry(type, entitySet, attrName, (ODataEntry) attrValue);
		}
		if (attrValue instanceof ODataFeed)
		{
			return convertedCollection(type, entitySet, attrName, ((ODataFeed) attrValue).getEntries());
		}
		if (attrValue instanceof Collection)
		{
			return convertedCollection(type, entitySet, attrName, (Collection) attrValue);
		}
		return attrValue;
	}

	private Object convertedODataEntry(final TypeDescriptor type, final EdmEntitySet entitySet, final String attrName, final ODataEntry entry) throws EdmException
	{
		if (isReferringLocalizedAttributesType(entitySet, attrName))
		{
			return LocalizedAttributes.createFrom(entry);
		}
		final TypeDescriptor nestedType = type.getAttribute(attrName)
				.map(TypeAttributeDescriptor::getAttributeType)
				.orElseThrow(() -> new MissingPropertyException(type.getItemCode(), attrName));
		return convert(nestedType, nestedEntitySet(entitySet, attrName), entry);
	}

	private EdmEntitySet nestedEntitySet(final EdmEntitySet entitySet, final String attrName) throws EdmException
	{
		final EdmNavigationProperty property = (EdmNavigationProperty) entitySet.getEntityType().getProperty(attrName);
		return entitySet.getRelatedEntitySet(property);
	}

	private Object convertedCollection(final TypeDescriptor type, final EdmEntitySet entitySet, final String attrName, final Collection value) throws EdmException
	{
		final Collection<Object> converted = new ArrayList<>(value.size());
		for (final Object el : value)
		{
			converted.add(convertedValue(type, entitySet, attrName, el));
		}
		return isReferringLocalizedAttributesType(entitySet, attrName)
				? converted.stream()
						.map(LocalizedAttributes.class::cast)
						.reduce(LocalizedAttributes.EMPTY, LocalizedAttributes::combine)
				: converted;
	}

	private boolean isReferringLocalizedAttributesType(final EdmEntitySet entitySet, final String attrName) throws EdmException
	{
		if (Odata2servicesConstants.LOCALIZED_ATTRIBUTE_NAME.equals(attrName))
		{
			final EdmTyped property = entitySet.getEntityType().getProperty(attrName);
			if (property instanceof EdmNavigationProperty)
			{
				final EdmAssociation relationship = ((EdmNavigationProperty) property).getRelationship();
				return relationship.getEnd2().getEntityType().getName().startsWith(Odata2servicesConstants.LOCALIZED_ENTITY_TYPE_PREFIX);
			}
		}
		return false;
	}

	private static AttributeValueHandler getAttributeValueHandler(final Object value)
	{
		return value instanceof LocalizedAttributes
				? ((i, a, v) -> ((LocalizedAttributes) value).forEachAttribute(i::setAttribute))
				: DEFAULT_ATTRIBUTE_VALUE_HANDLER;
	}

	@Required
	public void setModelEntityService(final ModelEntityService service)
	{
		modelService = service;
	}

	protected ModelEntityService getModelEntityService()
	{
		return modelService;
	}

	@Required
	public void setItemTypeDescriptorService(final ItemTypeDescriptorService service)
	{
		itemTypeDescriptorService = service;
	}

	protected ItemTypeDescriptorService getItemTypeDescriptorService()
	{
		return itemTypeDescriptorService;
	}

	@Required
	public void setServiceNameExtractor(final ServiceNameExtractor extractor)
	{
		serviceNameExtractor = extractor;
	}

	protected ServiceNameExtractor getServiceNameExtractor()
	{
		return serviceNameExtractor;
	}

	private interface AttributeValueHandler
	{
		void setItemAttribute(IntegrationItem item, String attr, Object value);
	}
}
