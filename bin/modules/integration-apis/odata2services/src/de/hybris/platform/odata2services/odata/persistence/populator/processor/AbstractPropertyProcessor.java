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
package de.hybris.platform.odata2services.odata.persistence.populator.processor;


import static de.hybris.platform.integrationservices.constants.IntegrationservicesConstants.INTEGRATION_KEY_PROPERTY_NAME;
import static de.hybris.platform.odata2services.constants.Odata2servicesConstants.LOCALIZED_ATTRIBUTE_NAME;

import de.hybris.platform.core.HybrisEnumValue;
import de.hybris.platform.core.model.ItemModel;
import de.hybris.platform.core.model.type.AttributeDescriptorModel;
import de.hybris.platform.integrationservices.model.DescriptorFactory;
import de.hybris.platform.integrationservices.model.IntegrationObjectItemModel;
import de.hybris.platform.integrationservices.model.TypeAttributeDescriptor;
import de.hybris.platform.integrationservices.model.TypeDescriptor;
import de.hybris.platform.integrationservices.service.IntegrationObjectService;
import de.hybris.platform.odata2services.odata.persistence.AbstractRequest;
import de.hybris.platform.odata2services.odata.persistence.ItemConversionRequest;
import de.hybris.platform.odata2services.odata.persistence.StorageRequest;
import de.hybris.platform.servicelayer.exceptions.ModelNotFoundException;
import de.hybris.platform.servicelayer.model.ModelService;
import de.hybris.platform.servicelayer.type.TypeService;

import java.util.Map;
import java.util.Optional;

import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

public abstract class AbstractPropertyProcessor implements PropertyProcessor
{
	private static final Logger LOG = LoggerFactory.getLogger(AbstractPropertyProcessor.class);

	private ModelService modelService;
	private IntegrationObjectService integrationObjectService;
	private TypeService typeService;
	private DescriptorFactory descriptorFactory;

	@Override
	public void processItem(final ItemModel item, final StorageRequest request) throws EdmException
	{
		final Map<String, Object> properties = request.getODataEntry().getProperties();

		for (final Map.Entry<String, Object> entry : properties.entrySet())
		{
			final String propertyName = entry.getKey();
			final Object propertyValue = entry.getValue();

			if (isItemPropertySettable(item, propertyName, request))
			{
				final TypeAttributeDescriptor attributeDescriptor = getTypeAttributeDescriptor(item, request, propertyName);
				final PropertyMetadata propertyMetadata = PropertyMetadata.create(attributeDescriptor, propertyName);
				if (isPropertySupported(propertyMetadata))
				{
					processItemInternal(item, propertyName, propertyValue, request);
				}
			}
		}
	}

	private TypeAttributeDescriptor getTypeAttributeDescriptor(final Object value, final AbstractRequest request, final String propertyName)
	{
		final String itemTypeCode = getItemTypeCode(value);
		final String integrationObjectCode = request.getIntegrationObjectCode();
		final IntegrationObjectItemModel integrationObjectItem = findMatchingIntegrationObjectModel(integrationObjectCode, itemTypeCode);
		return findTypeDescriptorAttributeForItem(integrationObjectItem, propertyName)
				.orElse(null);
	}

	protected final IntegrationObjectItemModel findMatchingIntegrationObjectModel(final String integrationObjectCode,
	                                                                              final String itemTypeCode)
	{
		try
		{
			return getIntegrationObjectService()
					.findIntegrationObjectItemByTypeCode(integrationObjectCode, itemTypeCode);
		}
		catch (final ModelNotFoundException e)
		{
			LOG.debug("The integrationObjectItem for typeCode {} could not be found. Looking up by parent types.", itemTypeCode);
			return findIntegrationObjectItemFallback(itemTypeCode, integrationObjectCode);
		}
	}

	private IntegrationObjectItemModel findIntegrationObjectItemFallback(final String itemTypeCode,
	                                                                     final String integrationObjectCode)
	{
		try
		{
			return getIntegrationObjectService()
					.findIntegrationObjectItemByParentTypeCode(integrationObjectCode, itemTypeCode);
		}
		catch (final ModelNotFoundException e)
		{
			LOG.debug("The fallback strategy search for typeCode {} could not find any model.", itemTypeCode);
			return null;
		}
	}

	protected Optional<TypeAttributeDescriptor> findTypeDescriptorAttributeForItem(final IntegrationObjectItemModel integrationObjectItem, final String integrationItemAttributeName)
	{
		return asTypeDescriptor(integrationObjectItem)
				.flatMap(type -> type.getAttribute(integrationItemAttributeName));
	}

	protected final Optional<TypeDescriptor> asTypeDescriptor(final IntegrationObjectItemModel model)
	{
		return Optional.ofNullable(model)
				.map(descriptorFactory::createItemTypeDescriptor);
	}

	@Override
	public void processEntity(final ODataEntry oDataEntry, final ItemConversionRequest conversionRequest) throws EdmException
	{
		for (final String propertyName : conversionRequest.getAllPropertyNames())
		{
			if (isPropertySupported(propertyName))
			{
				final TypeAttributeDescriptor attributeDescriptor = getTypeAttributeDescriptor(conversionRequest.getValue(), conversionRequest, propertyName);
				final PropertyMetadata propertyMetadata = PropertyMetadata.create(attributeDescriptor, propertyName);
				if (isPropertySupported(propertyMetadata) && shouldPropertyBeConverted(conversionRequest, propertyName))
				{
					final Object propertyValue = readPropertyValue(conversionRequest, propertyName);
					processEntityInternal(oDataEntry, propertyName, propertyValue, conversionRequest);
				}
			}
		}
	}

	protected Object readPropertyValue(final ItemConversionRequest request, final String propertyName) throws EdmException
	{
		final TypeAttributeDescriptor attributeDescriptor = getTypeAttributeDescriptor(request.getValue(), request, propertyName);

		return readPropertyValue(attributeDescriptor, request);
	}

	protected Object readPropertyValue(final TypeAttributeDescriptor descriptor, final ItemConversionRequest conversionRequest)
	{
		return descriptor.isLocalized() ?
				descriptor.accessor().getValue(conversionRequest.getValue(), conversionRequest.getAcceptLocale()) :
				descriptor.accessor().getValue(conversionRequest.getValue());
	}

	protected boolean shouldPropertyBeConverted(final ItemConversionRequest request, final String propertyName) throws EdmException
	{
		return request.isPropertyValueShouldBeConverted(propertyName);
	}

	protected boolean isItemPropertySettable(final ItemModel item, final String propertyName, final StorageRequest request) throws EdmException
	{
		if (isPropertySupported(propertyName))
		{
			final AttributeDescriptorModel attributeDescriptor = getAttributeDescriptor(item, propertyName, request);
			return getModelService().isNew(item) || attributeDescriptor.getWritable();
		}
		return false;
	}

	protected AttributeDescriptorModel getAttributeDescriptor(final ItemModel item, final String propertyName, final StorageRequest request) throws EdmException
	{
		final String integrationObjectItemCode = request.getEntityType().getName();
		final String itemPropertyName = getIntegrationObjectService()
				.findItemAttributeName(request.getIntegrationObjectCode(), integrationObjectItemCode, propertyName);
		return getAttributeDescriptor(item, itemPropertyName);
	}

	private AttributeDescriptorModel getAttributeDescriptor(final Object value, final String propertyName)
	{
		return getTypeService().getAttributeDescriptor(getItemTypeCode(value), propertyName);
	}

	private boolean isPropertySupported(final String propertyName)
	{
		return !INTEGRATION_KEY_PROPERTY_NAME.equals(propertyName) && !LOCALIZED_ATTRIBUTE_NAME.equals(propertyName);
	}

	private String getItemTypeCode(final Object value)
	{
		return value instanceof HybrisEnumValue ?
				((HybrisEnumValue) value).getType() :
				((ItemModel) value).getItemtype();
	}

	protected boolean isPropertySupported(final PropertyMetadata propertyMetadata)
	{
		return isPropertySupported(propertyMetadata.getAttributeDescriptor());
	}

	protected boolean isPropertySupported(final TypeAttributeDescriptor descriptor)
	{
		return descriptor != null && isApplicable(descriptor);
	}

	protected abstract boolean isApplicable(final TypeAttributeDescriptor typeAttributeDescriptor);

	protected abstract void processItemInternal(final ItemModel item, final String entryPropertyName, final Object value,
			final StorageRequest request) throws EdmException;

	protected abstract void processEntityInternal(final ODataEntry oDataEntry, final String propertyName, final Object value,
			final ItemConversionRequest request) throws EdmException;

	protected ModelService getModelService()
	{
		return modelService;
	}

	@Required
	public void setModelService(final ModelService modelService)
	{
		this.modelService = modelService;
	}

	protected IntegrationObjectService getIntegrationObjectService()
	{
		return integrationObjectService;
	}

	@Required
	public void setIntegrationObjectService(final IntegrationObjectService integrationObjectService)
	{
		this.integrationObjectService = integrationObjectService;
	}

	public TypeService getTypeService()
	{
		return typeService;
	}

	@Required
	public void setTypeService(final TypeService typeService)
	{
		this.typeService = typeService;
	}

	public void setDescriptorFactory(final DescriptorFactory descriptorFactory)
	{
		this.descriptorFactory = descriptorFactory;
	}

	public static class PropertyMetadata	{
		private TypeAttributeDescriptor attributeDescriptor;
		private String propertyName;

		private PropertyMetadata()
		{
			// Can't instantiate using constructor
		}

		static PropertyMetadata create(final TypeAttributeDescriptor attributeDescriptor, final String propertyName)
		{
			return new PropertyMetadata()
					.setAttributeDescriptor(attributeDescriptor)
					.setPropertyName(propertyName);
		}

		TypeAttributeDescriptor getAttributeDescriptor()
		{
			return attributeDescriptor;
		}

		private PropertyMetadata setAttributeDescriptor(final TypeAttributeDescriptor attributeDescriptor)
		{
			this.attributeDescriptor = attributeDescriptor;
			return this;
		}

		String getPropertyName()
		{
			return propertyName;
		}

		private PropertyMetadata setPropertyName(final String propertyName)
		{
			this.propertyName = propertyName;
			return this;
		}
	}
}