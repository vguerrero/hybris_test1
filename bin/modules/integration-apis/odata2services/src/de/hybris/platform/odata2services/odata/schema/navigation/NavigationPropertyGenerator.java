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
package de.hybris.platform.odata2services.odata.schema.navigation;

import de.hybris.platform.integrationservices.model.IntegrationObjectItemAttributeModel;
import de.hybris.platform.integrationservices.model.TypeAttributeDescriptor;
import de.hybris.platform.integrationservices.model.impl.DefaultTypeAttributeDescriptor;
import de.hybris.platform.odata2services.odata.NestedAbstractItemTypeCannotBeCreatedException;
import de.hybris.platform.odata2services.odata.UniqueCollectionNotAllowedException;
import de.hybris.platform.odata2services.odata.schema.SchemaElementGenerator;
import de.hybris.platform.odata2services.odata.schema.association.AssociationGenerator;
import de.hybris.platform.odata2services.odata.schema.association.AssociationGeneratorRegistry;
import de.hybris.platform.odata2services.odata.schema.utils.SchemaUtils;

import java.util.List;
import java.util.Optional;

import org.apache.olingo.odata2.api.edm.FullQualifiedName;
import org.apache.olingo.odata2.api.edm.provider.AnnotationAttribute;
import org.apache.olingo.odata2.api.edm.provider.NavigationProperty;
import org.springframework.beans.factory.annotation.Required;

import com.google.common.base.Preconditions;

public class NavigationPropertyGenerator implements SchemaElementGenerator<Optional<NavigationProperty>, IntegrationObjectItemAttributeModel>
{
	private static final String IS_UNIQUE = "s:IsUnique";
	private AssociationGeneratorRegistry associationGeneratorRegistry;
	private SchemaElementGenerator<List<AnnotationAttribute>, IntegrationObjectItemAttributeModel> attributeListGenerator;

	@Override
	public Optional<NavigationProperty> generate(final IntegrationObjectItemAttributeModel attribute)
	{
		Preconditions.checkArgument(attribute != null,
				"A NavigationProperty cannot be generated from a null IntegrationObjectItemAttributeModel.");

		final Optional<AssociationGenerator> associationGeneratorOptional = associationGeneratorRegistry.getAssociationGenerator(attribute);
		if (associationGeneratorOptional.isPresent())
		{
			final AssociationGenerator associationGenerator = associationGeneratorOptional.get();
			final String attrName = attribute.getAttributeName();
			final NavigationProperty navigationProperty = new NavigationProperty()
					.setName(attrName)
					.setRelationship(new FullQualifiedName(SchemaUtils.NAMESPACE,
							associationGenerator.getAssociationName(attribute)))
					.setFromRole(associationGenerator.getSourceRole(attribute))
					.setToRole(associationGenerator.getTargetRole(attribute))
					.setAnnotationAttributes(attributeListGenerator.generate(attribute));

			final TypeAttributeDescriptor attributeDescriptor = asDescriptor(attribute);
			validateNavigationProperty(attributeDescriptor, navigationProperty);
			return Optional.of(navigationProperty);
		}
		return Optional.empty();
	}

	private void validateNavigationProperty(final TypeAttributeDescriptor attributeDescriptor, final NavigationProperty navigationProperty)
	{
		if(attributeDescriptor.isAutoCreate() && attributeDescriptor.getAttributeType().isAbstract())
		{
			throw new NestedAbstractItemTypeCannotBeCreatedException(attributeDescriptor);
		}

		final boolean isUnique = navigationProperty.getAnnotationAttributes()
				.stream()
				.anyMatch(aa -> IS_UNIQUE.equals(aa.getName()) && "true".equals(aa.getText()));

		if (isUnique && isCollection(attributeDescriptor))
		{
			throw new UniqueCollectionNotAllowedException(attributeDescriptor);
		}
	}

	private boolean isCollection(final TypeAttributeDescriptor descriptor)
	{                                                                       
		return descriptor.isCollection() || descriptor.isMap();
	}

	private static TypeAttributeDescriptor asDescriptor(final IntegrationObjectItemAttributeModel attributeModel)
	{
		return DefaultTypeAttributeDescriptor.create(attributeModel);
	}

	@Required
	public void setAttributeListGenerator(final SchemaElementGenerator<List<AnnotationAttribute>, IntegrationObjectItemAttributeModel> generator)
	{
		attributeListGenerator = generator;
	}

	@Required
	public void setAssociationGeneratorRegistry(final AssociationGeneratorRegistry associationGeneratorRegistry)
	{
		this.associationGeneratorRegistry = associationGeneratorRegistry;
	}
}
