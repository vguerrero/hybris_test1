/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company.  All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.persistence.polyglot.repository;

import de.hybris.platform.persistence.polyglot.model.ChangeSet;
import de.hybris.platform.persistence.polyglot.search.criteria.Criteria;
import de.hybris.platform.persistence.polyglot.search.FindResult;
import de.hybris.platform.persistence.polyglot.model.Identity;
import de.hybris.platform.persistence.polyglot.model.ItemState;
import de.hybris.platform.persistence.polyglot.ItemStateRepository;
import de.hybris.platform.persistence.polyglot.search.StandardFindResult;

import java.util.stream.Stream;


public class TestItemStateRepo implements ItemStateRepository
{

	@Override
	public ItemState get(final Identity id)
	{
		return null;
	}

	@Override
	public ChangeSet beginCreation(final Identity id)
	{
		return (key, value) -> {
		};
	}

	@Override
	public void store(final ChangeSet changeSet)
	{

	}

	@Override
	public void remove(final ItemState state)
	{

	}

	@Override
	public FindResult find(final Criteria criteria)
	{
		return StandardFindResult.buildFromStream(Stream.empty()).withCriteria(criteria).build();
	}
}
