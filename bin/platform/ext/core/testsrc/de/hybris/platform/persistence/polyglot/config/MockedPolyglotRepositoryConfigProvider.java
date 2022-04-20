/*
 * [y] hybris Platform
 *
 * Copyright (c) 2019 SAP SE or an SAP affiliate company.  All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.persistence.polyglot.config;

import java.util.List;

public class MockedPolyglotRepositoryConfigProvider implements PolyglotRepositoriesConfigProvider
{
	private final List<RepositoryConfig> configs;

	public MockedPolyglotRepositoryConfigProvider(final List<RepositoryConfig> configs)
	{
		this.configs = configs;
	}

	@Override
	public List<RepositoryConfig> getConfigs()
	{
		return configs;
	}
}
