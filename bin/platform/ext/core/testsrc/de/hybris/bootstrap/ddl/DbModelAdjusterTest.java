/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.bootstrap.ddl;

import de.hybris.bootstrap.annotations.UnitTest;
import de.hybris.bootstrap.ddl.DbModelAdjuster.IndexEquivalence;
import de.hybris.bootstrap.ddl.model.YDbTableProvider;
import de.hybris.platform.testframework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


/**
 * Tests for {@link DbModelAdjuster} and contained inner classes.
 */
@UnitTest
public class DbModelAdjusterTest
{
	@Mock
	private YDbTableProvider tableProvider;

	@Before
	public void setUp()
	{
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void allInstancesOfIndexEquivalenceShouldAlwaysBeEqual()
	{
		// DbModelAdjuster instance required for inner class
		final DbModelAdjuster adjuster = new DbModelAdjuster(tableProvider);

		final IndexEquivalence equivalence1 = adjuster.new IndexEquivalence();
		final IndexEquivalence equivalence2 = adjuster.new IndexEquivalence();

		Assert.assertEquals(equivalence1, equivalence2);
	}
}
