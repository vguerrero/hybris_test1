/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package concerttours.jalo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import de.hybris.bootstrap.annotations.UnitTest;
import de.hybris.platform.testframework.HybrisJUnit4TransactionalTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;


/**
 * JUnit Tests for the Concerttours extension.
 */
@UnitTest
public class ConcerttoursTest extends HybrisJUnit4TransactionalTest
{
	/**
	 * Edit the local|project.properties to change logging behaviour (properties log4j2.logger.*).
	 */
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(ConcerttoursTest.class);

	@Before
	public void setUp()
	{
		// implement here code executed before each test
	}

	@After
	public void tearDown()
	{
		// implement here code executed after each test
	}

	/**
	 * This is a sample test method.
	 */
	@Test
	public void testConcerttours()
	{
		final boolean testTrue = true;
		assertThat(testTrue).isTrue();
	}

	@Test
	public void testExtensionCreatedOk() {
		assertTrue("New constants are not there",
				Files.exists(Path.of("../hybris/bin/custom/concerttours/src/concerttours/constants/ConcerttoursConstants.java")));
		assertTrue("New services are not there",
				Files.exists(Path.of("../hybris/bin/custom/concerttours/src/concerttours/service/ConcerttoursService.java")));
		assertTrue("New default services are not there",
				Files.exists(Path.of("../hybris/bin/custom/concerttours/src/concerttours/service/impl/DefaultConcerttoursService.java")));
		assertTrue("New setup is not there",
				Files.exists(Path.of("../hybris/bin/custom/concerttours/src/concerttours/setup/ConcerttoursSystemSetup.java")));
		assertTrue("New standalone is not there",
				Files.exists(Path.of("../hybris/bin/custom/concerttours/src/concerttours/ConcerttoursStandalone.java")));
	}
}
