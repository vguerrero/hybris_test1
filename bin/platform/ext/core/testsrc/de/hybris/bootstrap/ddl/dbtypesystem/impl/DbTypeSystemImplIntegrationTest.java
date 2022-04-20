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
package de.hybris.bootstrap.ddl.dbtypesystem.impl;

import de.hybris.bootstrap.annotations.IntegrationTest;
import de.hybris.bootstrap.codegenerator.CodeGenerator;
import de.hybris.bootstrap.config.ConfigUtil;
import de.hybris.bootstrap.config.PlatformConfig;
import de.hybris.bootstrap.ddl.DBCPDataSourceCreator;
import de.hybris.bootstrap.ddl.DataSourceCreator;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisDatabaseSettingsFactory;
import de.hybris.bootstrap.ddl.PropertiesLoader;
import de.hybris.bootstrap.ddl.StandalonePropertiesLoader;
import de.hybris.bootstrap.ddl.dbtypesystem.DbTypeSystem;
import de.hybris.bootstrap.typesystem.PatchedYTypeSystemLoader;
import de.hybris.bootstrap.typesystem.YTypeSystem;
import de.hybris.bootstrap.typesystem.YTypeSystemLoader;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.fest.assertions.Assertions;
import org.junit.Test;


/**
 * Sanity test checks whether the {@link DbTypeSystemFactory} correctly connects to database.
 */
@IntegrationTest
public class DbTypeSystemImplIntegrationTest
{
	private static final Logger LOG = Logger.getLogger(DbTypeSystemImplIntegrationTest.class);

	@Test
	public void assertLoadFromDB()
	{


		final DataSourceCreator dataSourceCreator = new DBCPDataSourceCreator();
		final PropertiesLoader propertiesLoader = new StandalonePropertiesLoader(
				ConfigUtil.getPlatformConfig(DbTypeSystemImplIntegrationTest.class), "junit");
		final PlatformConfig platformConfig = ConfigUtil.getPlatformConfig(DbTypeSystemImplIntegrationTest.class);

		final DatabaseSettings dbSettings = new HybrisDatabaseSettingsFactory(propertiesLoader).createDatabaseSettings();
		final DataSource dataSource = dataSourceCreator.createDataSource(dbSettings);

		final DbTypeSystem dbTypeSystem = new DbTypeSystemFactory().createDbTypeSystem(dataSource, dbSettings.getTablePrefix(),
				dbSettings.getTypeSystemName());

		final YTypeSystem system = new CodeGenerator(platformConfig, new PatchedYTypeSystemLoader(new YTypeSystemLoader(true),
				dbTypeSystem)).getTypeSystem();

		Assertions.assertThat(system.getComposedTypes()).isNotEmpty();
		Assertions.assertThat(system.getAtomicTypes()).isNotEmpty();
		Assertions.assertThat(system.getAttributes()).isNotEmpty();
		Assertions.assertThat(system.getMapTypes()).isNotEmpty();
		Assertions.assertThat(system.getCollectionTypes()).isNotEmpty();
		Assertions.assertThat(system.getDeployments()).isNotEmpty();

	}

}
