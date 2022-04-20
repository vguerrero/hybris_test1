/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.maintenance;


import static org.fest.assertions.Assertions.assertThat;

import de.hybris.bootstrap.annotations.IntegrationTest;
import de.hybris.platform.core.model.media.MediaFolderModel;
import de.hybris.platform.cronjob.model.CronJobHistoryModel;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.cronjob.model.MoveMediaCronJobModel;
import de.hybris.platform.cronjob.model.MoveMediaJobModel;
import de.hybris.platform.impex.jalo.ImpExException;
import de.hybris.platform.servicelayer.ServicelayerTest;
import de.hybris.platform.servicelayer.cronjob.CronJobService;
import de.hybris.platform.servicelayer.model.ModelService;
import de.hybris.platform.servicelayer.search.FlexibleSearchQuery;
import de.hybris.platform.servicelayer.search.FlexibleSearchService;
import de.hybris.platform.servicelayer.search.SearchResult;

import javax.annotation.Resource;

import org.junit.Test;

@IntegrationTest
public class CronJobHistoryCleanupIntegrationTest extends ServicelayerTest
{
	@Resource
	private CronJobService cronJobService;
	@Resource
	private ModelService modelService;
	@Resource
	private FlexibleSearchService flexibleSearchService;

	@Test
	public void testPerform() throws ImpExException
	{
		prepareData();

		final String query = "SELECT {PK} FROM {CronJobHistory}";
		final FlexibleSearchQuery fQuery = new FlexibleSearchQuery(query);

		SearchResult<CronJobHistoryModel> searchResult = flexibleSearchService.search(fQuery);

		assertThat(searchResult).isNotNull();
		assertThat(searchResult.getResult()).hasSize(5);

		importCsv("/impex/essentialdata-cleanup-cronjobhistory.impex", "UTF-8");

		cronJobService.performCronJob(cronJobService.getCronJob("cronJobHistoryRetentionCronJob"), true);

		searchResult = flexibleSearchService.search(fQuery);

		assertThat(searchResult).isNotNull();
		assertThat(searchResult.getResult()).hasSize(3);
	}

	private void prepareData()
	{
		final CronJobModel cronJob1 = createCronJob("test1");
		final CronJobModel cronJob2 = createCronJob("test2");

		cronJobService.performCronJob(cronJob1, true);
		cronJobService.performCronJob(cronJob1, true);
		cronJobService.performCronJob(cronJob1, true);

		cronJobService.performCronJob(cronJob2, true);
		cronJobService.performCronJob(cronJob2, true);
	}

	private CronJobModel createCronJob(final String name)
	{
		final MoveMediaCronJobModel cronJob = modelService.create(MoveMediaCronJobModel.class);
		cronJob.setCode(name);

		final MediaFolderModel newFolder = modelService.create(MediaFolderModel.class);
		newFolder.setQualifier(name);
		cronJob.setTargetFolder(newFolder);
		modelService.save(newFolder);

		final MoveMediaJobModel job = new MoveMediaJobModel();
		job.setCode(name);
		cronJob.setJob(job);
		modelService.save(job);

		modelService.save(cronJob);
		return cronJob;
	}
}
