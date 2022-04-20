/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.cronjob.jalo;

import static org.assertj.core.api.Assertions.assertThat;

import de.hybris.bootstrap.annotations.IntegrationTest;
import de.hybris.platform.core.PK;
import de.hybris.platform.core.Registry;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.ServicelayerBaseTest;
import de.hybris.platform.servicelayer.event.EventService;
import de.hybris.platform.servicelayer.event.events.AbstractCronJobEvent;
import de.hybris.platform.servicelayer.event.events.AfterCronJobCrashAbortEvent;
import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import de.hybris.platform.servicelayer.internal.model.ScriptingJobModel;
import de.hybris.platform.servicelayer.model.ModelService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@IntegrationTest
public class AbortingCronJobsIntegrationTest extends ServicelayerBaseTest
{

	private CronJobManager testCronJobManager;
	private int currentNodeId;

	@Resource
	private ModelService modelService;

	@Resource
	private EventService eventService;


	private final List<AfterCronJobCrashAbortEvent> events = new ArrayList<>();

	private final AbstractEventListener<AfterCronJobCrashAbortEvent> listener = new AbstractEventListener<>()
	{
		@Override
		protected void onEvent(final AfterCronJobCrashAbortEvent event)
		{
			events.add(event);
		}
	};


	@Before
	public void setUp() throws Exception
	{
		CronJobManager.getInstance().stopConjobEngine();

		testCronJobManager = new CronJobManager();
		currentNodeId = Registry.getClusterID();
	}

	@After
	public void tearDown() throws Exception
	{
		eventService.unregisterEventListener(listener);
		events.clear();

		CronJobManager.getInstance().startupCronjobEngine();
	}


	@Test
	public void shouldAbortCronJobOnRunningOnGivenNode()
	{
		final PK pk = createRunningCronJob(currentNodeId + 1).getPk();

		testCronJobManager.abortRunningCronJobsForClusterNodes(Set.of(currentNodeId + 1), 100);

		assertCronJobAborted(pk);
	}

	@Test
	public void shouldAbortCronJobOnRunningRestartedOnGivenNode()
	{
		final PK pk = createRunningRestartCronJob(currentNodeId + 1).getPk();

		testCronJobManager.abortRunningCronJobsForClusterNodes(Set.of(currentNodeId + 1), 100);

		assertCronJobAborted(pk);
	}

	@Test
	public void shouldNotAbortCronJobsInStatusesOtherThanRunning()
	{
		final CronJobModel cjAborted = createNotRunningCronJobWithStatus(CronJobStatus.ABORTED);
		final CronJobModel cjFinished = createNotRunningCronJobWithStatus(CronJobStatus.FINISHED);
		final CronJobModel cjPaused = createNotRunningCronJobWithStatus(CronJobStatus.PAUSED);
		final CronJobModel cjUnknown = createNotRunningCronJobWithStatus(CronJobStatus.UNKNOWN);

		final Map<PK, Long> versions = Stream.of(cjAborted, cjFinished, cjPaused, cjUnknown)
		                                     .peek(i -> modelService.detach(i))
		                                     .collect(Collectors.toMap(CronJobModel::getPk,
				                                     i -> i.getItemModelContext().getPersistenceVersion()));


		testCronJobManager.abortRunningCronJobsForClusterNodes(Set.of(currentNodeId), 100);

		assertCronJobsNotChanged(versions);
	}

	@Test
	public void shouldPublishAbortAfterCrashEvent()
	{

		eventService.registerEventListener(listener);

		final PK pk = createRunningCronJob(currentNodeId + 1).getPk();

		testCronJobManager.abortRunningCronJobsForClusterNodes(Set.of(currentNodeId + 1), 100);

		assertCronJobAborted(pk);

		assertThat(events).hasSize(1).extracting(AbstractCronJobEvent::getCronJobPK).contains(pk);


	}

	private void assertCronJobsNotChanged(final Map<PK, Long> cjAborted)
	{
		for (final Map.Entry<PK, Long> cronJob : cjAborted.entrySet())
		{
			assertCronJobNotChanged(cronJob.getKey(), cronJob.getValue());
		}
	}

	private void assertCronJobAborted(final PK pk)
	{
		final CronJobModel item = modelService.get(pk);

		assertThat(item).isNotNull();
		assertThat(item.getStatus()).isEqualTo(CronJobStatus.ABORTED);
	}

	private void assertCronJobNotChanged(final PK pk, final long version)
	{
		final CronJobModel item = modelService.get(pk);

		assertThat(item).isNotNull();
		assertThat(item.getItemModelContext().getPersistenceVersion()).isEqualTo(version);
	}


	private CronJobModel createCronJob()
	{

		final ScriptingJobModel job = modelService.create(ScriptingJobModel.class);
		job.setCode(UUID.randomUUID().toString());
		job.setScriptURI("media://someScript");

		final CronJobModel cronJob = modelService.create(CronJobModel.class);
		cronJob.setCode(UUID.randomUUID().toString());
		cronJob.setJob(job);

		modelService.saveAll(job, cronJob);

		return cronJob;
	}

	private CronJobModel createRunningCronJob(final int runningOnClusterNodeId)
	{
		final CronJobModel cronJob = createCronJob();

		cronJob.setStatus(CronJobStatus.RUNNING);
		cronJob.setRunningOnClusterNode(runningOnClusterNodeId);

		modelService.save(cronJob);

		return cronJob;
	}

	private CronJobModel createRunningRestartCronJob(final int runningOnClusterNodeId)
	{
		final CronJobModel cronJob = createCronJob();

		cronJob.setStatus(CronJobStatus.RUNNINGRESTART);
		cronJob.setRunningOnClusterNode(runningOnClusterNodeId);

		modelService.save(cronJob);

		return cronJob;
	}

	private CronJobModel createNotRunningCronJobWithStatus(final CronJobStatus status)
	{
		final CronJobModel cronJob = createCronJob();

		cronJob.setStatus(status);

		modelService.save(cronJob);
		return cronJob;
	}
}
