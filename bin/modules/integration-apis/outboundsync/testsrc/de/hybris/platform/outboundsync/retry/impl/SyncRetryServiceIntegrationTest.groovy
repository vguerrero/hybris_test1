/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.outboundsync.retry.impl

import de.hybris.bootstrap.annotations.IntegrationTest
import de.hybris.platform.apiregistryservices.model.BasicCredentialModel
import de.hybris.platform.apiregistryservices.model.ConsumedDestinationModel
import de.hybris.platform.apiregistryservices.model.DestinationTargetModel
import de.hybris.platform.apiregistryservices.model.EndpointModel
import de.hybris.platform.cronjob.model.CronJobModel
import de.hybris.platform.integrationservices.model.IntegrationObjectModel
import de.hybris.platform.integrationservices.util.IntegrationObjectTestUtil
import de.hybris.platform.integrationservices.util.IntegrationTestUtil
import de.hybris.platform.outboundservices.facade.OutboundServiceFacade
import de.hybris.platform.outboundsync.TestItemChangeDetector
import de.hybris.platform.outboundsync.TestOutboundFacade
import de.hybris.platform.outboundsync.TestOutboundItemConsumer
import de.hybris.platform.outboundsync.activator.OutboundItemConsumer
import de.hybris.platform.outboundsync.activator.impl.DefaultOutboundSyncService
import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel
import de.hybris.platform.outboundsync.model.OutboundSyncRetryModel
import de.hybris.platform.outboundsync.model.OutboundSyncStreamConfigurationModel
import de.hybris.platform.outboundsync.retry.SyncRetryService
import de.hybris.platform.outboundsync.util.OutboundSyncTestUtil
import de.hybris.platform.servicelayer.ServicelayerSpockSpecification
import de.hybris.platform.servicelayer.config.ConfigurationService
import de.hybris.platform.servicelayer.cronjob.CronJobService
import de.hybris.platform.servicelayer.search.FlexibleSearchService
import org.junit.Rule
import org.junit.Test

import javax.annotation.Resource

import static de.hybris.platform.integrationservices.util.IntegrationTestUtil.condition

@IntegrationTest
class SyncRetryServiceIntegrationTest extends ServicelayerSpockSpecification {
	@Resource
	CronJobService cronJobService
	@Resource
	DefaultOutboundSyncService outboundSyncService
	@Resource
	SyncRetryService syncRetryService
	@Resource(name = 'outboundServiceFacade')
	private OutboundServiceFacade outboundServiceFacade
	@Resource(name = 'outboundItemConsumer')
	private OutboundItemConsumer outboundItemConsumer
	@Resource
	private FlexibleSearchService flexibleSearchService
	@Rule
	TestOutboundFacade testOutboundFacade = TestOutboundFacade.respondWithNotFound()
	@Rule
	TestOutboundItemConsumer testOutboundItemConsumer = new TestOutboundItemConsumer()
	@Rule
	TestItemChangeDetector changeDetector = new TestItemChangeDetector()

	@Resource(name = "defaultConfigurationService")
	private ConfigurationService configurationService

	private final static String OBJECT_CODE = "OutboundCatalog"
	private final static String ITEM_CODE = "Catalog"
	private final static String CHANNEL_CODE = "outboundSyncRetryTestChannel"
	private static final CATALOG_ID = "catalogA"
	private CronJobModel cronJob
	def catalog

	def setup() {
		outboundSyncService.setOutboundServiceFacade(testOutboundFacade)
		outboundSyncService.setOutboundItemConsumer(testOutboundItemConsumer)

		importCsv '/impex/essentialdata-outboundsync.impex', 'UTF-8'

		def integrationObject = IntegrationObjectTestUtil.createIntegrationObject(OBJECT_CODE)
		IntegrationObjectTestUtil.createIntegrationObjectItem(integrationObject, ITEM_CODE)

		def channel = OutboundSyncTestUtil.outboundChannelConfigurationExists CHANNEL_CODE, OBJECT_CODE
		changeDetector.createChangeStream channel, ITEM_CODE

		cronJob = OutboundSyncTestUtil.outboundCronJob()
		catalog = IntegrationTestUtil.createCatalogWithId(CATALOG_ID)
	}

	def cleanup() {
		IntegrationTestUtil.removeAll OutboundChannelConfigurationModel
		IntegrationTestUtil.removeAll OutboundSyncStreamConfigurationModel
		IntegrationTestUtil.removeAll OutboundSyncRetryModel
		IntegrationTestUtil.removeAll IntegrationObjectModel

		IntegrationTestUtil.removeAll ConsumedDestinationModel
		IntegrationTestUtil.removeAll BasicCredentialModel
		IntegrationTestUtil.removeAll EndpointModel
		IntegrationTestUtil.removeAll DestinationTargetModel

		IntegrationTestUtil.remove cronJob
		IntegrationTestUtil.remove catalog

		outboundSyncService.setOutboundServiceFacade(outboundServiceFacade)
		outboundSyncService.setOutboundItemConsumer(outboundItemConsumer)
	}

	@Test
	def "changes are consumed when retry exceeds max retries"() {
		given:
		setMaxRetries("1")

		and:
		def condition = condition()

		when: "the job is executed first time"
		cronJobService.performCronJob(cronJob, true)

		then: "the failed change publication is not consumed (and a retry is created)"
		condition.eventually {
			assert testOutboundFacade.invocations() == 1
			assert testOutboundItemConsumer.invocations() == 0
			assert flexibleSearchService.getModelsByExample(triedOnce()).size() == 1
		}

		when: "the job is executed second time and the retry max has been reached"
		cronJobService.performCronJob(cronJob, true)

		then: "the failed change is consumed"
		condition.eventually {
			assert testOutboundFacade.invocations() == 2
			assert testOutboundItemConsumer.invocations() == 1
		}
	}

	@Test
	def "changes are consumed when max retries set to 0 and sync fails"() {
		given:
		setMaxRetries("0")

		when:
		cronJobService.performCronJob(cronJob, true)

		then:
		condition().eventually {
			assert testOutboundFacade.invocations() == 1
			assert testOutboundItemConsumer.invocations() == 1
		}
	}

	def setMaxRetries(final String maxRetries) {
		configurationService.getConfiguration().setProperty("outboundsync.max.retries", maxRetries)
	}

	def triedOnce() {
		def example = new OutboundSyncRetryModel()
		example.setSyncAttempts(1)
		example
	}
}
