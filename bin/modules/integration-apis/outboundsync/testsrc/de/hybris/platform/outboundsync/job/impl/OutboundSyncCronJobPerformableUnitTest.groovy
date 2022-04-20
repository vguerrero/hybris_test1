/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.outboundsync.job.impl

import de.hybris.bootstrap.annotations.UnitTest
import de.hybris.deltadetection.ChangeDetectionService
import de.hybris.deltadetection.ItemChangeDTO
import de.hybris.platform.core.PK
import de.hybris.platform.core.model.type.ComposedTypeModel
import de.hybris.platform.cronjob.enums.CronJobResult
import de.hybris.platform.cronjob.enums.CronJobStatus
import de.hybris.platform.integrationservices.model.IntegrationObjectModel
import de.hybris.platform.outboundsync.dto.OutboundItemDTO
import de.hybris.platform.outboundsync.job.GettableChangesCollector
import de.hybris.platform.outboundsync.job.ItemChangeSender
import de.hybris.platform.outboundsync.model.*
import org.junit.Test
import spock.lang.Specification
import spock.lang.Unroll

@UnitTest
class OutboundSyncCronJobPerformableUnitTest extends Specification {
	def changeDetectionService = Stub(ChangeDetectionService)
	def itemChangeSender = Mock(ItemChangeSender)
	def changesCollectorProvider = Stub(GettableChangesCollectorProvider)

	def cronJobPerformable = new OutboundSyncCronJobPerformable()

	def setup() {
		cronJobPerformable.setChangeDetectionService(changeDetectionService)
		cronJobPerformable.setGettableChangesCollectorProvider(changesCollectorProvider)
		cronJobPerformable.setItemChangeSender(itemChangeSender)
	}

	@Test
	def "perform results in error when stream configuration container is null"() {
		given:
		def cronJob = Stub(OutboundSyncCronJobModel) {
			getJob() >> Stub(OutboundSyncJobModel) {
				getStreamConfigurationContainer() >> null
			}
		}

		when:
		def result = cronJobPerformable.perform(cronJob)

		then:
		CronJobResult.ERROR == result.getResult()
		CronJobStatus.FINISHED == result.getStatus()
	}

	@Test
	def "perform results in failure when stream configuration collectChangesFromConfigurations throws RuntimeException"() {
		given:
		def cronJob = defaultCronJob([productStream: "Product"])

		changeDetectionService.collectChangesForType(_, _, _) >>   { throw new RuntimeException() }

		when:
		def result = cronJobPerformable.perform(cronJob)

		then:
		CronJobResult.FAILURE == result.getResult()
		CronJobStatus.FINISHED == result.getStatus()
	}

	@Test
	@Unroll
	def "perform results in success with #streamIdTypeCodeMap and changes #changes"() {
		given:
		def cronJob = defaultCronJob(streamIdTypeCodeMap)

		changesCollectorProvider.getCollector() >> Mock(GettableChangesCollector) {
			getChanges() >> changes
		}

		when:
		def result = cronJobPerformable.perform(cronJob)

		then:
		CronJobResult.SUCCESS == result.getResult()
		CronJobStatus.FINISHED == result.getStatus()
		itemSendInvocations * itemChangeSender.send(_ as OutboundItemDTO)
		changeDetectionService.collectChangesForType(_, _, _) >> { args -> assert args[1].getItemSelector() == "whereClause" }

		where:
		streamIdTypeCodeMap                                    | changes               | itemSendInvocations
		[productStream: "Product"]                             | [Stub(ItemChangeDTO)] | 1
		[productStream: "Product"]                             | []                    | 0
		[productStream: "Product", categoryStream: "Category"] | [Stub(ItemChangeDTO)] | 2
		[productStream: "Product", productStream2: "Product"]  | [Stub(ItemChangeDTO)] | 2
		[:]                                                    | [Stub(ItemChangeDTO)] | 0
	}

	def defaultCronJob(Map streamIdTypeCodeMap) {
		Stub(OutboundSyncCronJobModel) {
			getJob() >> Stub(OutboundSyncJobModel) {
				getStreamConfigurationContainer() >> Stub(OutboundSyncStreamConfigurationContainerModel) {
					getConfigurations() >> streamConfigurations(streamIdTypeCodeMap)
					getId() >> "outboundSyncDataStreams"
				}
			}
		}
	}

	def streamConfigurations(Map streamIdTypeCodeMap) {
		streamIdTypeCodeMap.collect {
			streamId, typeCode -> streamConfiguration(streamId, typeCode)
		}
	}

	def streamConfiguration(def streamId, def typeCode) {
		Stub(OutboundSyncStreamConfigurationModel) {
			getStreamId() >> streamId
			getItemTypeForStream() >> Stub(ComposedTypeModel) {
				getCode() >> typeCode
			}
			getOutboundChannelConfiguration() >> Stub(OutboundChannelConfigurationModel) {
				getIntegrationObject() >> Stub(IntegrationObjectModel) {
					getPk() >> PK.fromLong(1)
				}

				getPk() >> PK.fromLong(2)

			}
			getWhereClause() >> "whereClause"
		}
	}
}
