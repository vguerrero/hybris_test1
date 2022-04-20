/*
 * [y] hybris Platform
 *
 * Copyright (c) 2019 SAP SE or an SAP affiliate company.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.outboundsync.activator.impl

import de.hybris.bootstrap.annotations.UnitTest
import de.hybris.platform.apiregistryservices.model.ConsumedDestinationModel
import de.hybris.platform.core.PK
import de.hybris.platform.core.model.ItemModel
import de.hybris.platform.integrationservices.model.IntegrationObjectDescriptor
import de.hybris.platform.outboundservices.facade.OutboundServiceFacade
import de.hybris.platform.outboundsync.activator.OutboundItemConsumer
import de.hybris.platform.outboundsync.dto.*
import de.hybris.platform.outboundsync.job.OutboundItemFactory
import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel
import de.hybris.platform.outboundsync.model.OutboundSyncRetryModel
import de.hybris.platform.outboundsync.retry.RetryUpdateException
import de.hybris.platform.outboundsync.retry.SyncRetryService
import de.hybris.platform.servicelayer.exceptions.ModelLoadingException
import de.hybris.platform.servicelayer.model.ModelService
import org.junit.Test
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import rx.Observable
import spock.lang.Specification
import spock.lang.Unroll

@UnitTest
class DefaultOutboundSyncServiceUnitTest extends Specification {

	private static final int DEFAULT_PK = 4
	private static final int ROOT_ITEM_PK = 123
	private static final String TEST_INTEGRATION_OBJECT = "TestIntegrationObject"
	private static final String TEST_DESTINATION = "TestDestination"

	def defaultOutboundSyncService = new DefaultOutboundSyncService()

	def outboundServiceFacade = Mock(OutboundServiceFacade)
	def outboundItemConsumer = Mock(OutboundItemConsumer)
	def modelService = Stub(ModelService)
	def syncRetryService = Mock(SyncRetryService)

	def setup() {
		defaultOutboundSyncService.setOutboundServiceFacade(outboundServiceFacade)
		defaultOutboundSyncService.setOutboundItemConsumer(outboundItemConsumer)
		defaultOutboundSyncService.setModelService(modelService)
		defaultOutboundSyncService.setSyncRetryService(syncRetryService)
		defaultOutboundSyncService.outboundItemFactory = Stub(OutboundItemFactory) {
			createItem(_ as OutboundItemDTO) >> Stub(OutboundItem) {
				getIntegrationObject() >> Stub(IntegrationObjectDescriptor) {
					getCode() >> TEST_INTEGRATION_OBJECT
				}
				getChannelConfiguration() >> Stub(OutboundChannelConfigurationModel) {
					getDestination() >> Stub(ConsumedDestinationModel) {
						getId() >> TEST_DESTINATION
					}
				}
			}
		}
	}

	@Test
	@Unroll
	def "#changeType item change is received, item is found and outbound synched successfully"() {
		given: 'synchronization is successful'
		def itemModel = Stub(ItemModel)
		def outboundItemDTO = outboundItemDTO(itemModel, changeType)
		outboundServiceFacade.send(itemModel, TEST_INTEGRATION_OBJECT, TEST_DESTINATION) >> stubObservableSuccess()

		when:
		defaultOutboundSyncService.sync([outboundItemDTO])

		then:
		1 * outboundItemConsumer.consume(outboundItemDTO)
		1 * syncRetryService.handleSyncSuccess(_ as OutboundItemDTOGroup)

		where:
		changeType << [OutboundChangeType.CREATED, OutboundChangeType.MODIFIED]
	}

	@Test
	@Unroll
	def "#changeType item change is received, item is found and outbound facade resulted in error"() {
		given: 'synchronization failed'
		def itemModel = Stub(ItemModel)
		def outboundItemDTO = outboundItemDTO(itemModel, changeType)
		outboundServiceFacade.send(itemModel, TEST_INTEGRATION_OBJECT, TEST_DESTINATION) >> stubObservableError()
		and: 'more synchronization attempts are possible in future'
		syncRetryService.handleSyncFailure(_ as OutboundItemDTOGroup) >> false

		when:
		defaultOutboundSyncService.sync([outboundItemDTO])

		then:
		0 * outboundItemConsumer.consume(_)

		where:
		changeType << [OutboundChangeType.CREATED, OutboundChangeType.MODIFIED]
	}

	@Test
	def "facade returns error and it is the last retry"() {
		given: 'synchronization failed'
		def itemModel = Stub(ItemModel)
		def outboundItemDTO = outboundItemDTO(itemModel, OutboundChangeType.CREATED)
		outboundServiceFacade.send(itemModel, TEST_INTEGRATION_OBJECT, TEST_DESTINATION) >> stubObservableError()
		and: 'it was last synchronization attempt possible'
		syncRetryService.handleSyncFailure(_ as OutboundItemDTOGroup) >> true

		when:
		defaultOutboundSyncService.sync([outboundItemDTO])

		then:
		1 * outboundItemConsumer.consume(outboundItemDTO)
	}

	@Test
	def "changes are not consumed when RetryUpdateException is thrown on last retry"() {
		given: 'synchronization failed'
		def itemModel = Stub(ItemModel)
		def outboundItemDTO = outboundItemDTO(itemModel, OutboundChangeType.CREATED)
		outboundServiceFacade.send(itemModel, TEST_INTEGRATION_OBJECT, TEST_DESTINATION) >> stubObservableError()
		and: 'an exception is thrown while handling the failure'
		syncRetryService.handleSyncFailure(_ as OutboundItemDTOGroup) >> { throw new RetryUpdateException(Stub(OutboundSyncRetryModel)) }

		when:
		defaultOutboundSyncService.sync([outboundItemDTO])

		then:
		0 * outboundItemConsumer.consume(_)
	}

	@Test
	def "change not consumed on success case when a RetryUpdateException occurs"() {
		given: 'synchronization is successful'
		def itemModel = Stub(ItemModel)
		def outboundItemDTO = outboundItemDTO(itemModel, OutboundChangeType.CREATED)
		outboundServiceFacade.send(itemModel, TEST_INTEGRATION_OBJECT, TEST_DESTINATION) >> stubObservableSuccess()
		and: 'an exception is thrown while handling the success'
		syncRetryService.handleSyncSuccess(_ as OutboundItemDTOGroup) >> { throw new RetryUpdateException(createTestRetry()) }

		when:
		defaultOutboundSyncService.sync([outboundItemDTO])

		then:
		0 * outboundItemConsumer.consume(_)
	}

	@Test
	def "CREATED item change is received, item is not found"() {
		given:
		def outboundItemDTO = Stub(OutboundItemDTO) {
			getItem() >> Stub(OutboundItemChange) {
				getChangeType() >> OutboundChangeType.CREATED
			}
			getRootItemPK() >> ROOT_ITEM_PK
		}
		and:
		modelService.get(PK.fromLong(ROOT_ITEM_PK)) >> { throw new ModelLoadingException("test message") }

		when:
		defaultOutboundSyncService.sync([outboundItemDTO])

		then:
		0 * outboundServiceFacade.send(_, _, _)
		0 * outboundItemConsumer.consume(_)
	}

	def stubObservableSuccess() {
		Observable.just Stub(ResponseEntity) {
			getStatusCode() >> HttpStatus.CREATED
		}
	}

	def stubObservableError() {
		Observable.just Stub(ResponseEntity) {
			getStatusCode() >> HttpStatus.INTERNAL_SERVER_ERROR
		}
	}

	def outboundItemDTO(itemModel, changeType) {
		modelService.get(PK.fromLong(ROOT_ITEM_PK)) >> itemModel
		Stub(OutboundItemDTO) {
			getItem() >> Stub(OutboundItemChange) {
				getPK() >> DEFAULT_PK
				getChangeType() >> changeType
			}
			getRootItemPK() >> ROOT_ITEM_PK
		}
	}
}

