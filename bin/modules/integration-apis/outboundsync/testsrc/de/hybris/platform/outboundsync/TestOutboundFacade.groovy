/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.outboundsync

import de.hybris.platform.apiregistryservices.model.ConsumedDestinationModel
import de.hybris.platform.core.model.ItemModel
import de.hybris.platform.outboundservices.facade.OutboundServiceFacade
import org.junit.rules.ExternalResource
import org.springframework.http.ResponseEntity
import rx.Observable

class TestOutboundFacade extends ExternalResource implements OutboundServiceFacade {
	private static final def SOME_URI = new URI("test.uri")
	private static final ResponseEntity<Map> DEFAULT_RESPONSE = ResponseEntity.created(SOME_URI).build()

	final Collection<Invocation> invocations
	ResponseEntity<Map> responseEntity

	TestOutboundFacade() {
		responseEntity = DEFAULT_RESPONSE
		invocations = Collections.synchronizedList new ArrayList<Invocation>()
	}

	/**
	 * Creates new instance of this test facade, which will respond with HTTP 201 Created to all requests it receives.
	 * @return new test facade instance.
	 */
	static TestOutboundFacade respondWithCreated() {
		new TestOutboundFacade().respondWith ResponseEntity.created(SOME_URI)
	}

	/**
	 * Creates new instance of this test facade, which will respond with HTTP 404 Not Found to all requests it receives.
	 * @return new test facade instance.
	 */
	static TestOutboundFacade respondWithNotFound() {
		new TestOutboundFacade().respondWith ResponseEntity.notFound()
	}

	TestOutboundFacade respondWith(ResponseEntity.HeadersBuilder builder) {
		responseEntity = builder.build()
		return this
	}

	@Override
	Observable<ResponseEntity<Map>> send(final ItemModel itemModel, final String integrationObjectCode, final String destination)
	{
		invocations.add new Invocation(itemModel, integrationObjectCode, destination)
		Observable.just responseEntity
	}

	int invocations()
	{
		invocations.size()
	}

	@Override
	protected void after() {
		invocations.clear()
	}

	/**
	 * Retrieves items captured by this facade.
	 * @param dest consumed destination, to which the items should have been sent.
	 * @param ioCode code of IntegrationObject used for the items sent.
	 * @return a collection of items send to the specified destination with the specified IntegrationObject code or an empty
	 * collection, if no items were sent with the specified parameters.
	 */
	Collection<ItemModel> itemsFromInvocationsTo(ConsumedDestinationModel dest, String ioCode) {
		itemsFromInvocationsTo dest.id, ioCode
	}

	/**
	 * Retrieves items captured by this facade.
	 * @param dest specifies consumed destination ID, to which the items should have been sent.
	 * @param ioCode code of IntegrationObject used for the items sent.
	 * @return a collection of items send to the specified destination with the specified IntegrationObject code or an empty
	 * collection, if no items were sent with the specified parameters.
	 */
	Collection<ItemModel> itemsFromInvocationsTo(String dest, String ioCode) {
		invocations
				.findAll { it.matches(dest, ioCode) }
				.collect { it.itemModel }
	}

	private static class Invocation {
		private final String destination
		private final String integrationObject
		private final ItemModel item

		Invocation(ItemModel it, String ioCode, String dest) {
			destination = dest
			integrationObject = ioCode
			item = it
		}

		ItemModel getItemModel() {
			item
		}

		boolean matches(String dest, String ioCode) {
			destination == dest && integrationObject == ioCode
		}
	}
}
