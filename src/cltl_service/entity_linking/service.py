import logging
from typing import List

from cltl.combot.event.emissor import TextSignalEvent
from cltl.combot.infra.config import ConfigurationManager
from cltl.combot.infra.event import Event, EventBus
from cltl.combot.infra.resource import ResourceManager
from cltl.combot.infra.time_util import timestamp_now
from cltl.combot.infra.topic_worker import TopicWorker
from cltl_service.emissordata.client import EmissorDataClient
from emissor.representation.scenario import TextSignal

from cltl.entity_linking.api import BasicLinker

logger = logging.getLogger(__name__)

CONTENT_TYPE_SEPARATOR = ';'


class DisambiguationService:
    @classmethod
    def from_config(cls, linkers: List[BasicLinker], emissor_data: EmissorDataClient, event_bus: EventBus,
                    resource_manager: ResourceManager,
                    config_manager: ConfigurationManager):
        config = config_manager.get_config("cltl.entity_linking")

        return cls(config.get("topic_input"), config.get("topic_output"), linkers, emissor_data, event_bus,
                   resource_manager)

    def __init__(self, input_topic: str, output_topic: str, linkers: List[BasicLinker], emissor_data: EmissorDataClient,
                 event_bus: EventBus, resource_manager: ResourceManager):
        self._linkers = linkers

        self._emissor_data = emissor_data
        self._event_bus = event_bus
        self._resource_manager = resource_manager

        self._input_topic = input_topic
        self._output_topic = output_topic

        self._topic_worker = None

    @property
    def app(self):
        return None

    def start(self, timeout=30):
        self._topic_worker = TopicWorker([self._input_topic], self._event_bus, provides=[self._output_topic],
                                         resource_manager=self._resource_manager, processor=self._process,
                                         name=self.__class__.__name__)
        self._topic_worker.start().wait()

    def stop(self):
        if not self._topic_worker:
            pass

        self._topic_worker.stop()
        self._topic_worker.await_stop()
        self._topic_worker = None

    def _process(self, event: Event[List[dict]]):
        response = []
        for capsule in event.payload:
            logger.debug("Capsule: (%s)", capsule)
            try:
                for linker in self._linkers:
                    updated_capsule = linker.link(capsule)
                    if updated_capsule:
                        break

                response.append(updated_capsule)
            except:
                logger.exception("Replier error")

        if response:
            extractor_event = self._create_payload(response)
            self._event_bus.publish(self._output_topic, Event.for_payload(extractor_event))

    def _create_payload(self, response):
        scenario_id = self._emissor_data.get_current_scenario_id()
        signal = TextSignal.for_scenario(scenario_id, timestamp_now(), timestamp_now(), None, response)

        return TextSignalEvent.create(signal)
