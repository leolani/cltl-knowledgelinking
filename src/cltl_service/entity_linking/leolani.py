from cltl.brain.infrastructure.rdf_builder import RdfBuilder

from cltl.entity_linking.api import BasicLinker
from cltl.combot.event.emissor import ConversationalAgent
from emissor.representation.scenario import class_type


class AgentLinker(BasicLinker):
    def __init__(self):
        self._speaker = None

    def set_speaker(self, name, type, uri):
        self._speaker = {
            'label': name,
            'type': type,
            'uri': uri
        }

    def link(self, capsule):
        capsule = self.link_entities(capsule)

        return capsule

    def link_entities(self, capsule):
        for position in ['subject', 'object', 'author', 'item']:
            capsule = self._link_entity(capsule, position)

        return capsule

    def _link_entity(self, capsule, entity_position):
        if not self._speaker or entity_position not in capsule or capsule[entity_position]['uri']:
            return capsule

        if (class_type(ConversationalAgent) in capsule[entity_position]['type']
                and capsule[entity_position]['label'] == ConversationalAgent.SPEAKER.name):
            capsule[entity_position] |= self._speaker

        return capsule
