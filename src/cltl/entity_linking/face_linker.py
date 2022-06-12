from cltl.brain.infrastructure.rdf_builder import RdfBuilder

from cltl.entity_linking.api import BasicLinker
from cltl.entity_linking.entity_querying import EntitySearch


class FaceIDLinker(BasicLinker):
    def __init__(self, address, log_dir):
        super().__init__()
        self._rdf_builder = RdfBuilder()
        self._entity_search = EntitySearch(address, log_dir)

    def link(self, capsule):
        capsule = self._link_entity(capsule, 'subject')
        capsule = self._link_entity(capsule, 'object')
        capsule = self._link_entity(capsule, 'author')
        capsule = self._link_entity(capsule, 'item')

        return capsule

    def _link_entity(self, capsule, entity_position):
        if (entity_position not in capsule
                or 'face' not in capsule[entity_position]['type']):
            return capsule

        face_uri = self._create_uri(capsule[entity_position]['label'])
        person_uri, name = self._entity_search.search_entity_by_face(face_uri)

        if person_uri:
            capsule[entity_position]['label'] = name
            capsule[entity_position]['type'] = ["person"]
            capsule[entity_position]['uri'] = str(person_uri)

        return capsule

    def _create_uri(self, label):
        return str(self._rdf_builder.create_resource_uri('LW', label.lower()))
