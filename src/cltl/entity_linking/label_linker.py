from cltl.brain.infrastructure.rdf_builder import RdfBuilder

from cltl.entity_linking.api import BasicLinker


class LabelBasedLinker(BasicLinker):

    def __init__(self):
        """
        Create URI based on label Object

        Parameters
        ----------
        """

        super(BasicLinker, self).__init__()
        self._rdf_builder = RdfBuilder()

    def link(self, capsule):
        capsule = self.link_entities(capsule)
        capsule = self.link_predicates(capsule)

        return capsule

    def link_entities(self, capsule):
        capsule = self._link_entity(capsule, 'subject')
        capsule = self._link_entity(capsule, 'object')
        capsule = self._link_entity(capsule, 'author')

        return capsule

    def _link_entity(self, capsule, entity_position):
        if entity_position not in capsule:
            return capsule

        capsule[entity_position]['uri'] = str(self._rdf_builder.create_resource_uri('LW', capsule[entity_position]['label'].lower()))

        return capsule

    def link_predicates(self, capsule):
        capsule['predicate']['uri'] = str(
            self._rdf_builder.create_resource_uri('N2MU', capsule['predicate']['label'].lower()))

        return capsule
