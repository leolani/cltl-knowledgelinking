"""
Upon encountering NE do:
1) select NE token text as name
2) Query brain to find URI's with name (entities_with_label.rq)
3) if multiple URI's found:
    4) Query brain to count number of denotedBy links for each URI
    5) Rank URI's by number of links
    6) Select highest ranking URI
    7) Add URI to Annotation
3) if one URI found:
    4) Add URI to Annotation
3) if no URI's found:
    4) Add URI to brain
    5) Add URI to Annotation
"""

from cltl.brain.infrastructure.rdf_builder import RdfBuilder
from cltl.entity_linking.api import BasicLinker
from cltl.entity_linking.entity_querying import EntitySearch


class NamedEntityLinker(BasicLinker):

    def __init__(self, address, log_dir):

        super(NamedEntityLinker, self).__init__()
        self._rdf_builder = RdfBuilder()
        self._entity_search = EntitySearch(address, log_dir)

    def link(self, capsule):
        capsule = self.link_entities(capsule)
        capsule = self.link_predicates(capsule)

        return capsule

    def link_entities(self, capsule):
        if 'person' in capsule['subject']['type']:
            subject_label = capsule['subject']['label']
            uri = self._entity_search.search_entities_by_label(subject_label)
            if uri:
                capsule['subject']['uri'] = uri
            else:
                capsule['subject']['uri'] = str(
                    self._rdf_builder.create_resource_uri('LW', capsule['subject']['label'].lower()))
        else:
            capsule['subject']['uri'] = str(
                self._rdf_builder.create_resource_uri('LW', capsule['subject']['label'].lower()))

        if 'person' in capsule['object']['type']:
            object_label = capsule['object']['label']
            uri = self._entity_search.search_entities_by_label(object_label)
            if uri:
                capsule['object']['uri'] = uri
            else:
                capsule['object']['uri'] = str(
                    self._rdf_builder.create_resource_uri('LW', capsule['object']['label'].lower()))
        else:
            capsule['object']['uri'] = str(
                self._rdf_builder.create_resource_uri('LW', capsule['object']['label'].lower()))

        return capsule

    def link_predicates(self, capsule):
        capsule['predicate']['uri'] = str(
            self._rdf_builder.create_resource_uri('N2MU', capsule['predicate']['label'].lower()))

        return capsule


class PronounLinker(BasicLinker):

    def __init__(self, address, log_dir):

        super(PronounLinker, self).__init__()
        self._rdf_builder = RdfBuilder()
        self._entity_search = EntitySearch(address, log_dir)

    def link(self, capsule):
        capsule = self.link_entities(capsule)
        capsule = self.link_predicates(capsule)

        return capsule

    def link_entities(self, capsule):
        if capsule['subject']['type'] == ['person']:
            subject_label = capsule['subject']['label']
            uri = self._entity_search.search_entities_by_label(subject_label, algorithm='recency')
            if uri:
                capsule['subject']['uri'] = uri
            else:
                capsule['subject']['uri'] = str(
                    self._rdf_builder.create_resource_uri('LW', capsule['subject']['label'].lower()))
        else:
            capsule['subject']['uri'] = str(
                self._rdf_builder.create_resource_uri('LW', capsule['subject']['label'].lower()))

        if capsule['object']['type'] == ['person']:
            object_label = capsule['object']['label']
            uri = self._entity_search.search_entities_by_label(object_label, algorithm='recency')
            if uri:
                capsule['object']['uri'] = uri
            else:
                capsule['object']['uri'] = str(
                    self._rdf_builder.create_resource_uri('LW', capsule['object']['label'].lower()))
        else:
            capsule['object']['uri'] = str(
                self._rdf_builder.create_resource_uri('LW', capsule['object']['label'].lower()))

        return capsule

    def link_predicates(self, capsule):
        capsule['predicate']['uri'] = str(
            self._rdf_builder.create_resource_uri('N2MU', capsule['predicate']['label'].lower()))

        return capsule





