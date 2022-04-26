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

import jellyfish


class NamedEntityLinker(BasicLinker):
    """
    Used for disambiguation of Named Entities in text. Call when the part of speech tag of a label is 'NNP'
    Connection with GraphDB is needed to run the linker, since it queries the brain
    Input: capsule without url
    Output: capsule with url
    """

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
            uri = self._entity_search.search_entities_by_label(capsule['subject']['label'])
            if uri:
                capsule['subject']['uri'] = uri
            else:
                uri = self.fuzzy_label_match(capsule['subject']['label'])
            if uri:
                capsule['subject']['uri'] = uri
            else:
                capsule['subject']['uri'] = str(
                    self._rdf_builder.create_resource_uri('LW', capsule['subject']['label'].lower()))
        else:
            capsule['subject']['uri'] = str(
                self._rdf_builder.create_resource_uri('LW', capsule['subject']['label'].lower()))

        if 'person' in capsule['object']['type']:
            uri = self._entity_search.search_entities_by_label(capsule['object']['label'])
            if uri:
                capsule['object']['uri'] = uri
            else:
                uri = self.fuzzy_label_match(capsule['object']['label'])
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

    def fuzzy_label_match(self, label, algorithm='popularity'):
        entity_list = self._entity_search.search_entities(algorithm)
        smallest = 5
        match = 'placeholder'
        for entity in entity_list:
            levenshtein_distance = jellyfish.levenshtein_distance(label, entity['label'])
            if levenshtein_distance < smallest:
                smallest = levenshtein_distance
                found_label = entity['label']
                match = entity['uri']

        if match != 'placeholder':
            return match
        else:
            return None


class PronounLinker(BasicLinker):
    """
        Used for disambiguation of pronouns in text. Call when the part of speech tag of a label is 'PRP'
        Connection with GraphDB is needed to run the linker, since it queries the brain
        Input: capsule without url
        Output: capsule with url
        """

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
                entity_list = self._entity_search.search_entities(algorithm='recency')
                uri = entity_list[0]['uri']
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
                entity_list = self._entity_search.search_entities(algorithm='recency')
                uri = entity_list[0]['uri']
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





