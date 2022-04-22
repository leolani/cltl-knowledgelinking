from cltl.brain.basic_brain import BasicBrain
from cltl.brain.utils.helper_functions import read_query

from tempfile import TemporaryDirectory
from pathlib import Path


class EntitySearch(BasicBrain):

    def __init__(self, address, log_dir, clear_all=False):

        super(EntitySearch, self).__init__(address, log_dir, clear_all, is_submodule=True)

    def search_entities_by_label(self, ne_text, algorithm='popularity'):
        if algorithm == 'popularity':
            uri = self._get_most_popular_by_label(ne_text)
        elif algorithm == 'recency':
            uri = self._get_most_recent_by_label(ne_text)
        else:
            uri = None

        return uri

    def search_entities(self):
        entlist = self._get_most_popular()

        return entlist

    def _get_most_popular_by_label(self, ne_text):
        query = read_query('/Users/jaapkruijt/Documents/GitHub/cltl-knowledgelinking/src/cltl/entity_linking/popularity_label') % ne_text
        response = self._submit_query(query)
        # print(response)
        pop_ordered = []
        for row in response:
            uri = row['ent']['value']
            occurrences = row['num_mentions']['value']
            pop_ordered.append((uri, occurrences))
        if pop_ordered:
            uri, popularity = pop_ordered[0]
        else:
            uri = None
        return uri

    def _get_most_recent_by_label(self, ne_text):
        query = read_query('/Users/jaapkruijt/Documents/GitHub/cltl-knowledgelinking/src/cltl/entity_linking/recency_last_mention_label') % ne_text
        response = self._submit_query(query)
        if response:
            uri = response[0]['person']['value']
        else:
            uri = None

        return uri

    def _get_most_popular(self):
        query = read_query('/Users/jaapkruijt/Documents/GitHub/cltl-knowledgelinking/src/cltl/entity_linking/popularity')
        response = self._submit_query(query)

        pop_ordered = []
        for row in response:
            uri = row['ent']['value']
            occurrences = row['num_mentions']['value']
            label = row['l']['value']
            pop_ordered.append({'uri': uri, 'popularity': occurrences, 'label': label})

        return pop_ordered


if __name__ == "__main__":
    with TemporaryDirectory(prefix="brain-log") as log_path:
        entity_search = EntitySearch(address="http://localhost:7200/repositories/sandbox",
                                     log_dir=Path(log_path))

        # recent = entity_search.search_entities_by_label('selene', algorithm='recency')
        # popular = entity_search.search_entities_by_label('selene')
        #
        # print(recent)
        # print(popular)

        popularity = entity_search.search_entities()
        for dictionary in popularity:
            print(dictionary)
