import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import importlib_resources as pkg_resources
from cltl.brain.basic_brain import BasicBrain
import cltl.entity_linking

logger = logging.getLogger(__name__)


def read_query(query_filename):
    """
    Read a query from file and return as a string
    Parameters
    ----------
    query_filename: str name of the query. It will be looked for in the queries folder of this project

    Returns
    -------
    query: str the query with placeholders for the query parameters, as a string to be formatted

    """
    resources = pkg_resources.files(cltl.entity_linking)

    return (resources / f"{query_filename}.rq").read_text()


class EntitySearch(BasicBrain):

    def __init__(self, address, log_dir, clear_all=False):
        super(EntitySearch, self).__init__(address, log_dir, clear_all, is_submodule=True)

    def search_entities_by_name(self, ne_text, algorithm='popularity'):
        if algorithm == 'popularity':
            uri = self._get_most_popular_by_name(ne_text)
        elif algorithm == 'recency':
            uri = self._get_most_recent_by_name(ne_text)
        else:
            raise Exception("Unsupported algorithm: " + algorithm)

        return uri

    def search_entities_by_label(self, ne_text, algorithm='popularity'):
        if algorithm == 'popularity':
            uri = self._get_most_popular_by_label(ne_text)
        elif algorithm == 'recency':
            uri = self._get_most_recent_by_label(ne_text)
        else:
            raise Exception("Unsupported algorithm: " + algorithm)

        return uri

    def search_entities(self, algorithm='popularity'):
        if algorithm == 'popularity':
            entity_list = self._get_most_popular()
        elif algorithm == 'recency':
            entity_list = self._get_most_recent()
        else:
            raise Exception("Unsupported algorithm: " + algorithm)

        return entity_list

    def _get_most_popular_by_name(self, ne_text):
        query = read_query('./queries/popularity_name') % ne_text

        return self._get_most_popular_by_query(query)

    def _get_most_recent_by_name(self, ne_text):
        query = read_query('./queries/recency_last_mention_name') % ne_text

        return self._get_most_recent_by_query(query)

    def _get_most_popular_by_label(self, ne_text):
        query = read_query('./queries/popularity_label') % ne_text

        return self._get_most_popular_by_query(query)

    def _get_most_recent_by_label(self, ne_text):
        query = read_query('./queries/recency_last_mention_label') % ne_text

        return self._get_most_recent_by_query(query)

    def _get_most_popular_by_query(self, query):
        response = self._submit_query(query)
        logger.debug("Found popular entities by label %s", response)
        pop_ordered = []
        for row in response:
            if ('ent' not in row
                or not row['ent']['value']
                or 'num_mentions' not in row):
                continue
            uri = row['ent']['value']
            occurrences = row['num_mentions']['value']
            pop_ordered.append((uri, occurrences))
        if pop_ordered:
            uri, popularity = pop_ordered[0]
        else:
            uri = None
        return uri

    def _get_most_recent_by_query(self, query):
        response = self._submit_query(query)
        logger.debug("Found recent entities by label %s", response)
        if response:
            uri = response[0]['person']['value']
        else:
            uri = None

        return uri

    def _get_most_popular(self):
        query = read_query('./queries/popularity')
        response = self._submit_query(query)
        logger.debug("Found popular entities %s", response)

        pop_ordered = []
        for row in response:
            if ('ent' not in row
                or not row['ent']['value']
                or 'l' not in row
                or 'num_mentions' not in row):
                continue
            uri = row['ent']['value']
            occurrences = row['num_mentions']['value']
            label = row['l']['value']
            pop_ordered.append({'uri': uri, 'popularity': occurrences, 'label': label})

        return pop_ordered

    def _get_most_recent(self):
        query = read_query('./queries/recency_last_mention')
        response = self._submit_query(query)
        logger.debug("Found recent entities %s", response)

        rec_ordered = []
        for row in response:
            uri = row['person']['value']
            label = row['label']['value']
            date = f"{row['day']['value']}_{row['month']['value']}_{row['year']['value']}"
            rec_ordered.append({'uri': uri, 'label': label, 'date': date})

        return rec_ordered

    def search_entity_by_face(self, id):
        query = read_query('./queries/face') % id
        response = self._submit_query(query)
        if response:
            return response[0]['person']['value'], response[0]['name']['value']
        else:
            return None, None


if __name__ == "__main__":
    with TemporaryDirectory(prefix="brain-log") as log_path:
        entity_search = EntitySearch(address="http://localhost:7200/repositories/sandbox",
                                     log_dir=Path(log_path))

        recent = entity_search.search_entities_by_label('selene', algorithm='recency')
        popular = entity_search.search_entities_by_label('selene')
        # face = entity_search.search_entity_by_face("123")

        print(recent)
        print(popular)
        # print(face)

        recent = entity_search.search_entities('recency')
        for dictionary in recent:
            print(dictionary)
