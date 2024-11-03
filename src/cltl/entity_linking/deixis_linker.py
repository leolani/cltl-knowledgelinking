from cltl.brain.infrastructure.rdf_builder import RdfBuilder

from cltl.entity_linking.api import BasicLinker


class DeixisLinker(BasicLinker):

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

    def link_deixis(self, capsule):
        capsule = self._link_deixis(capsule, 'subject')
        capsule = self._link_deixis(capsule, 'object')
        capsule = self._link_deixis(capsule, 'author')
        capsule = self._link_deixis(capsule, 'item')

        return capsule

    def _link_deixis(self, capsule, entity_position):
        if entity_position not in capsule or capsule[entity_position]['uri']:
            return capsule

        capsule[entity_position]['uri'] = str(
            self._rdf_builder.create_resource_uri('LW', capsule[entity_position]['label'].lower()))

        return capsule

    def link_predicates(self, capsule):
        if 'predicate' in capsule and not capsule['predicate']['uri'] and bool(capsule['predicate']['label']):
            capsule['predicate']['uri'] = str(
                self._rdf_builder.create_resource_uri('N2MU', capsule['predicate']['label'].lower()))

        return capsule

# Get ids of existing objects in this location

# The brain has a capability to assign object ids by maximising object permanence.
# This means that if during one episode at location A, there were 5 chairs identified with ids 1-5,
# then the next episode at location A, when 3 chairs are detected the new detestions will be assigned ids 1-3,
# therefore assuming the same chairs stayed in the location.
#
# This functionality is currently part of the brain processing, but we now want it to be part of the linking/disambiguation module.
# Therefore, it should be something like this:

# you can indeed listen to multiple topics, an example would be the MentionExtractionService.
# You can distinguish the events either from the topic which is available in the event metadata or the payload type.
# You may also consider whether you actually need e.g. the image signal itself, or just e.g. the object annotation, then it is enough to listen to that topic.
# Some technicalities:
# In the TopicWorker the events are received from the event bus and put on a queue,
# so in the process method you will get them in the order they were received by the TopicWorker, one after another.
# The max length of that queue is configurable as well as the strategy to apply when it’s full,
# default is length 1 and events are dropped from the queue, so you might want  to make the queue a bit bigger to avoid loosing events.
# In the process method of the service you don’t need worry about concurrency, so you can just store information
# from different events in fields in the service. However, in principle you can’t make any assumptions on the order the events are delivered by the event bus,
# i.e. in theory e.g. a text signal event could arrive after an event with an annotation on that signal.
# However, it might be easiest and good enough to just accept that as a possible source of error.
# If you e.g. want to have the correct order of objects you would need the ImageSignals and check the actual timestamp of the image,
# but it is currently much harder than to just use the order of the object annotation events (and can lead to loss of instead of the incorrect order).
# I think it would be good to be able to query the EmissorDataService for the signals etc., but currently only querying the current scenario id is implemented.



