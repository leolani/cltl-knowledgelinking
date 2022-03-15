from cltl.brain.utils.base_cases import chat_1

from cltl.entity_linking.label_linker import LabelBasedLinker

linker = LabelBasedLinker()

capsules = [linker.link(capsule) for capsule in chat_1]

print(capsules)
