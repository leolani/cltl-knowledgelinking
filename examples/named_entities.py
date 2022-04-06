from cltl.entity_linking.linkers import  NamedEntityLinker
from tempfile import TemporaryDirectory
from pathlib import Path

from cltl.combot.backend.api.discrete import UtteranceType
from datetime import date
from random import getrandbits

context_id, place_id = getrandbits(8), getrandbits(8)

test_capsule = {
        "chat": 1,
        "turn": 2,
        "author": "piek",
        "utterance": "Bram is from Spain",
        "utterance_type": UtteranceType.STATEMENT,
        "position": "0-25",
        "subject": {
            "label": "bram",
            "type": ["object"]
        },
        "predicate": {
            "label": "be-from"
        },
        "object": {
            "label": "spain",
            "type": ["location"]
        },
        "perspective": {
            "certainty": 1,
            "polarity": 1,
            "sentiment": 0
        },
        "context_id": context_id,
        "date": date(2017, 10, 24),
        "place": "Piek's office",
        "place_id": place_id,
        "country": "Netherlands",
        "region": "North Holland",
        "city": "Amsterdam",
        "objects": [],
        "people": []
    }

if __name__ == "__main__":
    with TemporaryDirectory(prefix="brain-log") as log_path:
        linker = NamedEntityLinker(address="http://localhost:7200/repositories/sandbox",
                                   log_dir=Path(log_path))

        capsule = linker.link(test_capsule)
        print(capsule)



