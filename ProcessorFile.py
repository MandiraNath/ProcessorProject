import json
from typing import List


class ProcessorFile:
    # max record size is 1 MB
    MAX_RECORD_SIZE = 1024 * 1024
    # max batch size is 5 MB
    MAX_BATCH_SIZE = 5 * 1024 * 1024
    MAX_RECORD_PER_BATCH = 500

    @staticmethod
    def split_into_batches(records: List[str]) -> List[List[str]]:
        batches = []
        current_batch = []
        current_batch_size = 0

        for record in records:
            record_size = len(json.dumps(record))

            # start a new batch , if adding current record to existing batch exceeds limit
            if (len(current_batch) >= ProcessorFile.MAX_RECORD_PER_BATCH
                    or current_batch_size + record_size > ProcessorFile.MAX_BATCH_SIZE
                    or record_size > ProcessorFile.MAX_RECORD_SIZE):
                batches.append(current_batch)
                current_batch = []
                current_batch_size = 0

            # if within limit, add the record in existing batch
            if record_size <= ProcessorFile.MAX_RECORD_SIZE:
                current_batch.append(record)
                current_batch_size += record_size

        # if the last batch is note empty, append that with the batches
        if current_batch:
            batches.append(current_batch)

        return batches
