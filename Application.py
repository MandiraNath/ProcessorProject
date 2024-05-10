from ProcessorFile import ProcessorFile

input_records = ["record1", "record2", "record3", "record4","record5", "record6", "record7", "record8"]
batches = ProcessorFile.split_into_batches(input_records)
print(batches)