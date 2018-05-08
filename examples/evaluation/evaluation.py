import rltk


class EvaluationRecord(rltk.Record):
    remove_raw_object = True

    @rltk.cached_property
    def id(self):
        return self.raw_object['id']

    @rltk.cached_property
    def data(self):
        return self.raw_object['data']


class EvaluationRecord2(rltk.Record):
    remove_raw_object = True

    @rltk.cached_property
    def id(self):
        return self.raw_object['id']

    @rltk.cached_property
    def data2(self):
        return self.raw_object['data2']


saved_ground_truth_file_name = 'ground_truth.csv'
gt = rltk.GroundTruth()
gt.load(saved_ground_truth_file_name)

gt.add_ground_truth('1', '12', True)
gt.save('saved_' + saved_ground_truth_file_name)

dataset_1_file_name = 'data_1.csv'
dataset_2_file_name = 'data_2.csv'

ds1 = rltk.Dataset(reader=rltk.CSVReader(dataset_1_file_name),
                   record_class=EvaluationRecord)
ds2 = rltk.Dataset(reader=rltk.CSVReader(dataset_2_file_name),
                   record_class=EvaluationRecord2)

eva = rltk.Evaluation()

for min_confidence_100 in range(0, 100):
    min_confidence = min_confidence_100 / 100
    trial = rltk.Trial(gt, min_confidence=0.5, top_k=0, save_record=True, key_1='data', key_2='data2',
                       label="min confidence is: " + str(min_confidence), parameter_x=min_confidence)
    pairs = rltk.get_record_pairs(ds1, ds2)
    for r1, r2 in pairs:
        c = rltk.levenshtein_similarity(r1.data, r2.data2)
        p = (c >= min_confidence)
        trial.add_result(r1, r2, p, c)

    trial.evaluate()

    print('false positives of ' + str(min_confidence) + '(min_confidence) is: ' + str(trial.false_positives))
    eva.add_trial(trial)

eva.draw('false_positives')
