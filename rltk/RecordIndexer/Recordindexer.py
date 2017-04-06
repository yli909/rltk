import sys, os, json
from jsonpath_rw import parse
from jsonpath_rw.jsonpath import Fields
from collections import defaultdict

#Fix later - add relative import
#sys.path.append('/home/chinmay/CODE/ISI/toolkit/rltk/rltk/tokenizer/digCrfTokenizer')
#from ..tokeniser.digCrfTokenizer import ngramTokenizer as nt
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
	"../tokenizer/digCrfTokenizer")))
from crf_tokenizer import ngramTokenizer

'''
Base class which indexes records based on different indexing techniques
Expects a config file of format
{
	"method": 
		{
			"type": "base-block",
			"q": 2,
			"skip_html": "True"
		},
	"json_path": [],
	"id_path": ""
}
'''
class RecordIndexer(object):
	def __init__(self, config_path=None, file_path=None):
		self.index_type = "base-block"
		if config_path is None or file_path is None:
			raise ValueError('Config path is not supplied. Invalid initialization')
		else:
			self.build_indexer_config(config_path)
		self.indexer = defaultdict(list)
		self.file_path = file_path
		
	def is_qgram(self, index_val):
		return index_val == "q-gram"

	def is_base_blocking(self, index_val):
		return index_val == "base-block"

	def build_indexer_config(self, config_path):
		with open(config_path) as input_config:
			config_data = json.load(input_config)
			self.id_path = config_data.get("id_path")
			self.json_path = config_data.get("json_path")

			data = config_data.get("method")
			self.index_type = data.get("type")
			self.build_tokeniser_config(data)
			if self.is_qgram(self.index_type):
				self.build_qgram_config(data)
			elif self.is_base_blocking(self.index_type):
				self.build_base_blocking(data)

	def build_tokeniser_config(self, data):
		#self.tokenizer_type = data.get("tokenise_params").get("type")
		pass

	#Have kept separate the config for each indexer intentionally
	def build_base_blocking(self, data):
		pass

	def build_qgram_config(self, data):
		self.size = data.get("q")

	def get_indexer_keys(self):
		return self.indexer.keys()

	def qgram_indexer(self, record):
		nt = ngramTokenizer()
		blocking_value = record[1]
		qgrams = []
		#Get qgrams for each of the blocking value
		for bv in blocking_value:
			qgrams.extend(nt.basic(bv, self.size))

		for gram in qgrams:
			self.indexer[gram].append(record[0])

	def base_block_indexer(self, record):
		blocking_value = record[1]
		for bv in blocking_value:
			self.indexer[gram].append(record[0])

	def construct_index(self, record):
		if self.is_qgram(self.index_type):
			self.qgram_indexer(record)
		else:
			self.base_block_indexer(record)

	#TO-DO optimise later
	def build_index(self):
		if self.is_qgram(self.index_type):
			blocking_keys = list()
			id_path = parse(self.id_path)
			for blocking_key in self.json_path:
				blocking_keys.append(parse(blocking_key))
			with open(self.file_path) as ip_file:
				for line in ip_file:
					jline = json.loads(line)
					blocking_value = []
					for b in blocking_keys:
						blocking_value.extend([match.value for match in b.find(jline)])

					record_id = [match.value for match in id_path.find(jline)]
					record = (record_id[0], blocking_value)
					self.qgram_indexer(record)
		else:
			with open(self.file_path) as ip_file:
				for line in ip_file:
					jline = json.loads(line)
					blocking_value = []

					#TO-DO optimise later
					for blocking_key in self.json_path:
						blocking_value.extend([match.value for match in parse(blocking_key).find(jline)])
					record_id = [match.value for match in parse(self.id_path).find(jline)]
					record = (record_id[0], blocking_value)
					self.qgram_indexer(record)

	def retreive_index(self):
		return self.indexer


	def build_inverted_index(self, index=None):
		if index is None:
			index = self.indexer
		inverted_index = defaultdict(list)
		for k,id_list in index.items():
			for id_v in id_list:
				inverted_index[id_v].append(k)
		return inverted_index

	@staticmethod
	#TO-DO Write to output in batches
	def qgram_candidate_pairs(r1, r2, threshold,output_path):
		r1_index = r1.retreive_index()
		r2_index = r2.retreive_index()

		for k,v in r1_index.items():
			l = len(v) + len(r2_index[k])
			if l > threshold:
				del r1_index[k]
				del r2_index[k]

		inv_index = r1.build_inverted_index(r1_index)
		data = []
		for k,v in inv_index.items():
			candidate_set = set()
			for id_p in v:
				candidate_set |=  set(r2_index[id_p])

			for c in candidate_set:
				with open(output_path, 'a') as ofile:
					d = {'first_record_id': k, 'second_record_id': c}
					json.dump( d,ofile)
					ofile.write('\n')

def main():
	cur_dir = os.path.dirname(os.path.realpath(__file__))
	cp = os.path.realpath(os.path.join(cur_dir,"../../examples/indexer_examples/config.json"))
	config_path= os.path.realpath(os.path.join('../../examples/indexer_examples/',"config.json"))
	file_path = os.path.realpath(os.path.join('../../examples/indexer_examples/',"ulan.json"))

	r1 = RecordIndexer(config_path, file_path)
	r1.build_index()

	file_path= os.path.realpath(os.path.join('../../examples/indexer_examples/',"ima.json"))
	r2 = RecordIndexer(config_path, file_path)
	r2.build_index()

	output_path = os.path.realpath(os.path.join('../../examples/indexer_examples/', 'test_output.json'))
	RecordIndexer.qgram_candidate_pairs(r1, r2, 100, output_path)

if __name__ == "__main__":
    sys.exit(main())
