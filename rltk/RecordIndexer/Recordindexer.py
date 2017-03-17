import sys, os, json


'''
Base class which indexes records based on different indexing techniques
Expects a config file of format
{
	"indexer_config":
		{
			"method": "q-gram",
			"index_params": 
				{
					"threshold": 0.8,
					"sub-type": "inverted-index",
					"size": 2,
					"keys":
						{
							"common": ["age"],
							"first_record":
								{
									"primary": ["first_name"],
									"secondary": ["last_name"]
								},
							"second_record":
								{
									"primary": ["fname"],
									"secondary": ["lname"]
								}
						}
				}
		}
}
'''
class RecordIndexer(object):
	def __init__(self, config_path=None):
		self.index_type = "base-block"
		#self.common, self.first_pindex, self.first_sindex = None, None, None
		if config_path is None:
			raise ValueError('Config path is not supplied. Invalid initialization')
		else:
			self.set_indexer_config(config_path)
		

	def is_qgram(self, index_val):
		return index_val == "q-gram"

	def is_base_blocking(self, index_val):
		return index_val == "base-block"

	def set_indexer_config(self, config_path):
		with open(config_path) as input_config:
			config_data = json.load(input_config)
			data = config_data.get("indexer_config")
			self.index_type = data.get("method")
			index_p = data.get("index_params")
			if self.is_qgram(self.index_type):
				self.build_qgram_config(index_p)
			elif self.is_base_blocking(self.index_type):
				self.build_base_blocking(index_p)

	#Have kept separate the config for each indexer intentionally
	def build_base_blocking(self, data):
		self.common = data.get("keys").get("common")
		self.first_pindex = data.get("keys").get("first_record").get("primary")
		self.first_sindex = data.get("keys").get("first_record").get("secondary")
		self.sec_pindex = data.get("keys").get("second_record").get("primary")
		self.sec_sindex = data.get("keys").get("second_record").get("secondary")

	def build_qgram_config(self, data):
		self.common = data.get("keys").get("common")
		self.first_pindex = data.get("keys").get("first_record").get("primary")
		self.first_sindex = data.get("keys").get("first_record").get("secondary")
		self.sec_pindex = data.get("keys").get("second_record").get("primary")
		self.sec_sindex = data.get("keys").get("second_record").get("secondary")
		self.threshold = data.get("threshold")
		self.size = data.get("size")

	#TO-DO
	def fetch_records(self, rtype='first'):
		if rtype == 'first':
			#Get first set of records only based on keys needed
			records = [('id1', 'Smith'), ('id2', 'Smith')]
		else:
			#Get first set of records only based on keys needed
			records = [('id1', 'Smith'), ('id2', 'Smith')]

	#TO-DO
	def build_index(self):
		first_records = self.fetch_records('first')
		second_records = self.fetch_records('second')

	#TO-DO
	def retreive_index(self):
		return [{'index_val': ["id1", "id2", "id3"]}]

def main():
	config_path=os.path.join('.',"config.json")
	r = RecordIndexer(config_path)
	r.build_index()
	r.retreive_index()

if __name__ == "__main__":
    sys.exit(main())