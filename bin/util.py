import inflection

def underscore_keys(obj):
	for key in obj.keys():
		obj[inflection.underscore(key)] = obj[key]
		del obj[key]