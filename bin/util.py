import inflection

def underscore_keys(obj):
	for key in obj.keys():
		new_key = inflection.underscore(key)
		if new_key != key:
			obj[inflection.underscore(key)] = obj[key]
			del obj[key]