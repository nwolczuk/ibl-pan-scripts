require 'anystyle'
require 'json'

file = File.read('./data/sample_records.json')
data_hash = JSON.parse(file)

data_hash.each do |key, value|
	parsed_references = []
    value['citations'].each do |reference|
		parsed = AnyStyle.parse reference
		parsed_references.append([reference, parsed])
	end
	value[:parsed_references] = parsed_references
end

File.write('./data/anystyle_output.json', JSON.dump(data_hash))