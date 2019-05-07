import requests
import os, json
from etk.etk import ETK
from etk.extractors.excel_extractor import ExcelExtractor
from etk.knowledge_graph import KGSchema, URI, Literal, LiteralType, Subject, Reification
from etk.etk_module import ETKModule
from etk.wikidata.entity import WDProperty, WDItem
from etk.wikidata.value import Datatype, Item, TimeValue, Precision, QuantityValue
from etk.wikidata.statement import WDReference
from etk.wikidata import serialize_change_record
from SPARQLWrapper import SPARQLWrapper, JSON

# DATA_ABS_ADDRESS = "/Users/pszekely/Downloads/datamart_demo/wikidata-wikifier/wikifier/wikidata/data/"
# DATA_ABS_ADDRESS = "/Users/minazuki/Desktop/studies/master/2018Summer/DSBOX_2019/wikidata-wikifier/wikifier/wikidata/data/"
DATA_ABS_ADDRESS = "data/"

class FBI_Crime_Model():
    def __init__(self):

        self.value_dict = {
            'value': '$col,$row',
            'county': '$B,$row',
            'category': '$col,$6',
            'from_row': '$row',
            'from_col': '$col'
        }
        self.county_QNode = dict()

        # hashmap for states and their abbrviate
        self.state_abbr = {'alabama': 'al', 'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar', 'california': 'ca',
                           'colorado': 'co', 'connecticut': 'ct', 'delaware': 'de', 'florida': 'fl', 'georgia': 'ga',
                           'hawaii': 'hi',
                           'idaho': 'id', 'illinois': 'il', 'indiana': 'in', 'iowa': 'ia', 'kansas': 'ks',
                           'kentucky': 'ky',
                           'louisiana': 'la', 'maine': 'me', 'maryland': 'md', 'massachusetts': 'ma', 'michigan': 'mi',
                           'minnesota': 'mn',
                           'mississippi': 'ms', 'missouri': 'mo', 'montana': 'mt', 'nebraska': 'ne', 'nevada': 'nv',
                           'new-hampshire': 'nh', 'new-jersey': 'nj', 'new-mexico': 'nm', 'new-york': 'ny',
                           'north-carolina': 'nc', 'north-dakota': 'nd', 'ohio': 'oh', 'oklahoma': 'ok', 'oregon': 'or',
                           'pennsylvania': 'pa',
                           'rhode-island': 'ri', 'south-carolina': 'sc', 'south-dakota': 'sd', 'tennessee': 'tn',
                           'texas': 'tx', 'utah': 'ut', 'vermont': 'vt', 'virginia': 'va', 'washington': 'wa',
                           'west-virginia': 'wv',
                           'wisconsin': 'wi', 'wyoming': 'wy'}

        # table list for every year
        self.year_table = {2016: 8, 2017: 10}

    def add_value(self, item, key, value, unit, year_value, reference):

        # manually define kv pairs dict
        kv_dict = {'violent': 'C3001', 'murder': 'C3002','rape': 'C3003','robbery': 'C3005',
                   'aggravated': 'C3006', 'property': 'C3007', 'burglary': 'C3008',
                   'larceny-': 'C3009', 'motor': 'C3010', 'arson': 'C3011'}

        if key not in kv_dict:
            raise Exception(key + ' is not implemented')

        # add statement
        s = item.add_statement(kv_dict[key], QuantityValue(value, unit=unit))
        s.add_qualifier('P585', year_value)
        s.add_reference(reference)

    def download_data(self, year, states=None):

        # delete and make new folder
        if not os.path.exists(DATA_ABS_ADDRESS + str(year)):
            os.makedirs(DATA_ABS_ADDRESS + str(year))

        # download all data or designated states
        state_list = list(self.state_abbr.keys())
        if states is not None:
            state_list = list()
            for s in states:
                s = s.lower().replace(' ', '-')
                state_list.append(s)

        for state in state_list:

            # concatenate url
            download_url = 'https://ucr.fbi.gov/crime-in-the-u.s/' + str(year) + '/crime-in-the-u.s.-' + str(
                year) + '/tables/table-' + str(self.year_table[year]) + '/table-' + str(
                self.year_table[year]) + '-state-cuts/' + str(
                state) + '.xls' + '/output.xls'

            # download and save excel files
            local_filename = state + '.xls'
            try:
                with requests.get(download_url, stream=True) as r:
                    if r.status_code == 200:
                        print('Downloading crime data: ' + state + '_' + str(year))
                        with open('data/' + str(year) + '/' + local_filename, 'wb') as f:

                            # save files
                            for chunk in r.iter_content():
                                if chunk:
                                    f.write(chunk)
            except:
                pass
        print('\n\nDownload completed!')

    def extract_data(self, year, states=None):

        # Initiate Excel Extractor
        ee = ExcelExtractor()
        state_list = list(self.state_abbr.keys())

        # extract all data or designated states
        if states is not None:
            state_list = list()
            for s in states:
                s = s.lower().replace(' ', '-')
                state_list.append(s)
        res = dict()
        for state in state_list:

            # read file
            file_path = DATA_ABS_ADDRESS + str(year) + '/' + state + '.xls'
            if not os.path.isfile(file_path):
                # print('Crime data for ' + state + '_' + str(year) + ' does not exist. Please download it first!')
                continue
            else:
                print('Extracting crime data for ' + state + '_' + str(year))

                # extract data from excel files
                sheet_year = '' + str(self.year_table[year]) if self.year_table[year] >= 10 else '0' + str(
                    self.year_table[year])
                sheet_name = str(year - 2000) + 'tbl' + sheet_year + self.state_abbr[state]
                extractions = ee.extract(file_name=file_path,
                                         sheet_name=sheet_name,
                                         region=['B,7', 'N,100'],
                                         variables=self.value_dict)
                # build dictionary
                county_data = dict()
                for e in extractions:
                    if len(e['county']) > 0:
                        name = e['county'].replace(' ', '').lower()
                        if name[-1].isdigit():
                            name = name[:-1]
                        if 'policedepartment' in name:
                            name = name[:-16]
                        if 'county' in name:
                            name = name[:-6]
                        if '/' in name:
                            name = name.split('/')[1]
                        if '-' in name:
                            name = name.split('-')[1]
                        if 'publicsafety' in name:
                            name = name[:-12]
                        if 'unified' in name:
                            name = name[:-7]

                        # build county dictionary
                        if name not in county_data:

                            county_data[name] = dict()

                        # add extracted data
                        if e['value'] is not '' and isinstance(e['value'], int):
                            ctg = e['category'].replace('\n', ' ').split(' ')[0].lower()
                            if ctg[-1].isdigit():
                                ctg = ctg[:-1]
                            if ctg in county_data[name]:
                                county_data[name][ctg] += e['value']
                            else:
                                county_data[name][ctg] = e['value']
                res[state + '_' + str(year)] = county_data
        print('\n\nExtraction completed!')
        return res

    def model_data(self, crime_data, file_path, format='ttl'):

        # initialize
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

        # bind prefixes
        doc.kg.bind('wikibase', 'http://wikiba.se/ontology#')
        doc.kg.bind('wd', 'http://www.wikidata.org/entity/')
        doc.kg.bind('wdt', 'http://www.wikidata.org/prop/direct/')
        doc.kg.bind('wdtn', 'http://www.wikidata.org/prop/direct-normalized/')
        doc.kg.bind('wdno', 'http://www.wikidata.org/prop/novalue/')
        doc.kg.bind('wds', 'http://www.wikidata.org/entity/statement/')
        doc.kg.bind('wdv', 'http://www.wikidata.org/value/')
        doc.kg.bind('wdref', 'http://www.wikidata.org/reference/')
        doc.kg.bind('p', 'http://www.wikidata.org/prop/')
        doc.kg.bind('pr', 'http://www.wikidata.org/prop/reference/')
        doc.kg.bind('prv', 'http://www.wikidata.org/prop/reference/value/')
        doc.kg.bind('prn', 'http://www.wikidata.org/prop/reference/value-normalized/')
        doc.kg.bind('ps', 'http://www.wikidata.org/prop/statement/')
        doc.kg.bind('psv', 'http://www.wikidata.org/prop/statement/value/')
        doc.kg.bind('psn', 'http://www.wikidata.org/prop/statement/value-normalized/')
        doc.kg.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
        doc.kg.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
        doc.kg.bind('pqn', 'http://www.wikidata.org/prop/qualifier/value-normalized/')
        doc.kg.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        doc.kg.bind('prov', 'http://www.w3.org/ns/prov#')
        doc.kg.bind('schema', 'http://schema.org/')

        # first we add properties and entities
        # Define Qnode for properties related to crime.
        q = WDItem('D1001')
        q.add_label('Wikidata property related to crime', lang='en')
        q.add_statement('P279', Item('Q22984475'))
        q.add_statement('P1269', Item('Q83267'))
        doc.kg.add_subject(q)

        # violent crime offenses
        p = WDProperty('C3001', Datatype.QuantityValue)
        p.add_label('violent crime offenses', lang='en')
        p.add_description(
            "number of violent crime offenses reported by the sheriff's office or county police department", lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q1520311'))
        doc.kg.add_subject(p)

        # murder and non - negligent manslaughter
        p = WDProperty('C3002', Datatype.QuantityValue)
        p.add_label('murder and non-negligent manslaughter', lang='en')
        p.add_description(
            "number of murder and non-negligent manslaughter offenses reported by the sheriff's office or county police department",
            lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q1295558'))
        p.add_statement('P1629', Item('Q132821'))
        doc.kg.add_subject(p)

        # Rape(revised definition)
        p = WDProperty('C3003', Datatype.QuantityValue)
        p.add_label('Rape (revised definition)', lang='en')
        p.add_description(
            "number of rapes (revised definition) reported by the sheriff's office or county police department",
            lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q47092'))
        doc.kg.add_subject(p)

        # Rape(legacy definition)
        p = WDProperty('C3004', Datatype.QuantityValue)
        p.add_label('Rape (legacy definition)', lang='en')
        p.add_description(
            "number of rapes (legacy definition) reported by the sheriff's office or county police department",
            lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q47092'))
        doc.kg.add_subject(p)

        # robbery
        p = WDProperty('C3005', Datatype.QuantityValue)
        p.add_label('Robbery', lang='en')
        p.add_description("number of roberies reported by the sheriff's office or county police department", lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q53706'))
        doc.kg.add_subject(p)

        # aggravated assault
        p = WDProperty('C3006', Datatype.QuantityValue)
        p.add_label('Aggravated assault', lang='en')
        p.add_description("number of aggravated assaults reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q365680'))
        p.add_statement('P1629', Item('Q81672'))
        doc.kg.add_subject(p)

        # property crime
        p = WDProperty('C3007', Datatype.QuantityValue)
        p.add_label('Property crime', lang='en')
        p.add_description("number of property crimes reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q857984'))
        doc.kg.add_subject(p)

        # burglary
        p = WDProperty('C3008', Datatype.QuantityValue)
        p.add_label('Burglary', lang='en')
        p.add_description("number of Burglaries reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q329425'))
        doc.kg.add_subject(p)

        # larceny - theft
        p = WDProperty('C3009', Datatype.QuantityValue)
        p.add_label('Larceny-theft', lang='en')
        p.add_description("number of Larceny-theft reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q2485381'))
        p.add_statement('P1629', Item('Q2727213'))
        doc.kg.add_subject(p)

        # motor vehicle theft
        p = WDProperty('C3010', Datatype.QuantityValue)
        p.add_label('Motor vehicle theft', lang='en')
        p.add_description("number of Motor vehicle thefts reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q548007'))
        p.add_statement('P1629', Item('Q2727213'))
        doc.kg.add_subject(p)

        # arson
        p = WDProperty('C3011', Datatype.QuantityValue)
        p.add_label('Arson', lang='en')
        p.add_description("number of arsons reported by the sheriff's office or county police department", lang='en')
        p.add_statement('P31', Item('D1001'))
        p.add_statement('P1629', Item('Q327541'))
        doc.kg.add_subject(p)

        # Offenses are reported for a period of type,
        # so the quantity needs to be represented in units such as offenses / year
        unit = WDItem('D1002')
        unit.add_label('offenses per year', lang='en')
        unit.add_statement('P31', Item('Q47574'))
        unit.add_statement('P1629', Item('Q83267'))
        # doc.kg.add_subject(unit)

        # we begin to model data extracted
        for state_year in crime_data:
            print('Modeling data for ' + state_year)
            state, year = state_year.split('_')

            # add year value
            year_value = TimeValue(year, calendar=Item('Q1985727'), precision=Precision.year, time_zone=0)

            # add reference, data source
            download_url = 'https://ucr.fbi.gov/crime-in-the-u.s/' + str(year) + '/crime-in-the-u.s.-' + str(
                year) + '/tables/table-' + str(self.year_table[int(year)]) + '/table-' + str(
                self.year_table[int(year)]) + '-state-cuts/' + str(state) + '.xls'
            reference = WDReference()
            reference.add_property(URI('P248'), URI('wd:Q8333'))
            reference.add_property(URI('P854'), Literal(download_url))

            for county in crime_data[state_year]:

                # county entity
                Qnode = self.get_QNode(county, state)
                if Qnode is None:
                    continue
                q = WDItem(Qnode)

                # add value for each property
                for property in crime_data[state_year][county]:
                    self.add_value(q, property, crime_data[state_year][county][property], unit, year_value,
                                   reference)

                # add the entity to kg
                doc.kg.add_subject(q)
        print('\n\nModeling completed!\n\n')
        f = open(file_path, 'w')
        f.write(doc.kg.serialize(format))
        print('Serialization completed!')

    def query_QNodes(self):

        # query QNode for each county
        endpoint_url = "https://query.wikidata.org/sparql"

        query = """SELECT ?QNode ?name ?state
                    WHERE { ?QNode  wdt:P882 ?v .
                            ?QNode rdfs:label ?name.
                            ?QNode wdt:P131 ?sq.
                            ?sq rdfs:label ?state.
                          FILTER(LANG(?name) = "en")
                          FILTER(LANG(?state) = "en")}
                """

        # get Qnodes from wikidata
        def get_results(endpoint_url, query):
            sparql = SPARQLWrapper(endpoint_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            return sparql.query().convert()

        results = get_results(endpoint_url, query)
        county_Q = dict()

        # concatenate county name and state and build dict
        for result in results["results"]["bindings"]:
            name = result['name']['value'].lower().replace(' ', '')
            if 'parish' in name:
                name = name[:-6]
            state = result['state']['value'].lower().replace(' ', '-')
            if state in self.state_abbr:
                abbr = self.state_abbr[state]
                QNode = result['QNode']['value'].split('/')[-1]
                county_Q[name + '_' + abbr] = QNode

        # save json file
        f = open('county_QNode.json', 'w')
        f.write(json.dumps(county_Q, indent=4))
        f.close()
        print('\n\nCounty - QNode mapping dictionary created\n\n')
        return county_Q

    def get_QNode(self, county, state):
        # read county_QNode.json to build dictionary
        if len(self.county_QNode) == 0:
            f = open('county_QNode.json', 'r')
            self.county_QNode = json.loads(f.read())
            f.close()
        name = county.lower().replace(' ', '')

        temp_state = state.lower().replace(' ', '-')
        # map state to its abbreviate
        if temp_state in self.state_abbr:
            abbr = self.state_abbr[temp_state]

            # find Qnode

            county1 = county + 'county_' + abbr
            county2 = county + '_' + abbr
            # print(county1, county2)
            if county1 in self.county_QNode:
                return self.county_QNode[county1]
            if county2 in self.county_QNode:
                return self.county_QNode[county2]

            # corner cases
            if county1 == 'dadecounty_fl':
                return 'Q468557'
            if county1 == 'donaanacounty_nm':
                return 'Q112953'
            if county1 == 'oglalalakotacounty_sd':
                return 'Q495201'
        return 'Q504385'

def generate_fbi_data(states):
    for each_state in states:
        res = model.extract_data(2016, [each_state])
        model.model_data(res, each_state + '.ttl')
        with open('changes_' + each_state + '.tsv', 'w') as fp:
            serialize_change_record(fp)
            
if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    choice = sys.argv[1]
    state = sys.argv[2]
    model = FBI_Crime_Model()
    # run once to get QNodes dictionary
    model.query_QNodes()
    
    if choice == "download":
        model.download_data(2016, [state])
    elif choice == "generate":
        generate_fbi_data([state])

