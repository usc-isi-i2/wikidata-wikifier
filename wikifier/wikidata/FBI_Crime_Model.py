import requests
import os, json, re, argparse
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
            'from_col': '$col'}

        self.county_QNode = dict()

        # table list for every year
        self.year_table = {2006: 10, 2007: 10, 2008: 10, 2009: 10, 2010: 10, 2011: 10, 2012: 10, 2013: 10,
                           2014: 10, 2015: 10, 2016: 8, 2017: 10}

        # abbreviate?
        self.use_abbreviate = {2006: True, 2007: True, 2008: True, 2009: True, 2010: True, 2011: False, 2012: False,
                               2013: False, 2014: False, 2015: False, 2016: False, 2017: False}
        # year url format
        self.y_s_url_format = {2006: 'https://www2.fbi.gov/ucr/cius2006/data/documents/06tbl10al.xls',
                               2007: 'https://www2.fbi.gov/ucr/cius2007/data/documents/07tbl10al.xls',
                               2008: 'https://www2.fbi.gov/ucr/cius2008/data/documents/08tbl10al.xls',
                               2009: 'https://www2.fbi.gov/ucr/cius2009/data/documents/09tbl10al.xls',
                               2010: 'https://ucr.fbi.gov/crime-in-the-u.s/2010/crime-in-the-u.s.-2010/tables/table-10/10tbl10al.xls/output.xls',
                               2011: 'https://ucr.fbi.gov/crime-in-the-u.s/2011/crime-in-the-u.s.-2011/tables/table10statecuts/table_10_offenses_known_to_law_enforcement_alabama_by_metropolitan_and_nonmetropolitan_counties_2011.xls/output.xls',
                               2012: 'https://ucr.fbi.gov/crime-in-the-u.s/2012/crime-in-the-u.s.-2012/tables/10tabledatadecpdf/table-10-state-cuts/table_10_offenses_known_to_law_enforcement_alabama_by_metropolitan_and_nonmetropolitan_counties_2012.xls/output.xls',
                               2013: 'https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013/tables/table-10/table-10-pieces/table_10_offenses_known_to_law_enforcement_by_alabama_by_metropolitan_and_nonmetropolitan_counties_2013.xls/output.xls',
                               2014: 'https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-10/table-10-pieces/table_10_offenses_known_to_law_enforcement_alabama_by_metropolitan_and_nonmetropolitan_counties_2014.xls/output.xls',
                               2015: 'https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015/tables/table-10/table-10-state-pieces/table_10_offenses_known_to_law_enforcement_alabama_by_metropolitan_and_nonmetropolitan_counties_2015.xls/output.xls',
                               2016: 'https://ucr.fbi.gov/crime-in-the-u.s/2016/crime-in-the-u.s.-2016/tables/table-8/table-8-state-cuts/alabama.xls/output.xls',
                               2017: 'https://ucr.fbi.gov/crime-in-the-u.s/2017/crime-in-the-u.s.-2017/tables/table-10/table-10-state-cuts/alabama.xls/output.xls'}

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
        # manually define kv pairs dict
        self.kv_dict = {'violent': 'C3001', 'murder': 'C3002', 'rape': 'C3003', 'robbery': 'C3005',
                        'aggravated': 'C3006', 'property': 'C3007', 'burglary': 'C3008',
                        'larceny': 'C3009', 'motor': 'C3010', 'arson': 'C3011'}

    def add_value(self, item, key, value, unit, year_value, reference):
        for property in self.kv_dict:
            if property in key:
                # add statement
                s = item.add_statement(self.kv_dict[property], QuantityValue(value, unit=unit))
                s.add_qualifier('P585', year_value)
                s.add_reference(reference)
                return
        print(key + ' is not implemented')
        raise Exception('')

    def download_data(self, years=None, states=None):

        year_list = list(self.year_table.keys())
        if years is not None:
            year_list = years

        # download all data or designated states
        state_list = list(self.state_abbr.keys())
        if states is not None:
            state_list = list()
            for s in states:
                s = s.lower().replace(' ', '-')
                state_list.append(s)
        for year in year_list:
            # delete and make new folder
            if not os.path.exists(DATA_ABS_ADDRESS + str(year)):
                os.makedirs(DATA_ABS_ADDRESS + str(year))

            for state in state_list:
                # get url
                download_url = self.y_s_url_format[int(year)]

                if self.use_abbreviate[int(year)]:
                    download_url = download_url.replace('al', self.state_abbr[state])
                else:
                    download_url = download_url.replace('alabama', state)

                # download and save excel files
                local_filename = state + '.xls'
                try:
                    with requests.get(download_url, stream=True) as r:
                        if r.status_code == 200:
                            print('Downloading crime data: ' + state + '_' + str(year))
                            with open(DATA_ABS_ADDRESS + str(year) + '/' + local_filename, 'wb') as f:

                                # save files
                                for chunk in r.iter_content():
                                    if chunk:
                                        f.write(chunk)
                except:
                    pass
        print('\n\nDownload completed!')

    def extract_data(self, years=None, states=None):

        # Initiate Excel Extractor
        ee = ExcelExtractor()
        state_list = list(self.state_abbr.keys())
        year_list = list(self.year_table.keys())
        # extract all data or designated states
        if states is not None:
            state_list = list()
            for s in states:
                s = s.lower().replace(' ', '-')
                state_list.append(s)

        # extract all data or designated years
        if years is not None:
            year_list = years
        res = dict()

        # regex to trim postfixes
        regex = 'countyunifiedpolicedepartment|,|[0-9]|policedepartment|countybureauof|countyu|publicsafety|unified|police'

        # yearwise and statewise extracting
        for year in year_list:
            for state in state_list:

                # read file
                file_path = DATA_ABS_ADDRESS + str(year) + '/' + state + '.xls'
                if not os.path.isfile(file_path):
                    continue
                else:
                    print('Extracting crime data for ' + state + '_' + str(year))

                    # extract data from excel files
                    sheet_year = '' + str(self.year_table[year]) if self.year_table[year] >= 10 else '0' + str(
                        self.year_table[year])
                    year_pre = '0' + str(year - 2000) if year - 2000 < 10 else str(year - 2000)
                    sheet_name = year_pre + 'tbl' + sheet_year + self.state_abbr[state]

                    # corner cases
                    if year < 2008: sheet_name = 'Sheet1'
                    if year == 2011 and state == 'oregon': sheet_name = '11tbl10'
                    extractions = ee.extract(file_name=file_path,
                                             sheet_name=sheet_name,
                                             region=['B,7', 'N,100'],
                                             variables=self.value_dict)
                    # build dictionary
                    county_data = dict()
                    for e in extractions:
                        if len(e['county']) > 0:

                            # clean data
                            name = e['county'].replace(' ', '').lower()
                            if '/' in name:
                                name = name.split('/')[1]
                            if '-' in name:
                                name = name.split('-')[1]

                            # trim bunch of postfixes
                            name = re.sub(regex, '', name)

                            # build county dictionary
                            if name not in county_data:
                                county_data[name] = dict()

                            # add extracted data
                            if e['value'] is not '' and isinstance(e['value'], int):

                                # clean data
                                ctg = e['category'].replace('\n', '_').replace(' ', '_').lower()
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

        # serialization
        f = open(file_path, 'w')
        f.write(doc.kg.serialize(format))
        f.close()

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
            county1 = name + 'county_' + abbr
            if county1 in self.county_QNode:
                return self.county_QNode[county1]

            # corner cases
            if county1 == 'dadecounty_fl':
                return 'Q468557'
            if county1 == 'donaanacounty_nm':
                return 'Q112953'
            if county1 == 'oglalalakotacounty_sd':
                return 'Q495201'
            if county1 == 'bartholemewcounty_in':
                return 'Q504385'
            if county1 == 'pottawattamiecounty_ks':
                return 'Q376947'
            if name == 'st.helena' and temp_state == 'louisiana':
                return 'Q507112'
            if name == 'story' and temp_state == 'nevada':
                return 'Q484431'
            if name == 'allegheny' and temp_state == 'maryland':
                return 'Q156257'
            if name == 'allegany' and temp_state == 'michigan':
                return 'Q133860'

            county2 = name + '_' + abbr
            if county2 in self.county_QNode:
                return self.county_QNode[county2]

        return None


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    parser = argparse.ArgumentParser(description='Year list')
    parser.add_argument('option', type=int,
                        help='1 for download, 2 for extraction and modeling', default=1)

    parser.add_argument('--years', type=str,
                        help='years you want to extract (default: all)', default="all")

    parser.add_argument('--states', type=str, default='all',
                        help='states you want to extract (default: "all")')

    args = parser.parse_args()

    choice = args.option
    years_str = args.years

    if years_str == 'all':
        years = None
    else:
        years = [int(x.strip()) for x in years_str.split(',')]
    states_str = args.states
    if states_str == 'all':
        states = None
    else:
        states = [x.strip() for x in states_str.split(',')]
    model = FBI_Crime_Model()

    # run once to get QNodes dictionary
    model.query_QNodes()

    if choice == 1:
        model.download_data(years, states)
    elif choice == 2:
        res = model.extract_data(years, states)
        model.model_data(res, 'result.ttl')
        with open('changes.tsv', 'w') as fp:
            serialize_change_record(fp)
    else:
        print("Option: 1 for download, 2 for extraction and modeling")
