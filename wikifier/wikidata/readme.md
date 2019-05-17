# Wikidata: Crime Data Modeling

## How to use

This is a tool to download, extract and model FBI crime data. Output is a `result.ttl` file in wikidata format and a `changes.tsv` file. Check samples in the `example/Wikifier_wikidata` folder.

Please use Python 3 (with ETK environment) to run the scripts.


Command: `python FBI_Crime_Model.py <arg1> --years <arg2> --states <arg3>`

arg1: `1` or `2`. 

* `1` for downloading the data. `2` for extracting and modeling the downloaded data.

arg2(optional): year string concatinates by ",".

* Example: `2006,2007,2008,2009`
* Now we support years from `2006` to `2017`. if you want to get all the years above, leave it empty.

arg3(optional): state string concatinates by ",".

* Example: `california,alabama,north-carolina`
* Now we support most states in the US. if you want to get all the states, leave it empty.


## Ontology Definitions
We need to define classes and properties to model the crime data.

Define Qnode for properties related to crime. We do this to organize the properties similar to how it is done for other properties, eg, [politics](https://www.wikidata.org/wiki/Q22984475).

```
q = WDItem('D1001')
q.add_label('Wikidata property related to crime', lang='en')

q.add_statement('P279/subclass of', URI('wd:Q22984475/Wikidata property'))
q.add_statement('P1269/facet of', URI('wd:Q83267/crime'))
```

Now we can define the various properties related to crime, similar to [ballots cast](https://www.wikidata.org/wiki/Property:P1868).

### Crime Properties

violent crime offenses:

```
p = WDProperty('C3001', Datatype.Quantity)
p.add_label('violent crime offenses', lang='en')
p.add_description("number of violent crime offenses reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q1520311/violent crime'))
```


murder and non-negligent manslaughter:

```
p = WDProperty('C3002', Datatype.Quantity)
p.add_label('murder and non-negligent manslaughter', lang='en')
p.add_description("number of murder and non-negligent manslaughter offenses reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q1295558/voluntary manslaughter'))
p.add_statement('P1629/subject item of this property', URI('wd:Q132821/murder'))
```

Rape (revised definition):

```
p = WDProperty('C3003', Datatype.Quantity)
p.add_label('Rape (revised definition)', lang='en')
p.add_description("number of rapes (revised definition) reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q47092/rape'))
```

Rape (legacy definition):

```
p = WDProperty('C3004', Datatype.Quantity)
p.add_label('Rape (legacy definition)', lang='en')
p.add_description("number of rapes (legacy definition) reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q47092/rape'))
```

Robbery:

```
p = WDProperty('C3005', Datatype.Quantity)
p.add_label('Robbery', lang='en')
p.add_description("number of roberies reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q53706/robbery'))
```

Aggravated assault:

```
p = WDProperty('C3006', Datatype.Quantity)
p.add_label('Aggravated assault', lang='en')
p.add_description("number of aggravated assaults reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q365680/assault'))
p.add_statement('P1629/subject item of this property', URI('wd:Q81672/assault'))
```

Property crime:

```
p = WDProperty('C3007', Datatype.Quantity)
p.add_label('Property crime', lang='en')
p.add_description("number of property crimes reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q857984/property crime'))
```

Burglary:

```
p = WDProperty('C3008', Datatype.Quantity)
p.add_label('Burglary', lang='en')
p.add_description("number of Burglaries reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q329425/burglary'))
```

Larceny-theft:

```
p = WDProperty('C3009', Datatype.Quantity)
p.add_label('Larceny-theft', lang='en')
p.add_description("number of Larceny-theft reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q2485381/larceny'))
p.add_statement('P1629/subject item of this property', URI('wd:Q2727213/theft'))
```

Motor vehicle theft:

```
p = WDProperty('C3010', Datatype.Quantity)
p.add_label('Motor vehicle theft', lang='en')
p.add_description("number of Motor vehicle thefts reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q548007/motor vehicle theft'))
p.add_statement('P1629/subject item of this property', URI('wd:Q2727213/theft'))
```

Arson:

```
p = WDProperty('C3011', Datatype.Quantity)
p.add_label('Arson', lang='en')
p.add_description("number of arsons reported by the sheriff's office or county police department", lang='en')

p.add_statement('P31/instance of', URI('wd:D1001/wd prop related to crime'))
p.add_statement('P1629/subject item of this property', URI('wd:Q327541/arson'))
```

### Units
Offenses are reported for a period of type, so the quantity needs to be represented in units such as `offenses/year`:

```
q = WDItem('D1002')
q.add_label('offenses per year', lang='en')

q.add_statement('P31/instance of', URI('wd:Q47574/unit of measurement'))
q.add_statement('P1629/subject item of this property', URI('wd:Q83267/crime'))
```

## Modeling The FBI Data

We will represent each statistic as a property of a county item.

```
q = WDItem('Q156168/Autauga County')

year_2014 = TimeValue('2014', calendar=URI('wd:Q1985727'), precision=Precision.year))

reference = WDRef()
reference.add_property('P248/stated in', URI('Q8333/FBI'))
reference.add_property('P854/reference URL', 'https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-10/table-10-pieces/Table_10_Offenses_Known_to_Law_Enforcement_Alabama_by_Metropolitan_and_Nonmetropolitan_Counties_2014.xls')

s = q.add_statement('C3001/violent crime', QuantityValue(68, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3002/murder and non-negligent manslaughter', QuantityValue(2, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3003/Rape (revised definition)', QuantityValue(8, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3005/Robbery', QuantityValue(6, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3006/Aggravated assault', QuantityValue(52, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3007/Property crime', QuantityValue(414, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3008/Burglary', QuantityValue(170, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3009/Larceny-theft', QuantityValue(199, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)

s = q.add_statement('C3010/Motor vehicle theft', QuantityValue(45, URI('D1002/offenses per year'))
s.add_qualifier('P585/point in time', year_2014)
s.add_reference(reference)
```