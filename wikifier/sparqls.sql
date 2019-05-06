SELECT DISTINCT ?v ?x
WHERE
{
  ?x wdt:P212 ?v .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

SELECT DISTINCT ?P_ID ?P_IDLabel
WHERE
{
  ?P_ID wdt:P31/wdt:P279* wd:Q19847637 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
ORDER BY DESC(?P_ID)