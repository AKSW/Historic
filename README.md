# ExtracteurHistoriqueDBpedia

Need a Mongodb instance on port 27000.
Tested with Node.Js v0.10.41.

npm install

./init.sh

Put all the compressed (bz2) historic files in the dump directory.
Like this file for example frwiki-20160305-pages-meta-history1.xml-p000000003p000007538.bz2

Start the extractor with :

node HistoricExtractor.node.js





Fabien Gandon, Raphael Boyer, Olivier Corby, Alexandre Monnin. Wikipedia editing history in DBpedia : extracting and publishing the encyclopedia editing activity as linked data. IEEE/WIC/ACM International Joint Conference on Web Intelligence (WI' 16), Oct 2016, Omaha, United States. <hal-01359575>
https://hal.inria.fr/hal-01359575

Fabien Gandon, Raphael Boyer, Olivier Corby, Alexandre Monnin. Materializing the editing history of Wikipedia as linked data in DBpedia. ISWC 2016 - 15th International Semantic Web Conference, Oct 2016, Kobe, Japan. <http://iswc2016.semanticweb.org/>. <hal-01359583>
https://hal.inria.fr/hal-01359583
