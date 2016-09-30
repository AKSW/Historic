
var Readable = require('stream').Readable;
var util = require('util');
var fs = require('fs');
var Writable = require('stream').Writable;
var XmlParser = require('xml-parser');
var StringDecoder = require('string_decoder').StringDecoder;
var MongoClient = require('mongodb').MongoClient

var out  = "./rdfRelease/";
var addFile = "";
var firstPrintPrefix = true;


function RawPageSpliter(dbMongo){

	this._buffer = "";
    this._bufferForExtract = "";
    this.nextData = new Array();
	this._decoder = new StringDecoder('utf8');
    this.db = dbMongo;

    this.arrayPage = new Object();
    this.arrayRev = new Object();


    this.pagePos = -1;
    this.revisionPos = -1;
    this.first = true;

    this.pageCountRel = 0;
    this.revisionCountRel = 0;

    this.ruleModulo = 0;
    this.ruleSize = 0;
    this.currentIndexMultiple = 0;

    this.run = true;
    this.isRunning = false;
}



RawPageSpliter.prototype.setIndexRule = function(modulo, size, nameF){
    this.ruleModulo = modulo;
    this.ruleSize = size;
    addFile = nameF;
    firstPrintPrefix = true;
}

RawPageSpliter.prototype.push = function(data){
    this.nextData.unshift(""+data);
}


RawPageSpliter.prototype.stop = function(){
    var self = this;
    this.arrayPage[this.pageId].page = undefined;
    this.run = false;
    this.isRunning = false;
}

RawPageSpliter.prototype.start = function(){
    this.isRunning = true;

    while(this.nextData.length > 0){
        this._buffer += this.nextData.pop();

        do{
            var endText = this._buffer.indexOf("</text>");
            if(endText == -1)
                break;
            var textPos = this._buffer.indexOf("<text");
            if(textPos == -1){
                this._bufferForExtract += this._buffer;
                this._buffer = "";
                break;
            }
            var size = endText - (textPos + "<text xml:space=\"preserve\">".length);
            var sizeStr = "<size>"+size+"</size>";

            this._bufferForExtract += this._buffer.substring(0, textPos) + sizeStr;
            this._buffer = this._buffer.substring(endText+"</text>".length);
            
        }while(true);

    }

    this.extract();

}


//
//  Methode critique : necessite une optimisation maximale
//
//  Les indexOf et substring doivent etre utilise le moins possible, car couteux en ressources.
//
RawPageSpliter.prototype.extract = function(){

    this.revisionPos = this._bufferForExtract.indexOf("<revision>");
    this.pagePos = this._bufferForExtract.indexOf("<page>");
    do{
        //Page
        if( this.pagePos != -1 && ( this.pagePos < this.revisionPos || this.revisionPos == -1 ) ){
            if(!this.first){
                var self = this;
                if(self.arrayPage[self.pageId].page)
                    extractPage(self.arrayPage[self.pageId].page, self.arrayPage[self.pageId].revision);
                this.arrayPage[this.pageId].page = undefined;
                this.arrayPage[this.pageId].revision = undefined;
                this.arrayPage[this.pageId] = undefined;
            }else{
                this.first = false;
            }
            var endPage = this._bufferForExtract.indexOf("<revision>");
            if(endPage == -1)
                break ;
            var pageContent = this._bufferForExtract.substring(this.pagePos + "<page>".length, endPage);


            this.pageId = (this.currentIndexMultiple * this.ruleSize) + this.ruleModulo;
            this.arrayPage[this.pageId] = new Object();
            this.currentIndexMultiple++;


            this.arrayPage[this.pageId].page = "<page>"+pageContent+"</page>";

            this.pageCountRel++;

            this._bufferForExtract = this._bufferForExtract.substring(endPage);
        }

        //Revision
        if( this.revisionPos != -1 && ( this.revisionPos < this.pagePos || this.pagePos == -1 ) ){
            var endRevision = this._bufferForExtract.indexOf("</revision>");
            if(endRevision == -1)
                break ;
            var revisionContent = this._bufferForExtract.substring(this.revisionPos + "<revision>".length, endRevision);

            if(!this.arrayPage[this.pageId].revision)
                this.arrayPage[this.pageId].revision = new Array();
            this.arrayPage[this.pageId].revision.push("<revision>"+revisionContent+"</revision>");

            this.revisionCountRel++;

            this._bufferForExtract = this._bufferForExtract.substring(endRevision + "</revision>".length);
        }

        this.revisionPos = this._bufferForExtract.indexOf("<revision>");
        this.pagePos = this._bufferForExtract.indexOf("<page>");

    }while( !(this.revisionPos == -1 && this.pagePos == -1) );

    var self = this;
    if(this.isRunning)
        setTimeout(function(){self.start();}, 100);
}

















































































function extractPage(page, revisions){
    var data = {"page":page, "revisions":revisions};

    data = dataFormater(data);
    data = pageHeadParser(data);
    data = revisionCounter(data);
    data = dateStartEnd(data);
    data = contributorParser(data);
    data = compileData(data);

    data.page = null;
    data.revisions = null;

    fs.appendFileSync(out+addFile+".ttl", data);

}


function dataFormater(data){
    data.page = XmlParser(data.page);
    for(var i=0; i<data.revisions.length; i++)
        data.revisions[i] = XmlParser(data.revisions[i]);
    data.parsedData = new Object();
    return data;
}

function pageHeadParser(data){
    if(data.page.root){
        data.page.root.children.forEach(function(elem){
            if(elem.name == "title"){
                data.parsedData.uri = "http://fr.dbpedia.org" + '/resource/' + elem.content.replace(/ /g, "_");
                data.parsedData.titleOfWiki = elem.content.replace(/ /g, "_");
            }else if(elem.name == "id"){
                data.parsedData.id = elem.content;
            }
        });

        return data;
    }
}

function revisionCounter(data){
    data.parsedData.countRevision = data.revisions.length;
    return data;
}

function dateStartEnd(data){
    var count = 0;
    var arrayDate = new Array();
    var arraySize = new Array();


    data.revisions.forEach(function(elem){

        elem.root.children.forEach(function(elem2){
            if(elem2.name == "timestamp"){
                var date = elem2.content.split("T")[0];
                date += "T"+elem2.content.split("T")[1].split("Z")[0];
                arrayDate.push(date);
            }
            if(elem2.name == "size"){
                arraySize.push(elem2.content);
            }
        });


    });

    arrayDate.sort(function(a, b){
        var dateA = a.split("T")[0].split("-");
        var dateB = b.split("T")[0].split("-");

        for(var i=0; i<=1; i++){
            if(dateA[i] != dateB[i])
                return dateA[i]-dateB[i];
        }
        return 0;
    })

    data.parsedData.startDate = arrayDate[0];
    data.parsedData.endDate = arrayDate[arrayDate.length-1];
    data.parsedData.revisionsDate = arrayDate;
    data.parsedData.revisionsPerYear = new Object();
    data.parsedData.revisionsPerMonth = new Object();
    data.parsedData.averageSizePerYear = new Object();
    data.parsedData.averageSizePerMonth = new Object();
    arrayDate.forEach(function(elem, index){
        var year = elem.split("T")[0].split("-")[0];
        if(!data.parsedData.revisionsPerYear[ year ])
            data.parsedData.revisionsPerYear[ year ] = 0;
        data.parsedData.revisionsPerYear[ year ]++;

        if(!data.parsedData.averageSizePerYear[ year ])
            data.parsedData.averageSizePerYear[ year ] = new Array();
        data.parsedData.averageSizePerYear[ year ].push( arraySize[index] );
    });
    arrayDate.forEach(function(elem, index){
        var year = elem.split("T")[0].split("-")[0];
        var month = elem.split("T")[0].split("-")[1];
        if(!data.parsedData.revisionsPerMonth[ month+"/"+year ])
            data.parsedData.revisionsPerMonth[ month+"/"+year ] = 0;
        data.parsedData.revisionsPerMonth[ month+"/"+year ]++;

        if(!data.parsedData.averageSizePerMonth[ month+"/"+year ])
            data.parsedData.averageSizePerMonth[ month+"/"+year ] = new Array();
        data.parsedData.averageSizePerMonth[ month+"/"+year ].push( arraySize[index] );
    });

    for(var index in data.parsedData.averageSizePerYear){
        if(data.parsedData.averageSizePerYear[ index ].length > 0){
            var array = data.parsedData.averageSizePerYear[ index ];
            var sum = 0;
            for(var i=0; i<array.length; i++)
                sum += parseInt(array[i]);
            data.parsedData.averageSizePerYear[ index ] = parseInt((sum / array.length)*100) / 100;
        }
    }
    for(var index in data.parsedData.averageSizePerMonth){
        if(data.parsedData.averageSizePerMonth[ index ].length > 0){
            var array = data.parsedData.averageSizePerMonth[ index ];
            var sum = 0;
            for(var i=0; i<array.length; i++)
                sum += parseInt(array[i]);
            data.parsedData.averageSizePerMonth[ index ] = parseInt((sum / array.length)*100) / 100;
        }
    }

    return data;

}


function contributorParser(data){
    var uniqueContrib = new Object();

    if(!data.parsedData["post"])
        data.parsedData["post"] = new Array();

    if(data.page.root)
        data.revisions.forEach(function(elemP){

                var post = new Object();
                
                elemP.root.children.forEach(function(elem, index){

                    if(elem.name == "contributor"){ 
                        elem.children.forEach(function(elem){
                            if(elem.name == "username"){
                                post["contributor"] = elem.content;
                                uniqueContrib[ elem.content ] = 1;
                            }
                            if(elem.name == "idContributor"){
                                post["idContributor"] = elem.content;
                            }
                            if(elem.name == "ip"){
                                post["ip"] = elem.content;
                                uniqueContrib[ elem.content ] = 1;
                            }
                        });
                    }
                    
                    if(elem.name == "comment"){
                        post["comment"] = elem.content;
                    }

                    if(elem.name == "id"){
                        post["revisionId"] = elem.content; // console.log(post["revisionId"]);
                    }


                    if(elem.name == "size"){
                        post["size"] = elem.content;
                    }   
                });
                data.parsedData["post"].push(post);

        });

    var distinctUser = new Object();
    data.parsedData.post.forEach(function(elem){
        if(elem.contributor)
            distinctUser[ elem.contributor ];
        else
            distinctUser[ elem.ip ];
    });

    var nbUniqueContrib = 0;
    for(var index in uniqueContrib)
        nbUniqueContrib++;

    data.parsedData.nbUniqueContributor = nbUniqueContrib;

    return data;
}




var firstPrintPrefix = true;

var reg_1 = new RegExp("robot", "i");
var reg_2 = new RegExp("bot", "i");
var reg_3 = new RegExp("automatisée", "i");
var reg_4 = new RegExp("Bot", "i");
var reg_5 = new RegExp("automatique", "i");




var listBots = JSON.parse(fs.readFileSync("./bots.json")).bots;


function isBot(username, comment){

    for(var i=0; i<listBots.length; i++)
        if(listBots[i] == username)
            return true;
    return false;
    //return ( reg_2.test(username) || reg_4.test(username) ) && ( reg_1.test(comment) || reg_2.test(comment) || reg_3.test(comment)  || reg_5.test(comment) );
}

function cleanNote(note){
    return note.replace(/\\/g, "").replace(new RegExp("\"", "g"), "\\\"").replace(new RegExp("\n", "g"), "");
}


function ToRdf_Concat(){
    if(!(this instanceof ToRdf_Concat))
        return new ToRdf_Concat();

    Transform.call(this, { readableObjectMode : true});
    this._decoder = new StringDecoder('utf8');

    this.map = new Object();
    this.arrayDoneUri = new Array();

}








function compileData(data){


    var uri = "http://fr.wikipedia.org/wiki/"+data.parsedData.titleOfWiki;

    var rdf = "";
    if(firstPrintPrefix){
        rdf += "\n\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
        rdf += "@prefix swp: <http://www.w3.org/2004/03/trix/swp-2/> .\n";
        rdf += "@prefix dc: <http://purl.org/dc/element/1.1/> .\n";
        rdf += "@prefix dbfr: <http://ns.inria.fr/dbpediafr/voc#> .\n";
        rdf += "@prefix prov: <http://www.w3.org/ns/prov#> .\n";
        rdf += "@prefix foaf: <http://xmlns.com/foaf/> .\n";
        rdf += "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n";
        rdf += "@prefix sioc: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n";
        rdf += "@prefix scoro: <http://purl.org/spar/scoro/> .\n";
        rdf += "\n\n";
        firstPrintPrefix = false;
    }

    rdf += "<"+uri+">\n";
    rdf += "\t a prov:Revision ;\n";
    rdf += "\t dc:subject <http://fr.dbpedia.org/resource/" + data.parsedData.titleOfWiki + "> ;\n";
    rdf += "\t swp:isVersion \""+data.parsedData.countRevision+"\"^^xsd:integer ;\n";
    rdf += "\t dc:created \""+data.parsedData.startDate+"\"^^xsd:dateTime ;\n";
    rdf += "\t dc:modified \""+data.parsedData.endDate+"\"^^xsd:dateTime ;\n";
    rdf += "\t dbfr:uniqueContributorNb "+data.parsedData.nbUniqueContributor+" ;\n";
    for( var index in data.parsedData.revisionsPerYear ){
        rdf +=  "\t dbfr:revPerYear [ dc:date \""+index+"\"^^xsd:gYear ; rdf:value \""+data.parsedData.revisionsPerYear[ index ]+"\"^^xsd:integer ] ;\n";
    }
    for( var index in data.parsedData.revisionsPerMonth ){
        rdf +=  "\t dbfr:revPerMonth [ dc:date \""+index+"\"^^xsd:gYearMonth ; rdf:value \""+data.parsedData.revisionsPerMonth[ index ]+"\"^^xsd:integer ] ;\n";
    }

    for( var index in data.parsedData.averageSizePerYear ){
        rdf +=  "\t dbfr:averageSizePerYear [ dc:date \""+index+"\"^^xsd:gYear ; rdf:value \""+data.parsedData.averageSizePerYear[ index ]+"\"^^xsd:float ] ;\n";
    }
    for( var index in data.parsedData.averageSizePerMonth ){
        rdf +=  "\t dbfr:averageSizePerMonth [ dc:date \""+index+"\"^^xsd:gYearMonth ; rdf:value \""+data.parsedData.averageSizePerMonth[ index ]+"\"^^xsd:float ] ;\n";
    }
//cbo:pageCount
    if(data.parsedData["post"].length > 0){
        rdf += "\t dbfr:size \""+data.parsedData["post"][ data.parsedData["post"].length-1 ]["size"]+"\"^^xsd:integer ;\n";
        if(data.parsedData["post"][ 0 ].contributor != undefined)
            if( isBot(data.parsedData["post"][ 0 ].contributor, data.parsedData["post"][ 0 ].comment) )
                rdf += "\t dc:creator [ foaf:nick \""+data.parsedData["post"][ 0 ].contributor+"\" ; rdf:type scoro:ComputationalAgent ] ;\n";
            else
                rdf += "\t dc:creator [ foaf:nick \""+data.parsedData["post"][ 0 ].contributor+"\" ] ;\n";
        else if(data.parsedData["post"][ 0 ].ip != undefined)
            rdf += "\t prov:wasAttributedTo [ sioc:ip_address \""+data.parsedData["post"][ 0 ].ip+"\" ] ;\n";

        if(data.parsedData["post"][ data.parsedData["post"].length-1 ].comment != undefined)
            rdf += "\t sioc:note \""+cleanNote( data.parsedData["post"][ data.parsedData["post"].length-1 ].comment )+"\"^^xsd:string ;\n";
        if(data.parsedData["post"].length-2 > 0)
            rdf += "\t prov:wasRevisionOf <https://fr.wikipedia.org/w/index.php?title="+data.parsedData.titleOfWiki+"&oldid="+data.parsedData["post"][ data.parsedData["post"].length-2 ].revisionId+"> ;\n";

        if(data.parsedData["post"][ data.parsedData["post"].length-1  ].contributor != undefined)
            if( isBot(data.parsedData["post"][ data.parsedData["post"].length-1 ].contributor, data.parsedData["post"][ data.parsedData["post"].length-1 ].comment) )
                rdf += "\t prov:wasAttributedTo [ foaf:nick \""+data.parsedData["post"][ data.parsedData["post"].length-1 ].contributor+"\" ; a prov:SoftwareAgent ] .\n";
            else
                rdf += "\t prov:wasAttributedTo [ foaf:nick \""+data.parsedData["post"][ data.parsedData["post"].length-1 ].contributor+"\" ; a  prov:Person, foaf:Person ] .\n";
        else if(data.parsedData["post"][ data.parsedData["post"].length-1  ].ip != undefined)
            rdf += "\t prov:wasAttributedTo [ sioc:ip_address \""+data.parsedData["post"][ data.parsedData["post"].length-1 ].ip+"\"  ; a  prov:Person, foaf:Person] .\n";

        rdf += "\n";
    }
    for(var i=data.parsedData["post"].length-2; i>=0; i--){
        rdf += "<https://fr.wikipedia.org/w/index.php?title="+data.parsedData.titleOfWiki+"&oldid="+data.parsedData["post"][ i ].revisionId+">\n";
        rdf += "\t a prov:Revision ;\n";
        rdf += "\t dc:created \""+data.parsedData.revisionsDate[ i ]+"\"^^xsd:dateTime ;\n";
        rdf += "\t dbfr:size \""+data.parsedData["post"][ i ]["size"]+"\"^^xsd:integer ;\n";

        if(data.parsedData["post"][ i-1 ] != undefined)
            rdf += "\t dbfr:sizeNewDifference \""+( data.parsedData["post"][ i ]["size"] - data.parsedData["post"][ i-1 ]["size"] )+"\"^^xsd:integer ;\n";

        if(data.parsedData["post"][ i ].comment != undefined){
            rdf += "\t sioc:note \""+cleanNote( data.parsedData["post"][ i ].comment )+"\"^^xsd:string ";
        }

        if(data.parsedData["post"][ i ].contributor != undefined){
            if(data.parsedData["post"][ i ].comment != undefined)
                rdf += ";\n";
            if( isBot(data.parsedData["post"][ i ].contributor, data.parsedData["post"][ i ].comment) )
                rdf += "\t prov:wasAttributedTo [ foaf:nick \""+cleanNote(data.parsedData["post"][ i ].contributor)+"\" ; a prov:SoftwareAgent ] ";
            else
                rdf += "\t prov:wasAttributedTo [ foaf:nick \""+cleanNote(data.parsedData["post"][ i ].contributor)+"\" ; a  prov:Person, foaf:Person ] ";

            if( i - 1 >=0 )
                rdf += ";\n";
            else
                rdf += ".\n";

        }else if(data.parsedData["post"][ i ].ip != undefined){
            if(data.parsedData["post"][ i ].comment != undefined)
                rdf += ";\n";
            if( i - 1 >=0 )
                rdf += "\t prov:wasAttributedTo [ foaf:nick \""+data.parsedData["post"][ i ].ip+"\" ] ;\n";
            else
                rdf += "\t prov:wasAttributedTo [ foaf:nick \""+data.parsedData["post"][ i ].ip+"\" ] .\n";
        }else if( !(i - 1 >=0) ){
            if(data.parsedData["post"][ i ].comment != undefined)
                rdf += ".\n";
        }
            
        if( i - 1 >=0 )
            rdf += "\t prov:wasRevisionOf <https://fr.wikipedia.org/w/index.php?title="+data.parsedData.titleOfWiki+"&oldid="+data.parsedData["post"][ i-1 ].revisionId+"> .\n";
        rdf += "\n";
    }

    return rdf;

}

module.exports = RawPageSpliter;