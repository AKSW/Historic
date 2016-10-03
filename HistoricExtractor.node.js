/*
Fabien Gandon, Raphael Boyer, Olivier Corby, Alexandre Monnin. Wikipedia editing history in DBpedia : extracting and publishing the encyclopedia editing activity as linked data. IEEE/WIC/ACM International Joint Conference on Web Intelligence (WI' 16), Oct 2016, Omaha, United States. <hal-01359575>
https://hal.inria.fr/hal-01359575

Fabien Gandon, Raphael Boyer, Olivier Corby, Alexandre Monnin. Materializing the editing history of Wikipedia as linked data in DBpedia. ISWC 2016 - 15th International Semantic Web Conference, Oct 2016, Kobe, Japan. <http://iswc2016.semanticweb.org/>. <hal-01359583>
https://hal.inria.fr/hal-01359583
*/

var fs = require('fs');
var bz2 = require('unbzip2-stream');
var PassThrough = require('stream').PassThrough;
var MongoClient = require('mongodb').MongoClient;
var util = require('util');
var Readable = require('stream').Readable;
var spawn = require('child_process').spawn;

var RawPageSpliter = require('./RawPageSpliter.node.js');
var url = 'mongodb://localhost:27000/dbExtractorHisto';


var main = null;

util.inherits(SourceWrapper, Readable);
function SourceWrapper(){
	Readable.call(this, {});
}
SourceWrapper.prototype._read = function(size){

};

function MasterNode(path, fileArray, db){
	this.bytesReaded = 0;
	this.bytesReadedDecomp = 0;
	this.tempBytesReadedDecomp = 0;

	this.path = path;
	this.fileArray = fileArray;
	this.fileIndex = 0;
	this.fileName = fileArray[this.fileIndex];

	this.sizeFile = null;
	this.watcher = null;
	this.passCount = null;
	this.passCountDecomp = null;
	this.sockPrepare = null;


	
	this.db = db;

	this.lastTimeS = 0;
}

MasterNode.instance = null;

MasterNode.prototype.start = function(){
	MasterNode.instance = this;

	console.log("MasterNode Started");
	var self = this;

	this.interval = setInterval(MasterNode.showInfo, 2000);

	self.readNextFile();
}
MasterNode.prototype.readNextFile = function(){
	var self = this;
	this.bytesReaded = 0;
	this.bytesReadedDecomp = 0;

	
	this.fileName = this.fileArray[this.fileIndex];
	
	var collection = this.db.collection('fileSplit');

	function fileCheck(err, docs){
		if(docs.length != 0){
			self.fileIndex++;
			self.fileName = self.fileArray[self.fileIndex];
			collection.find({"name": self.fileName}).toArray(fileCheck);

		}else{
			if(self.fileName == undefined){
				console.log("No file available    or drop the 'filesplit' document of mongodb.");
				clearInterval(self.interval);
				setTimeout(process.exit, 10000);
				return;
			}
			console.log("File Extraction : "+self.fileName);

			collection.insert({"name": self.fileName});
			self.splitFile();
		}
	}
	collection.find({"name": self.fileName}).toArray(fileCheck);

}

MasterNode.prototype.splitFile = function(){
	var self = this;
 
	this.sizeFile = fs.statSync(this.path + "/" + this.fileName )['size'];
	this.watcher = require('time-calc')();
	this.watcher.viewer(function(diff, repl){return diff;});

	if(this.spliter == undefined)
		this.spliter = new RawPageSpliter(this.db);

	this.spliter.setIndexRule(self.fileIndex, self.fileArray.length, self.fileName);
	if(!this.spliter.isRunning)
		this.spliter.start();
					
	var child = spawn('bzcat', [this.path + "/" + this.fileName]);
	child.stdout.on('data', 
	    function (data) {
	    	self.bytesReadedDecomp += data.length;
	    	self.tempBytesReadedDecomp += data.length;
	        self.spliter.push(data);
	    }
	);
	child.stdout.on('close', 
	    function (code) {
	    	console.log("Wait 2min for the next file. "+code);
	    	self.spliter.stop();
	        //MasterNode.instance.readNextFile();
	        setTimeout(function(){
	        	MasterNode.instance.readNextFile();
	        }, 60000*2);
	        //console.log("Relancer pour le prochain fichier.");
	        //setTimeout(process.exit, 10000);
	    }
	);
	
}
MasterNode.showInfo = function(){
	var self = MasterNode.instance;
	if(self.sizeFile == null || self.watcher == null)
		return;


	var moTotal = parseInt(self.sizeFile/(1024*1024));
	var timeS = self.watcher({enable:{s:false, m:false, h:false, D:false, M:false, Y:false}})/1000



	var moReel = parseInt(self.bytesReadedDecomp/(1024*1024));
	var moActualPerSec = parseInt(self.tempBytesReadedDecomp/(1024*1024)/(timeS - self.lastTimeS));
	var moMoyenPerSec = parseInt(self.bytesReadedDecomp/(1024*1024)/timeS);

	var pagePerSec = parseInt( (self.spliter.pageCountRel/(timeS - self.lastTimeS))*10 )/10;
	var revisionPerSec = parseInt( (self.spliter.revisionCountRel/(timeS - self.lastTimeS))*10 )/10;


	console.log(moTotal+"Mo ---- ( "+moReel+" Mo reel --- "+moActualPerSec+" Mo/s - - Average : "+moMoyenPerSec+" Mo/s ) - - - "+
			pagePerSec+" page/s  -----  "+revisionPerSec+" revision/s");

	self.tempBytesReadedDecomp = 0;
	self.spliter.pageCountRel = 0;
	self.spliter.revisionCountRel = 0;
	self.lastTimeS = timeS;
}

var arrayFiles = fs.readdirSync("./dump");

MongoClient.connect(url, function(err, db){
	if(err)
		console.log(err);

	main = new MasterNode("./dump", arrayFiles, db);
	main.start();

});
