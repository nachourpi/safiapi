var express = require('express')
var admin = require('firebase-admin')
var env = require('./env.json')
const {readFile}  = require('fs').promises
const {promisify} = require('util')
const parse       = promisify(require('csv-parse'))



const collectionName = "machine_data_crono"

//Initialize Firestore
admin.initializeApp(env);
const db = admin.firestore();


/* STATES:

Off is power off, no data. 
On - unloaded is power on, no current (or essentially no current, it could be 0.1A or something). 
On - idle is less than 20% of operating load. 
On - loaded is 20-100+% of operating load. 

*/

const STATES = {
    OFF:1,
    UNLOADED:2,
    IDLE: 3,
    LOADED: 4
}

//Some constant limits that could be eventually set by API request :
const minThreshold = 0.60
const operatingLoad = 100 // operatinLoad * .2 to be considerated IDLE...

//Expected Min data duration between each record in seconds
const dataMinPeriod = 30

const writeToFirestore = (records)=>{
    const batchCommits = [];
    console.log("INIT RECORDING: ",records.length)
    let batch = db.batch();
    //let j =1

    records.forEach((record, i) => {
      
      var docRef = db.collection(collectionName).doc();
      
     batch.set(docRef,record)

      if ((i + 1) % 500 === 0) {
        batchCommits.push(batch.commit());
        batch = db.batch();
      }
    });
    batchCommits.push(batch.commit());
    console.log("BATCH COMMITS:",batchCommits.length)
    return Promise.all(batchCommits);
  }

const importCsv = async(csvFileName)=>{
    const fileContents = await readFile(csvFileName, 'utf8')
    let records = await parse(fileContents, { columns: true })

    console.log("PARSED :",records.length)


    //For the purpose of this example we are only considering 1 metric
    records = records.filter(v=>v.metricid=="Iavg_A")

    console.log("Records after filterind Iavg_A: ",records.length)

    //Group records by timestamp
    let cronoData = []

    records.forEach((record) => {
  
        if(cronoData[record.timestamp]){
            //To the purpose of this example we don't take into consideration posible
            //data problems were there are 2 or morerecords of the same metricic for the same timestamp
            return;
        }

        cronoData[record.timestamp]={
            deviceid:record.deviceid,
            timestampValue:parseInt(record.timestamp),
            date:new Date(parseInt(record.timestamp)),
            timestamp:admin.firestore.Timestamp.fromDate(new Date(parseInt(record.timestamp))),
            metricid:record.metricid,
            value:parseFloat(record.calcvalue)
        }
    })

    
    //Convert cronoData "associative-array/object" into Array
    cronoData = Object.values(cronoData)
    console.log("CronoData length: ",cronoData.length)

    //Order the array by timestamp ASC
    cronoData.sort((a, b) => a.timestampValue - b.timestampValue)
    
    //Here we should add OFF-records considering dataDuration if there are gaps with no-data
    let cronoDataWithGaps = []

    cronoData.forEach((record,i)=>{

        let timeDifference = i==0?0:(record.date.getTime() - cronoData[i-1].date.getTime()) / 1000
        //There should be a an "OFF record" here because there's no data
        if(timeDifference > dataMinPeriod){

            let newTimestamp = dataMinPeriod*1000 + cronoData[i-1].date.getTime()

            //We add an OFF record, machine was powered off so no-data 
            cronoDataWithGaps.push({
                deviceid:record.deviceid,
                value:-1,
                state:STATES.OFF,
                timestampValue:newTimestamp,
                date:new Date(newTimestamp),
                timestamp:admin.firestore.Timestamp.fromDate(new Date(newTimestamp))
            })
        }

        //STATES.OFF will be if there's no data
        if(record.value<=minThreshold){
            record.state=STATES.UNLOADED
        }else if(record.value<=operatingLoad*.2){
            record.state=STATES.IDLE
        }else{
            record.state=STATES.LOADED
        }


        cronoDataWithGaps.push(record)
    })

    console.log("cronoDataWithGaps length: ",cronoDataWithGaps.length)
   
    //Reduce data merging consecutives records with the same STATE adding duration attribute
    //NOTE: THIS WAS IF WE WANT TO PRE-PROCESS RAW DATA
    /*
    cronoDataWithGaps = cronoDataWithGaps.reduce((p,c,i)=>{
        if(p.length==0 || c.state!=p[p.length-1].state){
            c.duration=dataMinPeriod
            p.push(c)
        }else{
            p[p.length-1].duration+=dataMinPeriod
        }

        return p
    },[])
    console.log("cronoDataWithGaps reduced: ",cronoDataWithGaps.length)
    */
   


    try {
      await writeToFirestore(cronoDataWithGaps)
    }
    catch (e) {
      console.error(e)
      return e
    }
    console.log(`Wrote ${cronoDataWithGaps.length} records`)
  }
  
  

var app = express();
//CORS
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Authorization, X-API-KEY, Origin, X-Requested-With, Content-Type, Accept, Access-Control-Allow-Request-Method');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
    res.header('Allow', 'GET, POST, OPTIONS, PUT, DELETE');
    next();
});





app.get('/readData', async (req, res)=>{

    let querySnapshot = await db.collection(collectionName).where("metricid", "==", "Iavg_A").get()

    console.log(querySnapshot.getData().length)
    res.send(querySnapshot.getData().length)
});

app.get('/parseData', async (req, res)=>{

    console.log("Parsing data")
    let result 
    try{
        result = await importCsv("demoCompressorWeekData.csv")
    }catch(e){
        result = e
    }
    

    res.send(result)
});

app.listen(5001,  ()=>{
  console.log('Example app listening on port 5001!')

  console.log("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥\n\n\n\n\n\n\n\n\n\n\n\n\n")
});
