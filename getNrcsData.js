/*
 * This example demonstrates how to fetch a single row from a large table
 * one by one and get processed by application. It avoids allocation of 
 * large memory by the ibm_db and data processing becomes easier.
 *
 * This example has used Sync methods, but async methods also exist.
 */

//need to 
//https://www.cbrfc.noaa.gov/wsup/graph/nrcscompare_data.py?fdate=2022-1-1&area=&prob=cmp&residual=0&returnType=%27csv%27

// def nrcscompare(fdate_str,field,field_label,area,residual):
//     wso=wsocond.wsodata(fdate_str,area,"py",residual)
//     nrcs=nrcscond.nrcsdata(fdate_str)

const { water_year } = require("./utils.js")
var ibmdb = require("ibm_db")
const {dbConn} = require('./config/config')
const csv=require('csvtojson')
const fs = require('fs')
const { wsodata } = require("./wsocondRefactorFewerQueries.js")
const { resolve } = require("path")
const {usgsCbrfcStnInfo} = require('./usgsCbrfcStnInfo.js')
const {FullStationDataFactory, bbObj, keyMapAr, returnKeyAr, allData} = require('./getNrcsStationMetadata').factoryData
const fullStationObject = FullStationDataFactory(allData, bbObj, keyMapAr, returnKeyAr).metadataById()
// full station object is made from the data pulled from the NRCS website. Did it previously with stations downloaded fro the USGS but it didnt have all of the sites

var axios = require("axios")
var axiosRetry = require("axios-retry")
axiosRetry(axios, { retries: 3 });
// var testFn = ({a=3, b=4, c=6} = {}, d)=>{
  // console.log('a', a, 'b', b,'c', c, 'd', d)
// }
// for next time - look at the nrcs periods because some of them have two periods and am i just overwriting the period forecsts. 
async function templ(command, process){
  try{
    const req = await command
  }
  catch(e){
    return e
  }
}
const nrcsNameObj = {
  10:{
    name: 'crx'
  },
  30:{
    name: 'c30'
  },
  50:{
    name: 'cmp'
  },
  70:{
    name: 'c70'
  },
  90:{
    name: 'crn'
  }
}

const csvFn = async(year, month, day)=>{
  try{
    // const csvData = await csv().fromFile(`../nrcs/csv/${year}-${month}-${day}.csv`)
    const localCsvData = await csv().fromFile(`../nrcs/csv/${year}-${month}-${day}a.csv`)
    // console.log('csv scucess 60')
    return {success: localCsvData}
    // return {success: csvData}
  }catch(e){
    // console.log('35 error', e)
    return {error: JSON.stringify(e)}
  }
}
const localNrcsFn = async(year, month, day)=>{
  try{
    const localJsonData = await fs.promises.readFile(`./nrcsJsonData/${year}-${month}-${day}.json`)
    // const localJsonDataBad = await fs.promises.readFile(`./nrcsJsonData/${year}-${month}-${day}a.json`)
    // return {success: localJsonDataBad}
    return {success: localJsonData}
  }catch(e){
    // console.log('35 error', e)
    return {error: JSON.stringify(e)}
  }
}

const remoteNrcsFn = async(year, month, day)=>{
  try{
    const dataUrl = `https://www.nrcs.usda.gov/Internet/WCIS/data/SRVO/MONTHLY/FORECAST/SRVO_MONTHLY_FORECAST_${year}-${month}-${day}.json`
    const axiosData = await axios.get(dataUrl)
    // console.log('286 axios data success')
    return {success: axiosData.data}
  }catch(e){
    // console.log('288 error here', e)
    return {error: JSON.stringify(e)}
  }
}

const reformatNrcsJson = data =>{
  console.log('formatting NRCS Data')
  const nrcsForecastStationKeys = Object.keys(data)
  const cbrfcStates = ['UT', 'WY', 'CO', 'AZ']
  // console.log('95 nrcsForecastStationKeys', nrcsForecastStationKeys)
  const returnObj =Object.create({})
  // const returnAr=[]
  // const stnsInCBRFC = nrcsForecastStationKeys.filter(curr =>{
  //   const usState = curr.split(':')[1]
  //   return cbrfcStates.indexOf(usState) >=0
  // })
  const stnsInCBRFC = nrcsForecastStationKeys.filter(curr =>{
    const idString = curr.split(':')[0]
    // console.log(fullStationObject[idString],usgsCbrfcStnInfo[idString],'110 nrcs' )
    // console.log('id 111', idString)
    if(fullStationObject[idString]&& usgsCbrfcStnInfo[idString]){
      console.log('in both 112', fullStationObject[idString])
      // statonsInBoth.push(idString)
    }
    else if(! fullStationObject[idString]&& usgsCbrfcStnInfo[idString]){
      // console.log('in just your original usgs website map obect 112')
      // stationsInJustOriginalUSGSObj.push(idString)
    }
    else if(fullStationObject[idString]&& !usgsCbrfcStnInfo[idString]){
      // console.log('in just the object from the NRCS website object 112')
      // stationsJustInNewNRCSObj.push(idString)
    }
    else{
      // console.log('in neithe robject 121')
      // stationsInNeither.push(idString)
    }
    return usgsCbrfcStnInfo[idString]
  })

  const missingMetaeat = []
  stnsInCBRFC.map(curr=>{
    const dataAr = []
    const idString = curr.split(':')[0]
    // const seedObj = {metadata: usgsCbrfcStnInfo[idString]}
    const stationObject = {metadata: usgsCbrfcStnInfo[idString]}
    const dataObj = Object.create({})
    // console.log('in new object 109', stationObject)
    if(!usgsCbrfcStnInfo[idString]){
      // console.log('not in', curr)
      missingMetaeat.push(idString)
    }
    // const usState = curr.split(':')[1]
    // console.log('usState', usState)
    // console.log('curr', curr)
    const idData = data[curr]
    // console.log('idData', idData)
    for(const period in idData){
      const periodData = idData[period]
      const periodReturn = Object.create({})
      for(var i = 0; i<periodData.length; i+=2){
        // console.log(periodData[i], periodData[i+1])
        const keyName = nrcsNameObj[periodData[i]] ? nrcsNameObj[periodData[i]].name : periodData[i]
        // console.log('keyName', keyName)
        periodReturn[keyName]=periodData[i+1]
      }

      dataObj[period]=periodReturn
      dataAr.push({period, data:idData[period]})
    }
    // console.log('idData', idData)
    // console.log('data obj for', idString, dataObj)
    stationObject.forecastData = dataObj
    returnObj[idString]=stationObject
    // returnObj[idString]=dataAr
    // console.log(curr, idW)
    // return curr.split(':')[0]
  })

  // console.log(JSON.stringify(returnObj))
  // console.log('spot check 149', returnObj["13174500"])
  // console.log('missing metadata 142', missingMetaeat.length)
  return returnObj

}

const getNrcsData = async function getNrcsData(year, month, day){
  let localCsv
  console.log('in nrcs data dates request', year, month, day)
  // try{
  //   localCsv = await csvFn(year, month, day)
  //   // console.log('localcsv 165', localCsv)
  // }
  // catch(e){
  //   console.log('167 error', e)
  // }
  // if(localCsv.success){
  //   console.log('i got my csv file and will do stuff here 172')
  //   return reformatLocalCsv(localCsv.success)
  // }
  // else{
  let localJson
  try{
    console.log('looking for local NRCS json')
    localJson = await localNrcsFn(2024, month, day)
    // console.log('localJson 165', localJson)
  }
  catch(e){
    console.log('179 local NRCS Json error', e)
  }
  if(localJson.success1){
    // const reformattedLocalJson = reformatNrcsJson(JSON.parse(localJson.success.toString()))
    // console.log('i got my local json file and will do things with it 144', JSON.parse(localJson.success.toString()))
    console.log('succesfully  retrieved local JSON')
    return {data: JSON.parse(localJson.success.toString())}
  }
  else{
    let remoteJson
    console.log('no local NRCS JSON, requesting remote JSON')
    try{
      remoteJson = await remoteNrcsFn(year, month, day)
      // console.log('localJson 165', remoteJson)
    }
    catch(e){
      console.log('remote JSON reqeust error', e)
    }
    if(remoteJson.success){
      console.log('success requesting remote JSON')
      const reformattedRemoteJson = reformatNrcsJson(remoteJson.success)
      //save to local file here
      // console.log('add piece to save to local file 165')
      try{
        console.log('saving JSON')
        fs.writeFileSync(`./nrcsJsonData/${year}-${month}-${day}.json`,JSON.stringify(reformattedRemoteJson))
      }
      catch(e){
        console.log('error saving to local json', e)
      }
      return {data: reformattedRemoteJson}

      // console.log('i got my remote json and will do things with it 161', reformattedRemoteJson)
    }
  }
  // }

}
// getNrcsData(2022,1,1)
exports.getNrcsData = getNrcsData

function reformatLocalCsv(input){
  const returnObj = {}
  for(var dataPeriod in input){
    const currData = input[dataPeriod]
    const {id8, state, agency, bmon, emon, p1, p2, p3, p4, p5} = currData
    const periodString = `${bmon}-${emon}`
    const currDataAr = [p1, p2, p3, p4, p5]
    const currDataObj = {}
    // console.log('curr data ar 180', currDataAr)
    currDataAr.map(curr=>{
      // console.log('curr 181', curr)
      if(curr){
        const idString = curr.split(':')[0]
        const currName = nrcsNameObj[idString] ? nrcsNameObj[idString]['name'] : null
        const currVal = curr.split(':')[1] ? parseFloat(curr.split(':')[1]) : null
        currDataObj[currName] = currVal
      }
    })
    // console.log('currDataAr 178', currDataAr, currDataObj)
    if(returnObj[id8]){
      if(returnObj[id8][periodString]){
        console.log('wierd there is already data for this period and station')
      }
      else{
        returnObj[id8][periodString] = {...currDataObj}
      }
    }
    else{
      returnObj[id8]= {[periodString] : {...currDataObj}}
    }
  }
  // console.log('returObj 203', returnObj)
  return {data:returnObj}

}

// nrcsJson.map(currDat=>{
  //   const {id8, bmon, emon, p1, p2, p3, p4, p5} = currDat
  //   // add a W to the beginning so that it mateches the alias scheme in the database
  //   const idWa = 'W' + id8.substring(1)
  //   const bmonNum = monthNameToNum(bmon)
  //   const emonNum = monthNameToNum(emon)
  //   // console.log(' 84 emonNum', emonNum, emon, bmon, 'bmonNum', bmonNum)

  //   //get the ID from alias ID map
  //   const wsoIda = nrcsIdKeyMap.get(idWa)
  //   if(wsoIda && wsoDat[wsoIda]){
  //     const wsoStnDat = wsoDat[wsoIda]['data']
  //     wsoStnDat.map(curr=>{
  //       // console.log('91', curr.bper, curr.eper, bmonNum, emonNum)
  //       if(curr.bper - 3 === bmonNum && curr.eper - 3 === emonNum){
  //         // console.log('96 bmonth matches')
  //         // console.log('97 curr wso data', wsoDat[wsoIda])
  //         // console.log('98 currnrcs data', currDat)
  //       }
  //     })
  //     // console.log('wsoId 56', wsoIda, bmon, emon)
  //     // console.log(wsoDat[wsoIda])
  //   }
  // })

const nrcsIdToWso = async() =>{

}
// '13162225': {
//   'APR-JUL': { crx: 32000, c30: 27000, cmp: 23000, c70: 20000, crn: 15200 },
//   'APR-SEP': { crx: 33000, c30: 28000, cmp: 24000, c70: 21000, crn: 15900 }
// },

// '13162225': {
//   'APR-JUL': { crx: 32000, c30: 27000, cmp: 23000, c70: 20000, crn: 15200 },
//   'APR-SEP': { crx: 33000, c30: 28000, cmp: 24000, c70: 21000, crn: 15900 }
// },



// const nrcsNameObj = {
//   10:{
//     name: 'crx'
//   },
//   30:{
//     name: 'c30'
//   },
//   50:{
//     name: 'cmp'
//   },
//   70:{
//     name: 'c70'
//   },
//   90:{
//     name: 'crn'
//   }
// }
// const reformatNrcsJson = data =>{
//   const nrcsKeys = Object.keys(data)
//   const returnObj ={}
//   const returnAr=[]
//   nrcsKeys.map(curr=>{
//     const dataAr = []
//     const dataObj = {}
//     const idString = curr.split(':')[0]
//     const idData = data[curr]
//     for(const period in idData){
//       const periodReturn = {}
//       const periodData = idData[period]
//       for(var i = 0; i<periodData.length; i+=2){
//         // console.log(periodData[i], periodData[i+1])
//         const keyName = nrcsNameObj[periodData[i]] ? nrcsNameObj[periodData[i]].name : periodData[i]
//         periodReturn[keyName]=periodData[i+1]
//       }

//       dataObj[period]=periodReturn
//       dataAr.push({period, data:idData[period]})
//     }
//     // console.log('idData', idData)
//     returnObj[idString]=dataObj
//     // returnObj[idString]=dataAr
//     // console.log(curr, idW)
//     // return curr.split(':')[0]
//   })

//   // console.log(JSON.stringify(returnObj))
//   return returnObj

// }




// function csvPromise(year, month, day){
//   console.log('here 69')
//   return new Promise((resolve, reject)=>{
//     const localDat = csv().fromFile(`../nrcs/csv/${year}-${month}-${day}a.csv`)
//     // console.log('localData', localDat)
//     if(localDat){
//       resolve(localDat)
//     }
//     else{
//       reject(err)
//     }
//   }).catch(er => er)
// }

// function remoteNrcsPromise(year, month, day){
//   return axios.get(`https://www.nrcs.usda.gov/Internet/WCIS/data/SRVO/MONTHLY/FORECAST/SRVO_MONTHLY_FORECAST_${year}-${month}-${day}.json`)
// }

// function localNrcsPromise(year, month, day){
//   return new Promise((resolve, reject)=>{
//     const localDat = fs.promises.readFile(`./nrcsJsonData/${year}-${month}-${day}8.json`)
//     if(localDat.data){
//       resolve(localDat.data)
//     }
//     else{
//       reject('err')
//     }
//   }).catch(er => er)
// }
// const commandObj = {
//   localCsv:{
//     command: 'csv().fromFile(`../nrcs/csv/${year}-${month}-${day}a.csv`)',
    // onSuccess: ()=>console.log('success in localCsv'),
    // onFail: ()=>console.log('failure in localCsv')
//   },
//   localNrcs:{
//     command: 'fs.promises.readFile(`./nrcsJsonData/${year}-${month}-${day}8.json`)',
    // onSuccess: ()=>console.log('success in localNrcs'),
    // onFail: ()=>console.log('failure in localNrcs')
//   },
//   remoteNrcs:{
//     command: 'axios.get(`https://www.nrcs.usda.gov/Internet/WCIS/data/SRVO/MONTHLY/FORECAST/SRVO_MONTHLY_FORECAST_${year}-${month}-${day}.json`)',
    // onSuccess: ()=>console.log('success in localCsv'),
    // onFail: ()=>console.log('failure in localCsv')
//   }

// }
// const codeTest = async (year, month, day) =>{
//   try{
//     // await csvFn(year, month, day)
//     // const raceProm = await Promise.allSettled([
//     //   new Promise(resolve=>resolve(csvFn(year, month, day)),
//     //   new Promise(resolve=>resolve(localNrcsFn(year, month, day)),
//     // ])
//     // const raceProm = await Promise.race([
//     //   new Promise((resolve, reject)=>{
//     //     const datRet = await csvFn(year, month, day)
//     //     if(d)
//     //   }),
//     //   new Promise((resolve, reject)=>{
//     //     resolve(remoteNrcsFn(year, month, day))
//     //   }),
//     //   new Promise((resolve, reject)=>{
//     //     resolve(localNrcsFn(year, month, day))
//     //   })
//     // ]).then(value=>console.log('value', value))
//     // console.log('raceProm', raceProm)
//     console.log('here')
//     const testt = await Promise.all(
//       [
//         csvPromise(year, month, day),
//         remoteNrcsPromise(year, month, day),
//         localNrcsPromise(year, month, day)
//       ]
//     ).catch(e=>console.log('promise error 143', e))
//     console.log('testt', testt)
//   }
//   catch(e){
//     // console.log('e', e)
//   }
// }

// codeTest(2022,1,1)
// const allSettled = await Promise.allSettled(
//   stnsToQuery.map(currStn=>{

//     return new Promise((resolve, reject)=>{
//       resolve(buildStationDataObject(currStn, reqYear, reqMonth, reqDay))
//     })
//   })
// )
// console.log('allSettled', allSettled)