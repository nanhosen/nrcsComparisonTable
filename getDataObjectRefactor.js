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
var axios = require("axios")
var axiosRetry = require("axios-retry")
const fs = require('fs')
const { wsodata } = require("./wsocondRefactorFewerQueries.js")
// const { getNrcsData } = require("./getNrcsData.js")
const { getNrcsData } = require("./NrcsDataFactory.js")
const {FullStationDataFactory, bbObj, keyMapAr, returnKeyAr, allData} = require('./getNrcsStationMetadata').factoryData
const fullStationObject = FullStationDataFactory(allData, bbObj, keyMapAr, returnKeyAr).metadataById()
// console.log(fullStationObject)
axiosRetry(axios, { retries: 3 });
// var testFn = ({a=3, b=4, c=6} = {}, d)=>{
//   console.log('a', a, 'b', b,'c', c, 'd', d)
// }
// for next time - look at the nrcs periods because some of them have two periods and am i just overwriting the period forecsts. 
function getSearchString(riverName, locStrings){
  let searchString
  for(const index in locStrings){
    const currSearch = locStrings[index]
    if(riverName.search(currSearch) >= 0 ){
      searchString = currSearch
      return searchString
    }
  }
}
const getDataObject = async({field = 'cmp', year = new Date().getFullYear(), month = new Date().getMonth()+1, day=1, fdate='LATEST'}={}, area)=>{
    const newReturnObj = {}
    // console.log('here')
    try{
      // console.time('full')
      // const { wsodata } = require("./wsocondRefactor.js")
      const conn = new ibmdb.Database()
      conn.openSync(dbConn);


      
      //get nrcs alias ids and turn into a map object. Will use this to get wsoId of nrcs stations -> our ID
      const queryText = "select * from alias_id"
      const stnAliasData = await conn.query(queryText)
      const nrcsIdKeyMap = new Map()
      stnAliasData.map(curr=>{
        if(curr.ag && curr.ag.search('NRCS') >=0 ){
          nrcsIdKeyMap.set(curr.id8, curr.id)
        }
      })



      
      console.log('68 input dates', year, month, day)
      // console.time('wso')
      //get wso data from db and process. Comes back like this:
      // "YASC2":{"errors":["multiple data for stnWssData","multiple data for stnWspData"],"data":[{"ped":"QCMRZZZ","bper":7,"eper":10,"cmp":27,"crx":38,"crn":19.5,"c30":31,"c70":23,"pavg":100,"pmed":103.84615384615385,"iavg":4,"imed":5}],"metadata":{"riverLocation":" STAGECOACH RSVR; ABV","riverName":"YAMPA ","state":"CO","id":"YASC2","basin":"GN","subbasin":"YW"}},"YDLC2":{"errors":["multiple data for stnWspData"],"data":[{"ped":"QCMPAZZ","bper":7,"eper":10,"cmp":1330,"crx":1850,"crn":800,"c30":1570,"c70":1010,"pavg":111.76470588235294,"pmed":119.81981981981981,"iavg":6,"imed":6}],"metadata":{"riverLocation":" DEERLODGE PARK","riverName":"YAMPA ","state":"CO","id":"YDLC2","basin":"GN","subbasin":"YW"}},"YLLU1":{"errors":["multiple data for stnWspData"],"data":[{"ped":"QCMRZZZ","bper":7,"eper":10,"cmp":68,"crx":102,"crn":45,"c30":82,"c70":58,"pavg":113.33333333333333,"pmed":121.42857142857142,"iavg":6,"imed":6}],"metadata":{"riverLocation":" ALTONAH; NR ","riverName":"YELLOWSTONE ","state":"UT","id":"YLLU1","basin":"GN","subbasin":"DC"}},"ZUIN5":{"errors":["multiple data for stnWspData"],"data":[{"ped":"QCMRZZZ","bper":4,"eper":8,"cmp":0.2,"crx":6.2,"crn":0.01,"c30":0.53,"c70":0.06,"pavg":15.151515151515152,"pmed":153.84615384615387,"iavg":0,"imed":8}],"metadata":{"riverLocation":" BLACK ROCK RES; ABV","riverName":"ZUNI ","state":"NM","id":"ZUIN5","basin":"LC","subbasin":"LC"}}}
      const wsoDat = await wsodata({year, month, day})
      // const wsoDat = await wsodata(`${year}-${month}-${day}`)
      console.log('got wso data now processing')
      // console.log('wsodata', wsoDat, 'woso dat 75')
      // console.timeEnd('wso')


      //master object where all data will be returned
      const fullDataObject = {fDate:`${year}-${month}-${day}`}
      //Get NRCS data. returned nrcs data looks like this:
      // "08219500":{"APR-SEP":{"crx":205000,"c30":169000,"cmp":146000,"c70":124000,"crn":95000}},"08220000":{"APR-SEP":{"crx":740000,"c30":595000,"cmp":505000,"c70":425000,"crn":320000}},"08227000":{"APR-SEP":{"crx":39000,"c30":27000,"cmp":21000,"c70":14900,"crn":8200}},"08236000":{"APR-SEP":{"crx":112000,"c30":92000,"cmp":79000,"c70":68000,"crn":52000}},"08240500":{"APR-SEP":{"crx":8800,"c30":5600,"cmp":3800,"c70":2400,"crn":870}},"08241500":{"APR-SEP":{"crx":10400,"c30":5200,"cmp":2600,"c70":960,"crn":0}},"08242500":{"APR-SEP":{"crx":9000,"c30":5800,"cmp":4000,"c70":2600,"crn":1050}},"08246500":{"APR-SEP":{"crx":320000,"c30":265000,"cmp":230000,"c70":195000,"crn":152000}},"08247500":{"APR-SEP":{"crx":30000,"c30":22000,"cmp":16900,"c70":12600,"crn":7400}},"08248000":{"APR-SEP":{"crx":126000,"c30":99000,"cmp":83000,"c70":68000,"crn":49000}},"08250000":{"APR-SEP":{"crx":14100,"c30":9000,"cmp":6200,"c70":3900,"crn":1490}},"09384000":{"JAN-JUN":{"crx":14900,"c30":9900,"cmp":7300,"c70":5200,"crn":2900}},"BOWLLAKE":{"MAR-MAY":{"crx":3700,"c30":2400,"cmp":1600,"c70":990,"crn":360}},"CTOMWASH":{"MAR-MAY":{"crx":13200,"c30":4900,"cmp":1920,"c70":490,"crn":0}},"WHEATFLD":{"MAR-MAY":{"crx":6500,"c30":3900,"cmp":2500,"c70":1410,"crn":370}},"08477110":{"JAN-MAY":{"crx":6600,"c30":3100,"cmp":1440,"c70":420,"crn":300}},
      // console.log('running nrcs data fnctions')
      const  nrcsDataFunctions =await getNrcsData(year, month, day, {testing: 'true'}); 
      // console.log('running nrcs data fnctions 84 get raw filtered data')
      // console.log('nrcsDataFunctions', nrcsDataFunctions instanceof Error, nrcsDataFunctions.message, '85')
      const nrcsData = nrcsDataFunctions instanceof Error ? undefined : await nrcsDataFunctions.getRawFilteredData()
      // console.log('running nrcs data fnctions 84 get raw filtered etadata')
      // console.log('nrcsData', nrcsData, 'nrcsData 85')
      if(nrcsData){
        const nrcsMetadata = await nrcsDataFunctions.getFilteredMetadata(nrcsData)
        const nrcsAverageData = await nrcsDataFunctions.getFilteredNormals('average')
        const nrcsMedianData = await nrcsDataFunctions.getFilteredNormals('median')
        // console.log('avearage 90', nrcsAverageRequest)
        // console.log('median 90', nrcsMedianRequest)
        // const nrcsNormalData = JSON.parse(nrcsNormalsRequest)
        // console.log(nrcsNormals, 'asdfasdf78 89')
        // console.log('nrcsMetadata 70', nrcsMetadata, '83')
        const nrcsFormatted = {...nrcsData}

        if(!nrcsFormatted){
          console.log('no NRCS data 134 get object refactor')
          newReturnObj.errors[push('no NRCS data')]
        }
        else{
          //data is here. Going through to add wso ID for NRCS station and 
          //find corresponding wso data
          
          const nrcsStnAr = []
          // console.log('nrcsFormatted', nrcsFormatted)
          for(const nrcsStn in nrcsFormatted){
            const nrcsStnData = nrcsFormatted[nrcsStn]
            // add a W to the beginning so that it mateches the alias scheme in the database
            // console.log(' i am in your new shiny object 99', fullStationObject[nrcsStn])
            const idW = 'W' + nrcsStn.substring(1)
            //get the ID from alias ID map
            const wsoId = nrcsIdKeyMap.get(idW)
            var metadata = wsoDat[wsoId] ? wsoDat[wsoId]['metadata'] : undefined
            if(!metadata){
              // console.log('nrcs metatdatastn', nrcsMetadata[nrcsStn])
              const {name: riverName, state: ST} = nrcsMetadata[nrcsStn]? nrcsMetadata[nrcsStn]: {}
              const regRiver = /RIVER/
              const locStrings = ['at','ab', 'nr', 'bl', 'below', 'above', 'near', 'abv']
              const searchString = getSearchString(riverName, locStrings)
              // console.log('rivername 106', riverName)
              // console.log('searchString 106', riverName.split(searchString)[0])  
              metadata = {riverLocation: riverName, riverName: riverName.split(searchString)[0], state: ST.toUpperCase(), id: nrcsStn}
              // console.log('meta', meta)
              // const idString = curr.split(':')[0]

            }
            // if(metadata){
            //   console.log('metatata', metadata)
            // }
            // metatata {
            //   riverLocation: ' CLIFTON ',
            //   riverName: 'SAN FRANCISCO ',
            //   state: 'AZ',
            //   id: 'SFCA3',
            //   basin: 'LC',
            //   subbasin: 'GL'
            // }
            // in new object 109 {
            //   STANAME: 'BEAR RIVER ABOVE RESERVOIR, NEAR WOODRUFF, UT',
            //   ST: 'wy',
            //   HUC: '16010101'
//             // }
//             let str = "hey JudE"
// let re = /[A-Z]/g
// let reDot = /[.]/g
// console.log(str.search(re)) 
            
            

  
            //loop through NRCS station data to get the period data. Checks to see if 
            //there is wso data that corresponds to the station and period. If it exists, then
            //calcs diff and diff percent. If doesnt exist then just puts the NRCS data on the master object
            // console.log('88 nrcsStnData', nrcsStnData, nrcsStn)
            // const nrcsStnData = nrcsStnData.forecastData
            if(nrcsStnData){
              for(const period in nrcsStnData){
                const nrcsPeriodData = nrcsStnData[period]
                
                const stnNrcsData = nrcsPeriodData && nrcsPeriodData[field] ? nrcsPeriodData[field]/1000 : null
                const stnNrcsAverage = nrcsAverageData[nrcsStn] && nrcsAverageData[nrcsStn][period] ? nrcsAverageData[nrcsStn][period]/1000 : undefined
                const stnNrcsMedian = nrcsMedianData[nrcsStn] && nrcsMedianData[nrcsStn][period] ? nrcsMedianData[nrcsStn][period]/1000 : undefined
                // const {statType} = nrcsNormalData && nrcsNormalData[nrcsStn] ? nrcsNormalData[nrcsStn] : {}
                const percentOfAverage = stnNrcsAverage && stnNrcsAverage >0 ? (stnNrcsData/stnNrcsAverage)*100: undefined
                const percentOfMedian = stnNrcsMedian && stnNrcsMedian >0 ? (stnNrcsData/stnNrcsMedian)*100 : undefined
                // const nrcsAverages = stnNrcsNormal ? {nrcsNormal: stnNrcsNormal, percentOfNormal, statType} : undefined
                const nrcsNormals = {pavg: percentOfAverage, pmed: percentOfMedian, avg30: stnNrcsAverage, med30: stnNrcsMedian}
                // console.log('nrcsClimo 164', 'ob', nrcsNormals)
                const numericPeriod = getPeriodNumberString(period, nrcsStnData)
                const nrcsBper = monthNameToNum(period.split('-')[0])
                const nrcsEper = monthNameToNum(period.split('-')[1])
                if(!numericPeriod || !stnNrcsData){
                  if(!numericPeriod){
                    // console.log('103 numeric period issue here', numericPeriod, period, nrcsStnData, wsoId)
                    if(newReturnObj[wsoId ? wsoId : nrcsStn] && newReturnObj[wsoId ? wsoId : nrcsStn].errors){
                      newReturnObj[wsoId ? wsoId : nrcsStn].errors.push(`numeric period issue period:${period}`)
                    }
                    else{
                      newReturnObj[wsoId ? wsoId : nrcsStn]={errors:[`numeric period issue period:${period}`]}
                    }
                  }
                }
                else{ //has both numeric period and stn nrcs data
                  let stnWsoData
                  if(wsoDat[wsoId] && wsoDat[wsoId].data){
                    //if wao data maps, then find wso data that corresponds to station. 
                    //wso data is an array with an array entry for each period
                    wsoDat[wsoId].data.map(currWsoData=>{
                      //checks to see if the current data period of the wso data matches the current nrcs period
                      if(currWsoData.bper -3 === nrcsBper && currWsoData.eper -3 === nrcsEper){
                        //if the period and ID match then calculate the stats for the selected field
                        // console.log('currwso data 122', currWsoData)
                        const nrcsFieldData = nrcsPeriodData[field] ?  nrcsPeriodData[field]/1000 : null
                        const wsoFieldData = currWsoData[field] 
                        const{ pavg, pmed, avg30, med30} = currWsoData
                        const diff = wsoFieldData ? nrcsFieldData - wsoFieldData : null
                        const diffPct = wsoFieldData && wsoFieldData >0 ? nrcsFieldData/wsoFieldData * 100 : null
                        const dataToAdd = {period, field, nrcsStn, nrcsData: nrcsFieldData, rfcData: wsoFieldData, diff, diffPct, metadata, rfcNormals: {pavg, pmed, avg30, med30}, nrcsNormals}
                        fullDataObject[wsoId]= fullDataObject[wsoId] && typeof fullDataObject[wsoId] === 'object'
                          ? fullDataObject[wsoId].push(dataToAdd)
                            : fullDataObject[wsoId] = [dataToAdd]
                      }
                    })
                  }
                  else{ //has nrcs data but not wso data for this station so just returning nrcs data
                    if(fullDataObject[wsoId] || fullDataObject[nrcsStn]){
                      // console.log('nrcs data 191', {stnNrcsData:nrcsPeriodData[field]/1000, field, period: numericPeriod, metadata})
                      fullDataObject[wsoId ? wsoId : nrcsStn].push({stnNrcsData:nrcsPeriodData[field]/1000, field, period: numericPeriod, metadata, nrcsNormals})
                    }
                    // if(fullDataObject[nrcsStn]){
                    //   console.log('youre overwrting data', nrcsStn, fullDataObject[nrcsStn])
                    //   console.log('writing this data', {stnNrcsData:nrcsPeriodData[field]/1000, field, period: numericPeriod, metadata})
                    // }
                    else{
                      fullDataObject[wsoId ? wsoId : nrcsStn]=[{stnNrcsData:nrcsPeriodData[field]/1000, field, period: numericPeriod, metadata, nrcsNormals}]
                    }
      
                  }
                }
  
              
              }
            }
            else{
              console.log('handle no NRCS forecast data for ', nrcsStn)
              newReturnObj[wsoId ? wsoId : nrcsStn]={errors:[`no NRCS Data`]}
            }
          }
        }
      }
      else{ //has no nrcs data
        console.log('no nrcs data returned getobjectreactor 228', nrcsData)
        newReturnObj.requestErrors = 'no NRCS data returned from query'
      } 

      // looop through wsoDat and add any stations that didn't have NRCS dadta
      for(const secondWsoStn in wsoDat){
        //check to see if in master object
        if(!fullDataObject[secondWsoStn]){
          // console.log('no wso stn data in obj 224', secondWsoStn)
          const stnDataa = wsoDat[secondWsoStn].data
          const metadata = wsoDat[secondWsoStn].metadata
          for(const secondWsoPed in stnDataa){
            const currDataObject = stnDataa[secondWsoPed]
            const currData = currDataObject[field]
            const {eper, bper, pavg, pmed, avg30, med30} = currDataObject ? currDataObject : {}
            const currPeriod = bper && eper ? `${bper-3}-${eper-3}` : undefined // subtract 3 so that it matches calendar year number not water year month number
            // console.log('244 period', currPeriod, bper, eper)
            // const dataToAdd = {period, field, nrcsStn, nrcsData: nrcsFieldData, rfcData: wsoFieldData, diff, diffPct, metadata, rfcNormals: {pavg, pmed, avg30, med30}}

            if(currDataObject && currDataObject[field]){
              // console.log('167 this is the added data', {rfcData:currData, field, period: currPeriod, metadata, rfcNormals: {pavg, pmed, avg30, med30}})
              fullDataObject[secondWsoStn]=[{rfcData:currData, field, period: currPeriod, metadata, rfcNormals: {pavg, pmed, avg30, med30}}]
            }
            else{
              if(newReturnObj[secondWsoStn] && newReturnObj[secondWsoStn].errors){
                  newReturnObj[secondWsoStn].errors.push(`no data for wsoStn: ${secondWsoStn} period:${currPeriod} field: ${field}`)
                }
                else{
                  newReturnObj[secondWsoStn] = {...newReturnObj[secondWsoStn], errors:[`no data for wsoStn: ${secondWsoStn} period:${currPeriod} field: ${field}`]}
                }
            }
            // const secondWsoPedData = stnDataa[secondWsoPed]
            // for(const pedPeriod in secondWsoPedData){
            //   // console.log('161 pedPeriod', pedPeriod)
            //   // console.log('162 secondWsoPedData', secondWsoPedData)
            //   const newWsoPedData = secondWsoPedData[pedPeriod]
              
            //   if(newWsoPedData && newWsoPedData[field]){
            //     const{ pavg, pmed, avg30, med30} = newWsoPedData
            //     fullDataObject[secondWsoStn]=[{stnWsoData:newWsoPedData[field], field, period: pedPeriod, metadata, rfcNormals: {pavg, pmed, avg30, med30}}]
            //   }
            //   else{
            //     if(newReturnObj[secondWsoStn] && newReturnObj[secondWsoStn].errors){
            //       newReturnObj[secondWsoStn].errors.push(`no data for wsoStn: ${secondWsoStn} period:${pedPeriod} field: ${field}`)
            //     }
            //     else{
            //       newReturnObj[secondWsoStn] = {...newReturnObj[secondWsoStn], errors:[`no data for wsoStn: ${secondWsoStn} period:${pedPeriod} field: ${field}`]}

            //     }
            //   }
            // }
          }
        }
      }
      // console.log('182 stupid station final data here', fullDataObject['PRSC2'])
      newReturnObj.data = {...fullDataObject}
    
      // console.timeEnd('full')
      conn.closeSync();
      // console.log('newReturnObj', JSON.stringify(newReturnObj), 'getdataobjectrefactor 168')
      // try{
      //   fs.writeFileSync(`./staticData/${field}-${year}-${month}-${day}.json`,JSON.stringify(newReturnObj))
      // }
      // catch(e){
      //   console.log('error saving to local json', e)
      // }
      // console.log('newReturnObj', JSON.stringify(newReturnObj), '303 newreturnobj')
      console.log('finished in getDataObjectRefactor', 'field', field, 'request year', year, 'request month', month)
      return newReturnObj

    }
    catch(e){
      console.log('error get data object line 192', e)
    }

}


// getDataObject({month:1})
module.exports = getDataObject
















function getPeriodNumberString(period, stnData){
  const month1Num = monthNameToNum(period.split('-')[0])
  const month2Num = monthNameToNum(period.split('-')[1])
  if(!month2Num){
    // console.log('170 no month 2', period, period.split('-')[1], stnData)
  }
  return month1Num && month2Num ? `${month1Num}-${month2Num}` :  null
}


function monthNameToNum(monthString){
  return {
    'JAN':1,
    'FEB':2,
    'MAR':3,
    'APR':4,
    'MAY':5,
    'JUN':6,
    'JUL':7,
    'AUG':8,
    'SEP':9,
    'OCT':10,
    'NOV':11,
    'DEC':12
  }[monthString]
}