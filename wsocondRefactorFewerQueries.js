//notes: there's no wsoped in wss which means it will always be 13 for iavg and pmed
// mext steps: i'm getting all of the correct data now. Need to handle if request date is latest and do the approved 
//thng for one of rhe queries. Also need to refine the nmber of stations returned. I think it's based on wss stations
//the key that cass is using is the wss-date-date thing. I don't want to do that but id does seem like there are multiple possible
//data pieces per station. so maybe want to have a period thing? like key -> period -> data mayber. 
//okay, there are multiple ids in wss. the diff for some at least seems to be the espped and the status and the dobsped. so f
//so figure out if want just one of those. also something for period. turns out that i know nothing about this dtat. \//
// google doc with info on db: https://docs.google.com/document/d/1Un6xKfFmntKb1s-8WAUJWPsc9IsPPYpjjWtQ8IEYfFo/edit#heading=h.jpm9xrtpi5oi
//WSS (Water Supply Stations - CBRFC add-on to standard tables)
// id
// pedtsep (base shef code)
// bper (beginning water year month for period; i.e.7=Apr)
// eper (ending water year month for period; i.e. 10=Jul)
// basin
// subbasin
// slot (keep order within basin)
// byr (first year forecasts issued)
// eyr (last year forecasts issued where 9999=ongoing)
// espid, espped
// monthly obsid, obsped
// daily obsid, obsped


const { water_year, getLatestFcstData, runDbQuery, getWsoAppData } = require("./utils")
const { idDates, basins, areaIds } = require("./config/config")
const e = require("express")
// const wsoDat = await wsodata(`${year}-${month}-${day}`)
const wsodata = async({year, month, day}={}) =>{
  console.log('28 wso input', year, month, day)
  const reqYear = year ? year : new Date().getFullYear()
  const reqMonth = month ? month : new Date().getMonth() + 1
  const reqDay = day ? day : new Date(reqDate).getDate()
  // const wsodata = async(fdate_str = 'LATEST',area = 'CB',otype,residual) =>{
  const returnObj = {}
  const wsoStns = await runDbQuery(`select unique id from wso where fdtyr=${reqYear} and fdtmon=${reqMonth+3} and fdtday=${reqDay}`)
  const stnsInArea = await runDbQuery("select unique id from wss where basin='UC' or basin = 'GN'  or basin = 'SJ'  or basin = 'SL'  or basin = 'SV'  or basin = 'VG'  or basin = 'LC'")
  const wssData =  await runDbQuery(`select * from wss`)
  const wsoData = await runDbQuery(`select * from wso where fdtyr=${reqYear} and fdtmon=${reqMonth+3} and fdtday=${reqDay}`)
  const wspData = await runDbQuery(`select * from wsp`)
  const allStnMetadata = await runDbQuery(`select id, des, stat from stn`)
  // const allStnMetadata = await runDbQuery(`select * from stn`)
  const wssDataObj = dbDataToObj(wssData)
  const wsoDataObj = dbDataToObj(wsoData)
  const wspDataObj = dbDataToObj(wspData)
  const metadataObj = dbDataToObj(allStnMetadata)

  const stnsInAreaArray = stnsInArea.map(curr => curr.id)
  const stnsToQuery = []
  wsoStns.map(currStn=>{
    if(stnsInAreaArray.indexOf(currStn.id)>=0){
      stnsToQuery.push(currStn.id)
    }
  })

  if(stnsToQuery.length > 0 ){
    
    stnsToQuery.map(currStn=>{
      const stnWssData = wssDataObj[currStn].data
      const stnWsoData = wsoDataObj[currStn].data
      const stnWspData = wspDataObj[currStn].data
      const stnMetadata = metadataObj[currStn].data
      if(stnWsoData && stnWsoData.length > 0 ){
        returnObj[currStn]=buildStationDataObjectWithData(currStn, reqYear, reqMonth, reqDay, stnWssData, stnWsoData, stnWspData, stnMetadata)
      }
      else{
        returnObj[currStn]={error:['no WSO data']}
      }
    })
  }
  else{
    if(!returnObj.errors){
      returnObj.errors = []
    }
    returnObj.errors.push('no stations returned from the wso stns query')
  }
  
  // console.log(returnObj)
  return returnObj
}
function dbDataToObj(dbData){
  const returnObj = {}
  dbData.map(curr=>{
    if(!returnObj[curr.id]){
      returnObj[curr.id]={data:[curr]}
      // console.log(curr)
    }
    else{
      returnObj[curr.id]['data'].push(curr)
    }
  })
  return returnObj
}
function buildStationDataObjectWithData(currStn, reqYear, reqMonth, reqDay, stnWssData, stnWsoData, stnWspData, stnMetadata){
  // console.log('hi 94', currStn, stnWssData, stnWsoData, stnWspData, stnMetadata)
  const stnReturnObj = {errors:[], data:[]}
  const {obsped, basin, subbasin} = stnWssData && stnWssData[0] ? stnWssData[0] : {}
  const wsoMultiple = checkForMultiple(stnWsoData, stnReturnObj.errors, 'stnWsoData', currStn)
  checkForMultiple(stnWssData, stnReturnObj.errors, 'stnWssData', currStn)
  const wspMultiple = checkForMultiple(stnWspData, stnReturnObj.errors, 'stnWspData', currStn)
  stnWsoData.map(currWsoStnData =>{
    const wsoPed = pedObjtoString(currWsoStnData)
    const {cmp, crx, crn, c30, c70, bper, eper,} = currWsoStnData
    const wsoPeriod = makePeriodString(currWsoStnData)
    const matchingWspData = stnWspData.filter(currWspData=>{
      const wspPeriod = makePeriodString(currWspData)
      const wspPed = pedObjtoString(currWspData)
      let datToReturn = false
      if(wspPeriod === wsoPeriod && wspPed === wsoPed){
        // console.log('matching data here')
        datToReturn = true
      }
      return datToReturn
    })
    // console.log('matchingWspData 111', matchingWspData, currWsoStnData)
    if(matchingWspData[0]){
      const {avg25, med25, std25, avg30, med30, std30} = matchingWspData[0]
      const pavg = cmp && avg30 && avg30!==0 ? cmp /avg30*100 : 0
      const pmed = cmp && med30 && med30 !== 0 ? cmp /med30*100 : 0
      const iavg = pavg 
        ? getIThing(obsped, pavg, "QCVFVZZ", indicatorRanges, 12)
        : 13
      const imed = pmed
        ? getIThing(obsped, pmed, "QCVFVZZ", indicatorRanges, 12)
        : 13  
      // console.log('data object', {ped: wsoPed, bper, eper, cmp, crx, crn, c30, c70, pavg, pmed, iavg, imed, avg30, med30})  
      stnReturnObj.data.push({ped: wsoPed, bper, eper, cmp, crx, crn, c30, c70, pavg, pmed, iavg, imed, avg30, med30})

    }
    else{ //handle no corresponding wsop dataa
      stnReturnObj.data.push({ped: obsped, cmp, crx, crn, c30, c70, bper, eper} )
    }
  })
  // console.log('obsped', obsped)
  // if(wsoMultiple && wspMultiple){
  //   // console.log('mult wsp', wspMultiple, stnWspData)
  //   // console.log('mult wso', wsoMultiple, stnWsoData)
  // }
  // if(obsped && typeof obsped == 'string'){
  //   // const {pe1:wsope1, pe2:wsope2, dur:wsodur, t:wsot, s:wsos, e:wsoe, p:wsop} = stnWsoData && stnWsoData[0] ? stnWsoData[0] : {}
  //   // const {pe1:wsppe1, pe2:wsppe2, dur:wspdur, t:wspt, s:wsps, e:wspe, p:wspp} = stnWspData && stnWspData[0] ? stnWspData[0] : {}
  //   const wsoPed = pedObjtoString(stnWsoData)
  //   const wspPed = pedObjtoString(stnWspData)
  //   if(wsoPed === obsped && wspPed === obsped){
  //     const {avg25, med25, std25, avg30, med30, std30} = stnWspData && stnWspData[0]? stnWspData[0] : {}
  //     const {cmp, crx, crn, c30, c70, bper, eper} = stnWsoData && stnWsoData[0]? stnWsoData[0] : {}
  //     const pavg = cmp && avg30 && avg30!==0 ? cmp /avg30*100 : 0
  //     const pmed = cmp && med30 && med30 !== 0 ? cmp /med30*100 : 0
  //     const iavg = pavg 
  //       ? getIThing(obsped, pavg, "QCVFVZZ", indicatorRanges, 12)
  //       : 13
  //     const imed = pmed
  //       ? getIThing(obsped, pmed, "QCVFVZZ", indicatorRanges, 12)
  //       : 13  


  //     stnReturnObj.data.push({ped: obsped, bper, eper, cmp, crx, crn, c30, c70, pavg, pmed, iavg, imed})
  //     // console.log(obsped, bper, eper, cmp, crx, crn, c30, c70, pavg, pmed, iavg, imed)

  //   }
  //   else{
  //     // console.log(`121 obs ped doesnt match data ped. obsped: ${obsped}, wsoped: ${wsoPed}, wspped: ${wspPed}`)
  //     // console.log('122 stnWsoData', stnWsoData)
  //     // console.log('123 stnWspData', stnWspData)
  //     stnReturnObj.errors.push(`obs ped doesnt match data ped. obsped: ${obsped}, wsoped: ${wsoPed}, wspped: ${wspPed}`)
  //   }
  //   // console.log('wspPed', wspPed)
  // }
  // else{
  //   stnReturnObj.errors.push('no ped or ped not string')
  // }
  const{des: riverInfo, stat: state} = stnMetadata && stnMetadata[0] ? stnMetadata[0] : {}
  const {riverLocation, riverName} = riverInfo ? getRiverNameInfo(riverInfo) : false
  stnReturnObj.metadata = {riverLocation, riverName, state, id:currStn, basin, subbasin}

  // console.log(stnReturnObj, 'stn return obj 175')
  return stnReturnObj
}
function checkForMultiple(dataArray, errorObj, dataName, stnId){
  if(dataArray && dataArray.length>1){
    // console.log('140', `multiple data for ${dataName}`, stnId)
    errorObj.push(`multiple data for ${dataName}`)

    return true
  }
  else{
    return false
  }
}
function pedObjtoString(dataObj){
  const {pe1, pe2, dur, t, s, e, p} = dataObj ? dataObj : {}
  if(pe1&& pe2&& dur&& t&& s&& e&& p){
    return `${pe1}${pe2}${dur}${t}${s}${e}${p}`
  }
  else{
    return null
  }
}
function makePeriodString(dataObj){
  const {eper, bper} = dataObj ? dataObj : {}
  if(eper && bper){
    return `${bper}-${eper}`
  }
  else{
    return null
  }
}
function pedObjtoString1(pe1, pe2, dur, t, s, e, p){
  return `${pe1}${pe2}${dur}${t}${s}${e}${p}`
}
async function buildStationDataObject(currStn, reqYear, reqMonth, reqDay){
    const stnReturnObj = {errors:[]}
  // const currStn = wsoStns[0].id
    // const currWsoData = await runDbQuery(`select * from wso where id = "AFPU1"`)
    const currWssData = await runDbQuery(`select * from wss where id = "${currStn}"`)
    // const currWssData = await runDbQuery(`select obsped, basin, subbasin from wss where id = "${currStn}"`)
    if(currWssData && currWssData.length >1){
      console.log('this has more data than you are grabbing - wss (line 303 wsoCondRefactor)', currWssData)
      stnReturnObj.errors.push('not grabbing all wss data - check for addition periods')
    }
    const {obsped: reqPed, basin: currBasin, subbasin:currSubbasin} = currWssData && currWssData[0] ? currWssData[0] : {}
    let hasPed = false
    let currWsoData
    if(reqPed && typeof reqPed == 'string'){
      hasPed = true
      const [pe1, pe2, dur, t, s, e, p] = [...reqPed]
      // console.log(pe1, pe2, dur, t, s, e, p)
      const wsoPedQuery = `pe1 = "${pe1}" and pe2 = "${pe2}" and dur = "${dur}" and t = "${t}" and s = "${s}" and e = "${e}" and p = "${p}"`
      const wspPedQuery = `pe1 = "${pe1}" and pe2 = "${pe2}" and dur = "${dur}" and t = "${t}" and s = "${s}" and e = "${e}" and p = "${p}"`
      currWsoData = hasPed ? await runDbQuery(`select * from wso where fdtyr=${reqYear} and fdtmon=${reqMonth+3} and fdtday=${reqDay} and id = "${currStn}" and ${wsoPedQuery}`) : false
      if(currWsoData && currWsoData.length>0){

        const {bper:currBper, eper:currEper} = currWsoData && currWsoData[0]? currWsoData[0] : {}
        if(!currBper || !currEper){
          console.log('no bper eper', currWsoData, currWssData)
        }
        const currWspData = hasPed ? await runDbQuery(`select * from wsp where id = "${currStn}" and ${wsoPedQuery} and bper = ${currBper} and eper = ${currEper}`) : false
        const {avg25, med25, std25, avg30, med30, std30} = currWspData && currWspData[0]? currWspData[0] : {}
        const {cmp, crx, crn, c30, c70} = currWsoData && currWsoData[0]? currWsoData[0] : {}
        if(currWsoData && currWsoData.length >1){
          console.log('this has more data than you are grabbing - wso (line 321 wsoCondRefactor)', currWsoData)
        stnReturnObj.errors.push('not grabbing all wso data - check for addition periods')
        }
        // const {cmp, crz, crn, nws, cag, cagrx, cagrn, nwsrx, nwsrn, c30, c70, nws30, nws70} = currWsoData && currWsoData[0]? currWsoData[0] : {}
        const pavg = cmp && avg30 && avg30!==0 ? cmp /avg30*100 : 0
        const pmed = cmp && med30 && med30 !== 0 ? cmp /med30*100 : 0
        const iavg = pavg 
          ? getIThing(reqPed, pavg, "QCVFVZZ", indicatorRanges, 12)
          : 13
          const imed = pmed
          ? getIThing(reqPed, pmed, "QCVFVZZ", indicatorRanges, 12)
          : 13  
        console.log('avg30 251', avg30)
  
        stnReturnObj.data = {ped: reqPed, bper: currBper, eper: currEper, cmp, crx, crn, c30, c70, pavg, pmed, iavg, imed, avg30, med30}
      }
      else{
        stnReturnObj.errors.push('no wso data')
      }
    }
    else{
      stnReturnObj.errors.push('no ped data')
      // do i want to grab data if I get a ped error?
      currWsoData =  await runDbQuery(`select * from wso where fdtyr=${reqYear} and fdtmon=${reqMonth+3} and fdtday=${reqDay} and id = "${currStn}" `)

    }
    const stnMetadata = await runDbQuery(`select des, stat from stn where id="${currStn}"`)
    const{des: riverInfo, stat: state} = stnMetadata && stnMetadata[0] ? stnMetadata[0] : {}
    const {riverLocation, riverName} = riverInfo ? getRiverNameInfo(riverInfo) : false
    stnReturnObj.metadata = {riverLocation, riverName, state, id:currStn, basin: currBasin, subbasin:currSubbasin}
    // console.log('ln  stationReturnObj', stnReturnObj)
    return stnReturnObj
}

function newFunction(ids) {
  return ids.map(curr => {
    return curr.id
  })
}

function getRiverNameInfo(riverInfo){
  const riverName = riverInfo ? riverInfo.split('-')[0] : false
    const river1 = riverInfo ? riverInfo.split('-')[1] : false
    const river2 = river1 && typeof river1 == 'string' ? river1.replace(/,/g,';') : false //replace commas with semicolons
    const river2a = river2 && typeof river2 == 'string' ? river2.replace(/  /g,'') : false //replace commas with semicolons
    var riverLocation
    if(river2a && typeof river2a == 'string'){
      if(river2a.search("-") <0 ){
        riverLocation = river2a
      }
      else{
        riverLocation = river2a.split('-')[1]
      }
    }
    return{riverName, riverLocation}
}

function getIThing(wsoped, statVal, string, ranges, stringReturnVal){
  const returnObj = {}
  if(wsoped && wsoped == string){
    return stringReturnVal
  }
  else{
    for(const [index,value] of ranges.entries()){
      const minVal = index == 0 ? 0 : ranges[index-1]['bottomVal']
      const isValInRange = inRange(statVal, minVal, value.bottomVal)
      const isLast = index == ranges.length - 1 ? true : false
      if(isValInRange){
        return value.returnVal
      }
      else if(isLast){
        return value.returnVal
      }
    }
  }
}

function inRange(x, min, max) {
  return ((x-min)*(x-max) <= 0);
}

const indicatorRanges = [
  {
    bottomVal:30,
    returnVal: 0
  },   
  {
    bottomVal:50,
    returnVal:1 
  },   
  {
    bottomVal:70,
    returnVal:2
  },   
  {
    bottomVal:90,
    returnVal:3
  },   
  {
    bottomVal:100,
    returnVal:4
  },   
  {
    bottomVal:110,
    returnVal:5
  },   
  {
    bottomVal:130,
    returnVal:6
  },   
  {
    bottomVal:150,
    returnVal:7
  },   
  {
    bottomVal:200,
    returnVal:8
  },   
  {
    bottomVal:300,
    returnVal:9
  },   
  {
    bottomVal:500,
    returnVal:10
  },
  {
    bottomVal:501,
    returnVal:11
  }
]
  


exports.wsodata = wsodata

