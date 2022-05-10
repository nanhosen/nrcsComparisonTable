const fs = require('fs')
var axios = require("axios")
var axiosRetry = require("axios-retry")
axiosRetry(axios, { retries: 3 });
// make so that can just get data or get metadata. don't want all in same object, right? lake have factory that returns

const {FullStationDataFactory, bbObj, keyMapAr, returnKeyAr, allData} = require('./getNrcsStationMetadata').factoryData

// const  {reformatNrcsJson, remoteNrcsFn, localNrcsFn} = require('./utils/nrcsFunctions.js')
// const {reformatNrcsJson, remoteNrcsFn, localNrcsFn} = nrcsFunctions
// const NrcsDataRequestFactoryUgh = async(year, month, day, requestData, areaStationObject)=>{
  
//   async function getData(){
//     return await requestData(year, month, day)
//     // console.log('fulldataset', fullDataset)

//   }

//   async function returnFormattedData(){
//     const forecastData = await getData()
//     // console.log('forecastData', Object.keys(forecastData.data))
//     if(forecastData.data){
//       const dataWithMetadata = FilterAndAddMetadataFactory(forecastData.data, areaStationObject).addMetaData()
//       return FilterAndAddMetadataFactory(forecastData.data, areaStationObject)
//     }
//     else if(forecastData.error){

//     }
//     else{
//       console.log('no data returned and no error given')
//     }
//   }

  
//   return {getData, returnFormattedData}

// }


async function requestData(year, month, day, localNrcsFn, remoteNrcsFn){
  try{
    console.log('trying local function')
    const localDataStream = await localNrcsFn(year, month, day)
    // console.log('local length', localDataStream, 'local 44')
    if(localDataStream.success && Object.keys(localDataStream.success).length >200){
      return {data: localDataStream.success, type:'local'}
    }
    else{
      try{
        console.log('local failed trying remote function')
        const remoteDataStream = await remoteNrcsFn(year, month, day)
        if(remoteDataStream.success){
          console.log('remoteDataStream success')
          return {data: remoteDataStream.success, type:'remote'}
        }
        else{
          console.log('local fialed too ')
          return {e: `error requesting remote data ${remoteDataStream.error}`}
        }
      }
      catch(e){
        return {e: `error in requesting remote JSON ${JSON.stringify(e)}`}
      }
    }
  }
  catch(e){
    return {e: `get nrcs data error ${e}`}
  }
}

async function localNrcsFn(year, month, day){
  try{
    const localJsonData = await fs.promises.readFile(`./nrcsJsonData/${year}-${month}-${day}.json`)
    // const localJsonDataBad = await fs.promises.readFile(`./nrcsJsonData/${year}-${month}-${day}a.json`)
    // return {success: localJsonDataBad}
    if(!localJsonData){
      return new Error ('no data returned from local file')
    }
    return {success: JSON.parse(localJsonData.toString())}
  }catch(e){
    // console.log('35 error', e)
    return {error: JSON.stringify(e)}
  }
}
async function remoteNrcsFn(year, month, day){
  try{
    const dataUrl = `https://www.nrcs.usda.gov/Internet/WCIS/data/SRVO/MONTHLY/FORECAST/SRVO_MONTHLY_FORECAST_${year}-${month}-${day}.json`
    const axiosData = await axios.get(dataUrl)
    return {success: axiosData.data}
  }catch(e){
    return {error: JSON.stringify(e)}
  }
}


function FilterAndAddMetadataFactory(forecastDataObject, areaStationObject){
  const returnObj =Object.create({})
  const nrcsForecastStationKeys = Object.keys(forecastDataObject)
  
  function getAreaStationIds(keyType){
    try{
      const keyArray = Object.keys(forecastDataObject)
      const areaStations =  keyArray.filter(currFcstKey =>{
        const idString = currFcstKey.split(':')[0]
        return areaStationObject[idString]
      }).map(currFcstKey =>{
        const idString = currFcstKey.split(':')[0]
        return keyType === 'dataKey' ? currFcstKey : idString 
      })
      return {stations: areaStations}
    }
    catch(e){
      console.log('get area stations error', e)
      return {error: `error in getAreaStations ${e}`}
    }
  }


  function getReformattedForecast(){
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
    try{
      // console.log(forecastDataObject, 'corecast data oject 138')
      const reformattedForecast = Object.create({})
      for (const [currFcstKey, idData] of Object.entries(forecastDataObject)) {
        const stationPeriodData = Object.create({})
        const idString = currFcstKey.split(':')[0]
        for(const period in idData){
          const periodData = idData[period]
          const periodReturn = Object.create({})
          for(var i = 0; i<periodData.length; i+=2){
            const keyName = nrcsNameObj[periodData[i]] ? nrcsNameObj[periodData[i]].name : periodData[i]
            periodReturn[keyName]=periodData[i+1]
          }
          stationPeriodData[period]=periodReturn
          // console.log('period return', periodReturn)
        }
        // console.log('99',stationPeriodData, Object.keys(stationPeriodData).length)
        reformattedForecast[idString]=stationPeriodData
      }
      return {forecastData: reformattedForecast}
    }
    catch(e){
      return {error: `error in getReformattedForecast ${e}`}
    }
  }

  function filteredForecast(forecastObject){
    try{
      const stationKeyArray = getAreaStationIds('id')
      if(!stationKeyArray || !stationKeyArray.stations){
        throw new Error ('no array to filter on')
      }
      const formattedForecast = forecastObject
      if(formattedForecast.error || !formattedForecast.forecastData){
        throw new Error('no forecast data to filter')
      }
      if(stationKeyArray.error){
        throw new Error(`no station Array to filter on ${e}`)
      }
      const filteredForecast = Object.create({})
      for (const [key, value] of Object.entries(formattedForecast.forecastData)) {
        if(stationKeyArray.stations.indexOf(key) >=0){
          filteredForecast[key]=value
        }
      }
      return {forecastData: filteredForecast}

    }
    catch(e){
      console.log('filtered forecast error 122', e)
      return {error: `error in filterForecast ${e}`}
    }
  }

  function addMetaData(forecastData){
    try{
      if(!forecastData ||!areaStationObject){
        throw new Error('addMetadata missing either forecast data or station data')
      }
      const returnObject = Object.create({})
      for (const [key, value] of Object.entries(forecastData)){
        // console.log(key, value, '190')
        // console.log({...value, metadata: areaStationObject[key]} ,'191')
        returnObj[key]= areaStationObject[key] ?  {forecastData: value, metadata: areaStationObject[key]} : {forecastData: value}
        // console.log('returnOb', returnObj, '197')
      }


      // console.log('formattedForecast', formattedForecast)
      // console.log('returnObject', returnObj, '201')
      return returnObj
      // stationArray.map(currStationData)
    }
    catch(e){
      return {error: `error in addMetaData ${e}`}
    }
  }

  function filterMetadata(forecastData){
    try{
      if(!forecastData ||!areaStationObject){
        throw new Error('addMetadata missing either forecast data or station data')
      }
      const returnObject = Object.create({})
      for (const [key, value] of Object.entries(forecastData)){
        // console.log(key, value, '190')
        // console.log({...value, metadata: areaStationObject[key]} ,'191')
        if(areaStationObject[key]){
          returnObj[key]= areaStationObject[key]
        }
        // console.log('returnOb', returnObj, '197')
      }


      // console.log('formattedForecast', formattedForecast)
      // console.log('returnObject', returnObj, '201')
      return returnObj
      // stationArray.map(currStationData)
    }
    catch(e){
      return {error: `error in addMetaData ${e}`}
    }
  }

  return {getAreaStationIds, addMetaData, filteredForecast, getReformattedForecast, filterMetadata}

}

async function getNrcsData(year, month, day){
  const cbrfcStationMetadata = FullStationDataFactory(allData, bbObj, keyMapAr, returnKeyAr).metadataById()
  const forecastData = await requestData(year, month, day,localNrcsFn, remoteNrcsFn)
  if(!forecastData || !forecastData.data){
    return new Error(`no forecast data retunred from requestData for ${year}-${month}-${day}`)
  }
  // console.log(forecastData.data, '248')
  const factoryStuff = FilterAndAddMetadataFactory(forecastData.data, cbrfcStationMetadata)

  async function getRawFilteredData(){
    // console.log('running get raw fltered data 255')
    let returnData
    try{
      if(!forecastData){
        throw new Error(`no forecast data returned from request`)
      }
      if(forecastData.type === 'local'){
        // return forecastData.data
        returnData = forecastData.data
        // console.log('return data lengt 264', Object.keys(returnData).length ,'returndata 264') //length 27 when short
    
      }
      else if(forecastData.type === 'remote'){
        const unfilteredForecast =  factoryStuff.getReformattedForecast()
        // console.log(unfilteredForecast, 'unfiltered 267')
        const filteredForecast =  factoryStuff.filteredForecast(unfilteredForecast)
        try{
          console.log(`'saving JSON' ${year}-${month}-${day}`)
          fs.writeFileSync(`./nrcsJsonData/${year}-${month}-${day}.json`,JSON.stringify(filteredForecast.forecastData))
        }
        catch(e){
          console.log('error saving to local json', e)
        }
        returnData = filteredForecast.forecastData
        // console.log('return data lengt 279', Object.keys(returnData).length ,'returndata 279') //length 27 when short

      }
      else{
        throw new Error(`invalid forecast data type ${JSON.stringify(forecastData.type)}`)
      }
    
      
      // console.log('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$', returnData, '283')
      return Object.freeze(returnData)
    }
    catch(e){
      throw new Error(`get raw data error ${e}`)
    }
  }
  async function getFilteredForecastWithMetadata(){
    const filteredForecastData = await getRawFilteredData()
    const forecastWithMetadata = factoryStuff.addMetaData(filteredForecast.forecastData)
    return forecastWithMetadata
  }
  async function getFilteredMetadata(forecastData){
    return factoryStuff.filterMetadata(forecastData)
  }

  async function getFilteredNormals(type='average'){
    try{
      if(type!=='median' && type!== 'average'){
        throw new Error(`${type} is not a valid type`)
      }
      const localJsonData = await fs.promises.readFile(`./nrcsClimoData/${type}.json`)
      return JSON.parse(localJsonData.toString())
    }
    catch(e){
      throw new Error(`get normals error ${e}`)
    }
  }

  return {getRawFilteredData, getFilteredForecastWithMetadata, getFilteredMetadata, getFilteredNormals}

  
  // console.log('formattedData 40', formattedData, DataFactory)

}
// async function run(){
//   const data = await getNrcsData(2021,3,1)
// }

// run()

exports.getNrcsData = getNrcsData


// async function getNrcsData(year, month, day){
//   async function getRawData(){
//     let returnData
//     const forecastData = await requestData(year, month, day,localNrcsFn, remoteNrcsFn)
//     if(!forecastData){
//       throw new Error(`no forecast data returned from request`)
//     }
//     if(forecastData.type === 'local'){
//       // return forecastData.data
//       returnData = forecastData.data
  
//     }
//     else if(forecastData.type === 'remote'){
//       const cbrfcStationMetadata = FullStationDataFactory(allData, bbObj, keyMapAr, returnKeyAr).metadataById()
//       const factoryStuff = FilterAndAddMetadataFactory(forecastData.data, cbrfcStationMetadata)
//       const unfilteredForecast = factoryStuff.getReformattedForecast()
//       const filteredForecast = factoryStuff.filteredForecast(unfilteredForecast)
//       const forecastWithMetadata = factoryStuff.addMetaData(filteredForecast.forecastData)
//       try{
//         console.log(`'saving JSON' ${year}-${month}-${day}`)
//         fs.writeFileSync(`./nrcsJsonData/${year}-${month}-${day}.json`,JSON.stringify(forecastWithMetadata))
//       }
//       catch(e){
//         console.log('error saving to local json', e)
//       }
//       returnData = forecastWithMetadata
//     }
//     else{
//       throw new Error(`invalid forecast data type ${JSON.stringify(forecastData.type)}`)
//     }
  
    
  
//     return {returnData}
//   }
//   try{
//   }
//   catch(e){
//     throw new Error(`NRCS request error ${JSON.stringify(e)}`)

//   }

  
  // console.log('formattedData 40', formattedData, DataFactory)

// }