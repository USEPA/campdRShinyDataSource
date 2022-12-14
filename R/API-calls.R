
# API info
apiUrlBase <- "https://api.epa.gov/easey"
apiKEY <- Sys.getenv("API_KEY")

# annual emissions url
annualEmissionsUrl <- paste0(apiUrlBase,"/streaming-services/emissions/apportioned/annual?API_KEY=",apiKEY)
# quarter emissions url
quarterEmissionsPageUrl <- paste0(apiUrlBase,"/emissions-mgmt/emissions/apportioned/quarterly?API_KEY=",apiKEY)
# allowance compliance stream url
complianceUrl <- paste0(apiUrlBase,"/streaming-services/allowance-compliance?API_KEY=",apiKEY)
# allowance compliance page url
compliancePageUrl <- paste0(apiUrlBase,"/account-mgmt/allowance-compliance?API_KEY=",apiKEY)
# allowance compliance url
complianceApplicableUrl <- paste0(apiUrlBase,"/account-mgmt/allowance-compliance/attributes/applicable?api_key=",apiKEY)
# facilities stream url
facilitiesUrl <- paste0(apiUrlBase,"/streaming-services/facilities/attributes?API_KEY=",apiKEY)
# facilities (applicable) url
facilitiesApplicableUrl <- paste0(apiUrlBase,"/facilities-mgmt/facilities/attributes/applicable?API_KEY=",apiKEY)
# account info url
accountInfoUrl <- paste0(apiUrlBase,"/streaming-services/accounts/attributes?API_KEY=",apiKEY)
# program mdm url
programMdmUrl <- paste0(apiUrlBase,"/master-data-mgmt/programs?API_KEY=",apiKEY)
# program mdm url
statesMdmUrl <- paste0(apiUrlBase,"/master-data-mgmt/states?API_KEY=",apiKEY)


## global API functions

get_facility_data <- function(years){

  query <- list(year=(paste0(years, collapse = '|')))

  res = GET(facilitiesUrl, query = query)

  if (length(res$status_code) != 0){
    if (res$status_code != 200){
      stop(paste("API status code:",res$status_code,annualEmissionsUrl,"..",res$message))
    }
  }

  yearFacilityData <- fromJSON(rawToChar(res$content))

  yearFacilityData
}

# API calls to get compliance data
# format queryList - list(stateCode = paste0(c("AL"), collapse = '|'),programCodeInfo = paste0(c("ARP"), collapse = '|'))
# where states is a c() vector of elements
get_allow_comp_data <- function(complianceYears, programs=NULL,
                                states=NULL, facilities=NULL){

  query <- list(year=(paste0(complianceYears, collapse = '|')))

  if (!is.null(programs)){query <- append(query, list(programCodeInfo = (paste0(programs, collapse = '|'))))}
  if (!is.null(states)){query <- append(query, list(stateCode = (paste0(states, collapse = '|'))))}
  if (!is.null(facilities)){query <- append(query, list(facilityId = (paste0(facilities, collapse = '|'))))}

  res = GET(complianceUrl, query = query)

  if (length(res$status_code) != 0){
    if (res$status_code != 200){
      stop(paste("API status code:",res$status_code,annualEmissionsUrl,"..",res$message))
    }
  }

  yearComplianceData <- fromJSON(rawToChar(res$content))

  yearComplianceData
}

# API call to get allowance holdings info for a facility
get_account_info_data <- function(facilities=NULL,states=NULL){

  baseQuery <- list()

  if (!is.null(facilities)){baseQuery <- append(baseQuery, list(facilityId = (paste0(facilities, collapse = '|'))))}
  if (!is.null(states)){baseQuery <- append(baseQuery, list(stateCode = (paste0(states, collapse = '|'))))}

  res = GET(accountInfoUrl, query = baseQuery)

  if (length(res$status_code) != 0){
    if (res$status_code != 200){
      stop(paste("API status code:",res$status_code,accountInfoUrl,"..",res$message))
    }
  }

  returnData <- fromJSON(rawToChar(res$content))

  returnData
}

# get latest year for an endpoint returning yearly data
get_latest_valid_vear <- function(url, programs=NULL){
  latestYear <- as.integer(format(Sys.Date(), "%Y"))
  baseQuery <- list(page="1",
                    perPage="1")
  runExit <- 0

  if (!is.null(programs)){baseQuery <- append(baseQuery, list(programCodeInfo = paste0(programs, collapse = '|')))}
  query <- append(baseQuery, list(year=latestYear))
  res = GET(url, query = query)

  if (length(res$content) <= 2 | res$status_code==400){
    while(length(res$content) <= 2 | res$status_code==400){
      latestYear <- latestYear - 1
      runExit <- runExit + 1
      if (runExit > 2){
        return(NA)
        break
      }
      query <- append(baseQuery, list(year=latestYear))
      res = GET(url, query = query)
    }
  }

  return(latestYear)
}

# get latest year for an endpoint returning yearly data
get_latest_emission_valid_vear <- function(url, programs=NULL){
  latestYear <- as.integer(format(Sys.Date(), "%Y"))
  baseQuery <- list(page="1",
                    perPage="1",
                    quarter="4")
  runExit <- 0

  if (!is.null(programs)){baseQuery <- append(baseQuery, list(programCodeInfo = paste0(programs, collapse = '|')))}
  query <- append(baseQuery, list(year=latestYear))
  res = GET(url, query = query)

  if (length(res$content) <= 2 | res$status_code==400){
    while(length(res$content) <= 2 | res$status_code==400){
      latestYear <- latestYear - 1
      runExit <- runExit + 1
      if (runExit > 2){
        return(NA)
        break
      }
      query <- append(baseQuery, list(year=latestYear))
      res = GET(url, query = query)
    }
  }

  return(latestYear)
}
