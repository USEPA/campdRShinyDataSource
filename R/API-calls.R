
# API info
apiUrlBase <- "https://api.epa.gov/easey"
# If .env exists, load it. Otherwise, use environment variables
if (file.exists(".env")) {
  if (require(dotenv)) {
    dotenv::load_dot_env()
  } else {
    stop("dotenv package is required to load .env file.")
  }
}
apiKEY <- Sys.getenv("API_KEY")

# quarter emissions url
emissionsSumbissionUrl <- paste0(apiUrlBase,"/emissions-mgmt/emissions/submission-progress?API_KEY=",apiKEY)
# allowance compliance stream url
complianceUrl <- paste0(apiUrlBase,"/streaming-services/allowance-compliance?API_KEY=",apiKEY)
# allowance compliance url
complianceApplicableUrl <- paste0(apiUrlBase,"/account-mgmt/allowance-compliance/attributes/applicable?api_key=",apiKEY)
# facilities stream url
facilitiesUrl <- paste0(apiUrlBase,"/streaming-services/facilities/attributes?API_KEY=",apiKEY)
# account info url
accountInfoUrl <- paste0(apiUrlBase,"/streaming-services/accounts/attributes?API_KEY=",apiKEY)
# program mdm url
programMdmUrl <- paste0(apiUrlBase,"/master-data-mgmt/program-codes?API_KEY=",apiKEY)
# program mdm url
statesMdmUrl <- paste0(apiUrlBase,"/master-data-mgmt/state-codes?API_KEY=",apiKEY)


## global API functions

get_facility_data <- function(years){

  query <- list(year=(paste0(years, collapse = '|')))

  res = GET(facilitiesUrl, query = query)

  if ((res$status_code != 200) & (res$status_code != 304)){
    errorFrame <- fromJSON(rawToChar(res$content))
    stop(paste("Error Code:",errorFrame$statusCode,errorFrame$message))
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

  if ((res$status_code != 200) & (res$status_code != 304)){
    errorFrame <- fromJSON(rawToChar(res$content))
    stop(paste("Error Code:",errorFrame$statusCode,errorFrame$message))
  }

  yearComplianceData <- fromJSON(rawToChar(res$content))

  yearComplianceData
}

# Allowance compliance (applicable) data
# all arguments must be in vector format i.e. c("CT","NY","WI")
get_allow_comp_applicable_data <- function(){

  res = GET(complianceApplicableUrl)

  if ((res$status_code != 200) & (res$status_code != 304)){
    errorFrame <- fromJSON(rawToChar(res$content))
    stop(paste("Error Code:",errorFrame$statusCode,errorFrame$message))
  }

  allowanceComplianceData <- fromJSON(rawToChar(res$content))

  allowanceComplianceData
}

# API call to get allowance holdings info for a facility
get_account_info_data <- function(facilities=NULL,states=NULL){

  baseQuery <- list()

  if (!is.null(facilities)){baseQuery <- append(baseQuery, list(facilityId = (paste0(facilities, collapse = '|'))))}
  if (!is.null(states)){baseQuery <- append(baseQuery, list(stateCode = (paste0(states, collapse = '|'))))}

  res = GET(accountInfoUrl, query = baseQuery)

  if ((res$status_code != 200) & (res$status_code != 304)){
    errorFrame <- fromJSON(rawToChar(res$content))
    stop(paste("Error Code:",errorFrame$statusCode,errorFrame$message))
  }

  returnData <- fromJSON(rawToChar(res$content))

  returnData
}

# get latest year from emissions submission info
get_latest_emission_valid_vear <- function(dateInput=Sys.Date()){
  date <- as.Date(dateInput)
  latestYear <- year(date)

  res = GET(emissionsSumbissionUrl,query=list(submissionPeriod = date))

  if (length(res$content)!=0) {
    submissionData <- fromJSON(rawToChar(res$content))
    if (submissionData$quarter == 4){
      latestYear <- latestYear - 1
    }
  }
  latestYear <- latestYear - 1

  return(latestYear)

}
