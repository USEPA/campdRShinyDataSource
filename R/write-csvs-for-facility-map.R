## Script for packaging csvs

library(data.table)
library(httr)
library(htmltools)
library(jsonlite)
library(tidyverse)
library(lubridate)
library(zoo)

## store functions

# to get all facility data for downloading off of app
store_facility_data <- function(unitDataBase){
  unitData <- unitDataBase %>%
    mutate("so2ControlsInstalled" = case_when(
      !is.na(so2ControlInfo) ~ "Yes",
      is.na(so2ControlInfo) ~ "No"
    ))

  unitData <- unitData %>%
    mutate("noxControlsInstalled" = case_when(
      !is.na(noxControlInfo) ~ "Yes",
      is.na(noxControlInfo) ~ "No"
    ))

  unitData <- unitData %>%
    mutate("particulateMatterControlsInstalled" = case_when(
      !is.na(pmControlInfo) ~ "Yes",
      is.na(pmControlInfo) ~ "No"
    ))

  unitData <- unitData %>%
    mutate("mercuryControlsInstalled" = case_when(
      !is.na(hgControlInfo) ~ "Yes",
      is.na(hgControlInfo) ~ "No"
    ))

  facilityTableForDownload <- unitData[,c("facilityName","facilityId","stateCode",
                                          "stateName","county",
                                          "latitude","longitude","unitId","operatingStatus",
                                          "primaryFuelInfo","so2ControlsInstalled",
                                          "noxControlsInstalled",
                                          "particulateMatterControlsInstalled",
                                          "mercuryControlsInstalled",
                                          "year","programCodeInfo")]

  facilityTableForDownload
}

# API calls
source("./src/API-calls.R")


### Program year data ###
# Get all programs and storing appropriate emission and compliance years
res = GET(programMdmUrl)
allPrograms <- fromJSON(rawToChar(res$content))

allPrograms$programShorthandDescription <- NA
allPrograms$programShorthandDescription[which(allPrograms$programCode == "ARP")] <- "Acid Rain Program (SO2)"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CAIRNOX")] <- "CAIR NOx Annual Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CAIROS")] <- "CAIR NOx Ozone Season Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CAIRSO2")] <- "CAIR SO2 Annual Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSNOX")] <- "CSAPR NOx Annual Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSNOXOS")] <- "CSAPR NOx Ozone Season Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG1")] <- "CSAPR NOx Ozone Season Program Group 1"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG2")] <- "CSAPR NOx Ozone Season Program Group 2"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG3")] <- "CSAPR NOx Ozone Season Program Group 3"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSSO2G1")] <- "CSAPR SO2 Annual Program Group 1"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSSO2G2")] <- "CSAPR SO2 Annual Program Group 2"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "MATS")] <- "Mercury and Air Toxics Standards"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "NBP")] <- "NOx Budget Trading Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "NHNOX")] <- "New Hampshire NOx Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "NSPS4T")] <- "New Source Performance Standards subpart TTTT"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "OTC")] <- "OTC NOx Budget Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "RGGI")] <- "Regional Greenhouse Gas Initiative"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "SIPNOX")] <- "SIP NOx Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "TXSO2")] <- "Texas SO2 Trading Program"

# Get all allowance programs
allAllowancePrograms <- allPrograms#[allPrograms$allowanceUIFilter == TRUE,]

# adding emission and compliance year columns
allAllowancePrograms["emissionYears"] <- NA
allAllowancePrograms["complianceYears"] <- NA

# adding CAIR years
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CAIROS","CAIRNOX"))] <- paste(seq(2008, 2014), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CAIRSO2"))] <- paste(seq(2009, 2014), collapse=',')
allAllowancePrograms$complianceYears[which(allAllowancePrograms$programCode %in% c("CAIROS","CAIRNOX","CAIRSO2"))] <- paste(seq(2009, 2014), collapse=',')

# adding CSAPR (retired)
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CSNOXOS"))] <- paste(seq(2015, 2016), collapse=',')
allAllowancePrograms$complianceYears[which(allAllowancePrograms$programCode %in% c("CSNOXOS"))] <- paste(seq(2015, 2016), collapse=',')

# adding NBP and OTC
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("NBP"))] <- paste(seq(2003, 2008), collapse=',')
allAllowancePrograms$complianceYears[which(allAllowancePrograms$programCode %in% c("NBP"))] <- paste(seq(2003, 2008), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("OTC"))] <- paste(seq(1999, 2002), collapse=',')
allAllowancePrograms$complianceYears[which(allAllowancePrograms$programCode %in% c("OTC"))] <- paste(seq(1999, 2002), collapse=',')

# adding beginning year to current programs
startYears <- as.data.frame(allAllowancePrograms[allAllowancePrograms$retiredIndicator == FALSE,]$programCode)
colnames(startYears) <- c('programCode')
startYears["startYear"] <- NA
startYears$startYear[which(startYears$programCode %in% c("CSNOX","CSSO2G1","CSSO2G2"))] <- 2015
startYears$startYear[which(startYears$programCode %in% c("CSOSG1","CSOSG2"))] <- 2017
startYears$startYear[which(startYears$programCode %in% c("CSOSG3"))] <- 2021
startYears$startYear[which(startYears$programCode %in% c("TXSO2"))] <- 2019

# (ARP) using the APIs to get the last year of data (what year returns data)
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode == "ARP")] <- paste(
  append(c(1980,1985,1990),seq(1995,get_latest_emission_valid_vear(quarterEmissionsPageUrl, c("ARP")))), collapse=',')
allAllowancePrograms$complianceYears[which(allAllowancePrograms$programCode == "ARP")] <- paste(
  seq(1995,get_latest_valid_vear(compliancePageUrl, c("ARP"))), collapse=',')

# For all other programs, use the APIs to find the latest year of data per program
for (prg in allAllowancePrograms[allAllowancePrograms$retiredIndicator == FALSE,]$programCode){
  if(!(prg %in% c("ARP","MATS","NHNOX","NSPS4T","RGGI","SIPNOX"))){
    # API calls
    latestEmissionYear <- get_latest_emission_valid_vear(quarterEmissionsPageUrl, c(prg))
    latestComplianceYear <- get_latest_valid_vear(compliancePageUrl, c(prg))
    # In case an emission year isn't present, 'NA' remains in the cell
    if(!is.na(latestEmissionYear)){
      allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode == prg)] <-
        paste(seq(startYears$startYear[startYears$programCode==prg],
                  latestEmissionYear), collapse=',')
    }
    # In case a compliance year isn't present, 'NA' remains in the cell
    if(!is.na(latestComplianceYear)){
      allAllowancePrograms$complianceYears[which(allAllowancePrograms$programCode == prg)] <-
        paste(seq(startYears$startYear[startYears$programCode==prg],
                  latestComplianceYear), collapse=',')
    }
  }
}

write.csv(allAllowancePrograms, file = "data/facility-map/programTable.csv", row.names = FALSE)

# reassign dataframe to avoid conflict in loop
compliancePrograms <- allAllowancePrograms

# loops above store years as strings to be able to save to a file. The is reverting back to interger lists.
for (i in 1:nrow(allAllowancePrograms)){
  if (!is.na(allAllowancePrograms$complianceYears[i])){
    allAllowancePrograms$complianceYears[i] <- list(c(as.integer(unlist(strsplit(compliancePrograms$complianceYears[i], ",")))))
  }
  if (!is.na(allAllowancePrograms$emissionYears[i])){
    allAllowancePrograms$emissionYears[i] <- list(c(as.integer(unlist(strsplit(compliancePrograms$emissionYears[i], ",")))))
  }
}

#### Unit data for latest compliance year ###
unitData <- get_facility_data(latestComplianceYear)

res = GET(statesMdmUrl)
states <- fromJSON(rawToChar(res$content))

unitData <- merge(unitData,states[,c("stateCode","stateName")],all.x=TRUE)

facilitiesForLatestYear <- unique(unitData[,c("facilityId","facilityName")])

### Collect compliance data for all years ###
complianceYears <- unique(na.omit(unlist(allAllowancePrograms$complianceYears)))

# getting all compliance data for all applicable compliance years
allYearComplianceFacilityData <- get_allow_comp_data(complianceYears)

# trim to latest compliance year
complianceFacilityDataLatestYear <- allYearComplianceFacilityData[allYearComplianceFacilityData$year == latestComplianceYear,]

# account info for current facilities
allAccountInfo <- get_account_info_data()

accountInfo <- allAccountInfo[allAccountInfo$facilityId %in% facilitiesForLatestYear$facilityId,]

accountInfoFac <- unique(accountInfo[accountInfo$accountNumber %like% 'FACLTY',c("accountNumber","facilityId")])

# left join the valid facility accounts with facilities in account info
accountInfoFac <- merge(facilitiesForLatestYear,accountInfoFac,by=c("facilityId"),all.x = TRUE)

# Find facility account info (or blank if doesn't exist) of those not in the compliance data
leftOutFacAccts <- anti_join(accountInfoFac,unique(complianceFacilityDataLatestYear[,c("facilityId","facilityName","accountNumber")]), by = c("facilityId", "facilityName", "accountNumber"))

# bind for full set of accounts/facilities
complianceFacilityDataLatestYearFull <- bind_rows(complianceFacilityDataLatestYear,leftOutFacAccts)

# bundle for displaying - latest year
complianceFacilityDataLatestFormat <- bind_rows(lapply(1:nrow(complianceFacilityDataLatestYearFull), function(row){
  if (is.na(complianceFacilityDataLatestYearFull$programCodeInfo[row])){
    compStr <- NA
  }
  else if (is.na(complianceFacilityDataLatestYearFull$excessEmissions[row])){
    compStr <- "Yes"
  }
  else{compStr <- "No"}
  c("facilityName"=complianceFacilityDataLatestYearFull$facilityName[row],
    "facilityId"=complianceFacilityDataLatestYearFull$facilityId[row],
    "accountNumber"=complianceFacilityDataLatestYearFull$accountNumber[row],
    "programCode"=complianceFacilityDataLatestYearFull$programCodeInfo[row],
    "year"=complianceFacilityDataLatestYearFull$year[row],
    "inCompliance?"=compStr)
}))

# bundle for displaying - all years
allYearComplianceFacilityDataFormat <- bind_rows(lapply(1:nrow(allYearComplianceFacilityData), function(row){
  if (!is.na(allYearComplianceFacilityData[row,"excessEmissions"])){
    c("facilityName"=allYearComplianceFacilityData$facilityName[row],
      "facilityId"=allYearComplianceFacilityData$facilityId[row],
      "accountNumber"=allYearComplianceFacilityData$accountNumber[row],
      "programCode"=allYearComplianceFacilityData$programCodeInfo[row],
      "year"=allYearComplianceFacilityData$year[row],
      "inCompliance?"="No")
  }
}))

complianceDataTableForDownload <- rbind(complianceFacilityDataLatestFormat,allYearComplianceFacilityDataFormat)

write.csv(complianceDataTableForDownload, file = "data/facility-map/complianceDataTableForDownload.csv", row.names = FALSE)

### Collect facility data for all years ###
facilityDataTableForDownload <- store_facility_data(unitData)
write.csv(facilityDataTableForDownload, file = "data/facility-map/facilityDataTableForDownload.csv", row.names = FALSE)


