import os
import pandas as pd
import re
from dataclasses import dataclass
import threading
from typing import Dict

@dataclass
class Report:
    year: int | str
    month: int | str
    type: str

    corruption: int = 0
    minorCorruption: int = 0
    moderateCorruption: int = 0
    graveCrime: int = 0
    especiallyGraveCrime: int = 0
    embezzlement: int = 0
    fraud: int = 0
    legalizationOfMoney: int = 0
    economicSmuggling: int = 0
    raiding: int = 0
    abuseOfOfficialAuthority: int = 0
    abuseOfPowerOrOfficialAuthority: int = 0
    illegalParticipationInBusinessActivities: int = 0
    obstructionOfLegitimateBusinessActivities: int = 0
    takingBribes: int = 0
    givingBribes: int = 0
    mediationInBribery: int = 0
    officialForgery: int = 0
    inactionInTheService: int = 0
    abuseOfPower: int = 0
    inactionOfPower: int = 0
DIR = "D:\Github\Kazakhstan-Crime-Data-Data-Extractor"
INPUT_DIR = DIR + "/data"
OUTPUT_DIR = DIR + "/reports"

reportsByID: Dict[tuple[int, int, str], Report] = {}
lock = threading.Lock()
threads = []
finalResults: Dict[tuple[int, int], Report] = {}

def parse_filename(filename):
    """Parses the report file name and extracts date and type (general/agency)."""

    old_format_match = re.match(r"(\d{6})3K.*?_ru\.(xlsx|XLSX)", filename, re.IGNORECASE)
    if old_format_match:
        date_str = old_format_match.group(1)
        month = int(date_str[:2])
        year = int(date_str[2:])
        return {"year": year, "month": month, "type": "general"}

    new_format_match = re.match(r"(\d{6})_3k_(\d{5})___ru\.(xlsx|XLSX)", filename, re.IGNORECASE)
    if new_format_match:
        date_str = new_format_match.group(1)
        agency_code = new_format_match.group(2)
        year = int(date_str[:4])
        month = int(date_str[4:])
        report_type = "general" if agency_code == "00000" else "agency"
        return {"year": year, "month": month, "type": report_type, "agency_code": agency_code}

    return None

def extractCorruptionCrimeCounts(file_path, localFileName, year, month, reportType):

    tot_corruption_count = 0
    tot_minor_corruption_count = 0
    tot_moderate_corruption_count = 0
    tot_grave_crime_count = 0
    tot_especially_grave_crime_count = 0
    tot_embezzlement_crime_count = 0
    tot_fraud_crime_count = 0
    tot_legalization_crime_count = 0

    try:
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        sheetName = xls.sheet_names[1] if xls.sheet_names[1] == "R1" else xls.sheet_names[0]
        print(f"Processing file: {localFileName}, Sheet: {sheetName}")



        df = pd.read_excel(xls, sheet_name=sheetName, header=None)
        if df.shape[0] < 14 or df.shape[1] < 5:
            print(f"Warning: {file_path} does not have enough data in R1 sheet.")
            return 0, 0, 0, 0, 0, 0, 0, 0
        if df.shape[0] > 10 and df.shape[1] > 10:
            # Total corruption crime check (B7, E7)
            if str(df.iat[6, 1]).strip() == "Всего коррупционных преступлений":
                val = df.iat[6, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_corruption_count = int(val)

            # Total corruption crime check for not R1
            if str(df.iat[7, 0]) == "Всего лиц, осужденных за совершение коррупционных преступлений ":
                val = df.iat[7, 3]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_corruption_count = int(val)

            # Minor corruption crime check (C8, E8)
            if str(df.iat[7, 2]).strip() == "небольшой тяжести":
                val = df.iat[7, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_minor_corruption_count = int(val)

            # Moderate corruption crime check (C9, E9)
            if str(df.iat[8, 2]).strip() == "средней тяжести":
                val = df.iat[8, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_moderate_corruption_count = int(val)

            # Grave corruption crime check (C10, E10)
            if str(df.iat[9, 2]).strip() == "тяжкие":
                val = df.iat[9, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_grave_crime_count = int(val)

            # Especially grave corruption crime check (C11, E11)
            if str(df.iat[10, 2]).strip() == "особо тяжкие":
                val = df.iat[10, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_especially_grave_crime_count = int(val)

            # Embezzlement or entrusted property crime check (C12, E12)
            if str(df.iat[
                       11, 2]).strip() == "Присвоение или растрата вверенного чужого имущества (п.2) ч.3 ст.189  УК РК) " or \
                    str(df.iat[
                            11, 2]) == "Присвоение или растрата вверенного чужого имущества (п.2) ч.3 ст.189  УК РК) " or \
                    str(df.iat[
                            11, 2]).strip() == "Присвоение или растрата вверенного чужого имущества (п.2) ч.3 ст.189  УК РК)" or \
                    str(df.iat[
                            11, 2]) == "Присвоение или растрата вверенного чужого имущества (п.2) ч.3 ст.189  УК РК)" or \
                    str(df.iat[
                            11, 2]) == "Присвоение или растрата вверенного чужого имущества (п.2) ч.3 ст.189, ч.4 ст.189  УК РК)":
                val = df.iat[11, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_embezzlement_crime_count = int(val)

            # Embezzlement or entrusted property crime check (C12, E12) - alternative
            if str(df.iat[
                       8, 1]) == "Присвоение или растрата вверенного чужого имущества (п.2) ч.3 ст.189, ч.4 ст.189  УК РК":
                val = df.iat[8, 3]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_embezzlement_crime_count = int(val)

            # Fraud crime check (C13, E13)
            if str(df.iat[12, 2]).strip() == "Мошенничество (п.2) ч.3 ст.190 УК РК)" or \
                    str(df.iat[12, 2]) == "Мошенничество (п.2) ч.3 ст.190 УК РК)" or \
                    str(df.iat[12, 2]).strip() == "Мошенничество (п.2) ч.3 ст.190, ч.4 ст.190 УК РК)" or \
                    str(df.iat[12, 2]) == "Мошенничество (п.2) ч.3 ст.190, ч.4 ст.190 УК РК)":
                val = df.iat[12, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_fraud_crime_count = int(val)

            # Embezzlement or entrusted property crime check (C12, E12) - alternative
            if str(df.iat[
                       9, 1]) == "Мошенничество (п.2) ч.3 ст.190, ч.4 ст.190 УК РК":
                val = df.iat[9, 3]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_fraud_crime_count = int(val)

            # Legalization of money crime check (C14, E14)
            if str(df.iat[
                       13, 2]).strip() == "Легализация (отмывание) денег и (или)иного имущества, полученных преступным путем (п.1) ч.3 ст.218 УК РК)  " or \
                    str(df.iat[
                            13, 2]) == "Легализация (отмывание) денег и (или)иного имущества, полученных преступным путем (п.1) ч.3 ст.218 УК РК) " or \
                    str(df.iat[
                            13, 2]) == "Легализация (отмывание) денег и (или)иного имущества, полученных преступным путем (п.1) ч.3 ст.218 УК РК)  ":
                val = df.iat[13, 4]
                if pd.notna(val) and isinstance(val, (int, float)):
                    tot_legalization_crime_count = int(val)

        if tot_corruption_count is None:
            tot_corruption_count = 0
        if tot_minor_corruption_count is None:
            tot_minor_corruption_count = 0
        if tot_moderate_corruption_count is None:
            tot_moderate_corruption_count = 0
        if tot_grave_crime_count is None:
            tot_grave_crime_count = 0
        if tot_especially_grave_crime_count is None:
            tot_especially_grave_crime_count = 0
        if tot_embezzlement_crime_count is None:
            tot_embezzlement_crime_count = 0
        if tot_fraud_crime_count is None:
            tot_fraud_crime_count = 0
        if tot_legalization_crime_count is None:
            tot_legalization_crime_count = 0

        if tot_corruption_count is None \
            and tot_minor_corruption_count is None \
            and tot_moderate_corruption_count is None \
            and tot_grave_crime_count is None \
            and tot_especially_grave_crime_count is None \
            and tot_embezzlement_crime_count is None \
            and tot_fraud_crime_count is None \
            and tot_legalization_crime_count is None:
            print(f"Warning: No valid crime data found in {file_path}")

        # print("------ Process Complete ------")
        # print(f"File: {localFileName}")
        # print(f"Year: {year}, Month: {month}, Type: {reportType}")
        # print(f"Total Crimes: {tot_corruption_count}")
        # print(f"Minor Corruption Crimes: {tot_minor_corruption_count}")
        # print(f"Moderate Corruption Crimes: {tot_moderate_corruption_count}")
        # print(f"Grave Corruption Crimes: {tot_grave_crime_count}")
        # print(f"Especially Grave Corruption Crimes: {tot_especially_grave_crime_count}")
        # print(f"Embezzlement or Entrusted Property: {tot_embezzlement_crime_count}")
        # print(f"Fraud Crimes: {tot_fraud_crime_count}")
        # print(f"Legalization of Money Crimes: {tot_legalization_crime_count}")
        # print("")

        return tot_corruption_count, \
            tot_minor_corruption_count, \
            tot_moderate_corruption_count, \
            tot_grave_crime_count, \
            tot_especially_grave_crime_count, \
            tot_embezzlement_crime_count, \
            tot_fraud_crime_count, \
            tot_legalization_crime_count

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0, 0, 0, 0, 0, 0, 0, 0

def processReportFiles(filename):
    """Processes all report files in the specified directory."""
    file_path = os.path.join(INPUT_DIR, filename)

    print(f"[{threading.current_thread().name}] Started reading {filename}")

    file_info = parse_filename(filename)
    if not file_info:
        print(f"Skipping unknown file format: {filename}")
        return

    year, month, report_type = file_info["year"], file_info["month"], file_info["type"]

    with lock:
        localReport = Report(year=year, month=month, type=report_type)
        reportsByID[(year, month, filename)] = localReport

        tot_corruption_count, \
            tot_minor_corruption_count, \
            tot_moderate_corruption_count, \
            tot_grave_crime_count, \
            tot_especially_grave_crime_count, \
            tot_embezzlement_crime_count, \
            tot_fraud_crime_count, \
            tot_legalization_crime_count = extractCorruptionCrimeCounts(
            file_path, filename, year, month, report_type
        )

        localReport.corruption = tot_corruption_count
        localReport.minorCorruption = tot_minor_corruption_count
        localReport.moderateCorruption = tot_moderate_corruption_count
        localReport.graveCrime = tot_grave_crime_count
        localReport.especiallyGraveCrime = tot_especially_grave_crime_count
        localReport.embezzlement = tot_embezzlement_crime_count
        localReport.fraud = tot_fraud_crime_count
        localReport.legalizationOfMoney = tot_legalization_crime_count


def printFormat(localReport: Report):
    """Formats the report data for printing."""
    return (
            "--------------------------------------------- \n"
            f"Report ID: {id(localReport)} \n"
            f"Year: {localReport.year} \n"
            f"Month: {localReport.month} \n"
            f"Type: {localReport.type} \n"
            f"Total Corruption Crimes: {localReport.corruption} \n"
            f"Minor Corruption Crimes: {localReport.minorCorruption} \n"
            f"Moderate Corruption Crimes: {localReport.moderateCorruption} \n"
            f"Grave Crimes: {localReport.graveCrime} \n"
            f"Especially Grave Crimes: {localReport.especiallyGraveCrime} \n"
            f"Embezzlement: {localReport.embezzlement} \n"
            f"Fraud: {localReport.fraud} \n"
            f"Legalization of Money: {localReport.legalizationOfMoney} \n"
            "--------------------------------------------- \n \n"
    )


for fileName in os.listdir(INPUT_DIR):
    t = threading.Thread(target=processReportFiles, args=(fileName,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

for report in reportsByID.values():
    print(printFormat(report))

    # Collect final results
    with lock:
        if report.type == "general": # Only process agency reports
            print('Processing general report...')
            finalResults[(report.year, report.month)] = report



df_result = pd.DataFrame([
    {
        "Date": f"{report.year}-{str(report.month).zfill(2)}",
        "Corruption": report.corruption,
        "Minor Corruption": report.minorCorruption,
        "Moderate Corruption": report.moderateCorruption,
        "Grave Crime": report.graveCrime,
        "Especially Grave Crime": report.especiallyGraveCrime,
        "Embezzlement": report.embezzlement,
        "Fraud": report.fraud,
        "Legalization of Money": report.legalizationOfMoney,
    }
    for (year, month), report in sorted(finalResults.items())
    if int(year) > 2016 or (int(year) == 2016 and int(month) >= 13)  # 2016-12 excluded
])

df_result.to_csv(OUTPUT_DIR + "/results.csv", index=False, encoding="utf-8")
print("\nParsing complete. Results saved to 'parsed_crime_data.csv'.")
