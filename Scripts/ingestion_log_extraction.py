from datetime import datetime
import dateparser
import json

open_ingestions: dict[str, datetime] = {}
ingestions: list[dict] = []

def read_log_line(line: str):
    log_object: dict = json.loads(line)
    log_date: datetime = dateparser.parse(log_object["date"])
    log_message = log_object.get("message")
    log_props = log_object.get("properties", {})
    if log_message == "ingestion-start":
        ingestion_id = log_props["IngestionId"]
        open_ingestions[ingestion_id] = log_date
    elif log_message == "ingestion-end":
        ingestion_id = log_props["IngestionId"]
        ingestion_start_time = open_ingestions[ingestion_id]
        ingestion_end_time = log_date
        ingestion_duration = (ingestion_end_time - ingestion_start_time)
        ingestion_type = log_props["IngestionType"]
        ingested_files = int(log_props["FilesCount"])
        ingestion_size = log_props["TotalSize"]

        if ingested_files:
            ingestion_data = {"id": ingestion_id, "ingestion_type": ingestion_type, "duration": ingestion_duration.total_seconds(), "num_of_files": ingested_files, "total_size": ingestion_size}
            ingestions.append(ingestion_data)
        open_ingestions.pop(ingestion_id)


with open("log.json", "r") as f:
    all_logs = f.readlines()
    for log in all_logs:
        read_log_line(log)
with open("ingestions.csv", "a") as f:
    lines = []
    lines.append("id,ingestion_type,duration,num_of_files,total_size\n")
    for ingestion_data in ingestions:
        data_line = f"{ingestion_data['id']},{ingestion_data['ingestion_type']},{ingestion_data['duration']},{ingestion_data['num_of_files']},{ingestion_data['total_size']}\n"
        lines.append(data_line)
    f.writelines(lines)
