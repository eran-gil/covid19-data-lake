from datetime import datetime
import dateparser
import json

open_queries: dict[str, datetime] = {}
queries: list[dict] = []

def read_log_line(line: str):
    log_object: dict = json.loads(line)
    log_date: datetime = dateparser.parse(log_object["date"])
    log_message = log_object.get("message")
    log_props = log_object.get("properties", {})
    if log_message == "query-start":
        query_id = log_props["SessionId"]
        open_queries[query_id] = log_date
    elif log_message == "query-end":
        query_id = log_props["SessionId"]
        query_start_time = open_queries[query_id]
        query_end_time = log_date
        query_duration = (query_end_time - query_start_time)
        query_type = log_props["QueryType"]
        num_of_results = int(log_props["ResultCount"])
        if num_of_results:
            query_data = {"id": query_id, "query_type": query_type, "duration": query_duration.total_seconds(), "num_of_results": num_of_results}
            queries.append(query_data)
        open_queries.pop(query_id)


with open("log.json", "r") as f:
    all_logs = f.readlines()
    for log in all_logs:
        read_log_line(log)
with open("queries.csv", "a") as f:
    lines = []
    lines.append("id,query_type,duration,num_of_results\n")
    for query_data in queries:
        data_line = f"{query_data['id']},{query_data['query_type']},{query_data['duration']},{query_data['num_of_results']}\n"
        lines.append(data_line)
    f.writelines(lines)
