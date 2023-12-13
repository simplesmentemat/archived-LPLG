from src.LPLGather import parse

df = parse.get_schedule_data().write_csv("schedule.csv")

print(df)