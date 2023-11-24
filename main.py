from src.LPLGather import parse

df = parse.get_player_details(10572,201,77).write_csv()

print(df)