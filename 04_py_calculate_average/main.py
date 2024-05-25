from dataclasses import dataclass

MAX_LINES: int = 1000000000
# MAX_LINES: int = 10

# STATISTIC = dict[str, dict]

STATISTIC = dict()

# @dataclass(unsafe_hash=True)
# class Stat:
#     """Class for keeping track of an item in inventory."""
#     min: float
#     avg: float
#     max: float

# @dataclass(unsafe_hash=True)
# class StationStat:
#     """Class for keeping track of an item in inventory."""
#     name: str
#     stat: Stat

# # Abha=-33.9/18.0/77.8
# STATISTIC = StationStat()



# STATISTIC: dict = {
#     "Abha": {
#         "min": 23.0,
#         "avg": 18.0,
#         "max": 59.2
#     },
#     "Adelaide": {
#         "min": -27.8,
#         "avg": 17.3,
#         "max": 58.5
#     },
#     }

# STATISTIC['Abha']['avg'] = 0
# print(STATISTIC)

def print_stations_stat(STATISTIC):
    result_stat = dict(sorted(STATISTIC.items()))
    output: str = "{"
    for station, stat in result_stat.items():
        avg = stat["avg_sum"] / stat["avg_count"]
        avg = round(avg, 1)
        output += f'{station}={stat["min"]}/{avg}/{stat["max"]}, '

    output = output.rstrip(", ")
    output += "}"
    return output


# result = print_stations_stat(STATISTIC)
# print(result)

'''
l = "New Orleans;22.1"
station_stat =
{
    "Abha": {
        "avg_sum": 18.0,
        "avg_count": 2,
        "min": 23.0,
        "max": 59.2
    },
}
'''
def process_line(l: str):
    station, temp = l.split(";")
    temp = float(temp)
    if not STATISTIC.get(station):
        STATISTIC[station] = {
            "avg_sum": temp,
            "avg_count": int(1),
            "min": temp,
            "max": temp
        }
    else:
        avg_sum = STATISTIC[station]["avg_sum"] + temp
        STATISTIC[station]["avg_sum"] = avg_sum
        avg_count = STATISTIC[station]["avg_count"] + 1
        STATISTIC[station]["avg_count"] = avg_count

        if temp < STATISTIC[station]["min"]:
            STATISTIC[station]["min"] = temp

        if temp > STATISTIC[station]["max"]:
            STATISTIC[station]["max"] = temp



def main():
    # use file object as iterator
    with open('measurements.txt','r') as f:
        line_num = int(0)
        for l in f:
            process_line(l)
            line_num+=1
            if line_num == MAX_LINES:
                break
    # print(line_num)
    import json
    print(json.dumps(STATISTIC))
    print(print_stations_stat(STATISTIC))


main()

# time python3 04_py_calculate_average/main.py > 04_py_calculate_average_out.txt
# python3 04_py_calculate_average/main.py > 04_py_calculate_average_out.txt  393,36s user 1,80s system 99% cpu 6:35,17 total
