from datetime import datetime


def main():
    start_time = int(datetime(2022, 10, 1, 0).timestamp() * 1e3)
    end_time = int(datetime(2022, 10, 1, 10).timestamp() * 1e3)
    one_hour_ms = 3600000
    while start_time < end_time:
        st = start_time
        et = min(end_time, start_time + one_hour_ms)
        print(st, et)
        start_time += one_hour_ms


if __name__ == "__main__":
    main()
