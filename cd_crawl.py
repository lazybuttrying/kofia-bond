import os
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from util import monthly, fetch_data, handler
import pandas as pd
import sys
import time

results = []
batch_size = 10  # 배치 크기 조정 (한 번에 처리할 날짜 범위 개수)
batch_results = []


def main(fetch_type, max_retries=3):
    starts, ends = monthly(handler[fetch_type]['init_date'])

    with ThreadPoolExecutor(max_workers=5) as executor: 
        for i in range(0, len(starts), batch_size):
            batch_starts = starts[i:i + batch_size]
            batch_ends = ends[i:i + batch_size]

            future_to_range = {
                executor.submit(fetch_data, start, end, fetch_type): (start, end)
                for start, end in zip(batch_starts, batch_ends)
            }

            for future in as_completed(future_to_range):
                start, end = future_to_range[future]
                attempt = 0
                success = False

                while attempt < max_retries and not success:
                    try:
                        data = future.result()
                        batch_results.append(data)
                        print(f"Data fetched for range: {start} to {end}")
                        success = True  # Fetch successful, exit the retry loop
                    except Exception as e:
                        attempt += 1
                        print(
                            f"Attempt {attempt} failed for range {start} to {end}: {e}")
                        time.sleep(2)  # Wait before retrying
                        if attempt < max_retries:
                            # Resubmit the task if retry attempts are remaining
                            future = executor.submit(
                                fetch_data, start, end, fetch_type)
                        else:
                            print(
                                f"Max retries reached for range {start} to {end}. Skipping.")

            if batch_results:
                results.append(pd.concat(batch_results, ignore_index=True))
                batch_results.clear()  # 메모리 관리를 위해 배치 리스트 초기화
                time.sleep(1)  # 잠깐 쉬면서 부하를 줄임

    if not results:
        print("No data fetched")
        return

    return pd.concat(results, ignore_index=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argparse')
    parser.add_argument('--fetch-type', type=str)
    args = parser.parse_args()

    df = main(args.fetch_type)

    df.sort_values(handler[args.fetch_type]["date_col"]).to_csv(
        f'data/{args.fetch_type}_CD.csv', index=False, encoding='utf8')

    print(f"Data saved to {args.fetch_type}_CD.csv")
