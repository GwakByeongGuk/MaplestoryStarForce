import mysql.connector
import requests
from datetime import datetime

# API 키 및 MariaDB 연결 설정
API_KEY = "test_9809f61f0704f7738247167f97a3e7a337806bdad3f7fa08abd1a1fa3a7a6babefe8d04e6d233bd35cf2fabdeb93fb0d"
BASE_URL = "https://open.api.nexon.com/maplestory/v1/history/starforce"
DB_CONFIG = {
    'user': 'clouds2024',
    'password': 'clouds2024',
    'host': '3.35.133.116',
    'database': 'clouds2024',
}

def create_table():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS starforce (
                id VARCHAR(255) PRIMARY KEY,
                item_upgrade_result VARCHAR(255),
                before_starforce_count INT,
                after_starforce_count INT,
                character_name VARCHAR(255),
                world_name VARCHAR(255),
                target_item VARCHAR(255),
                date_create DATETIME
            )
        ''')
        conn.commit()
        print("테이블이 성공적으로 생성되었습니다.")
    except mysql.connector.Error as e:
        print(f"MySQL 오류: {e}")
    finally:
        conn.close()

def get_starforce_data(count, date):
    headers = {
        "x-nxopen-api-key": API_KEY
    }

    params = {
        "count": count,
        "date": date
    }

    try:
        response = requests.get(BASE_URL, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            print(f"API 응답 데이터: {data}")  # 디버깅용 출력
            return data.get('starforce_history', [])
        else:
            print(f"HTTP 오류 발생: 상태 코드 {response.status_code}")
    except requests.RequestException as e:
        print(f"HTTP 요청 오류: {e}")
    except ValueError:
        print("응답을 JSON으로 변환하는 데 실패했습니다.")
    return []

def insert_data(data_list):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for result in data_list:
            print(f"삽입할 데이터: {result}")  # 디버깅용 출력
            id = result.get('id', 'N/A')
            item_upgrade_result = result.get('item_upgrade_result', 'N/A')
            before_starforce_count = result.get('before_starforce_count', 0)
            after_starforce_count = result.get('after_starforce_count', 0)
            character_name = result.get('character_name', 'N/A')
            world_name = result.get('world_name', 'N/A')
            target_item = result.get('target_item', 'N/A')
            date_create = result.get('date_create', 'N/A')

            if date_create != 'N/A':
                try:
                    date_create = datetime.fromisoformat(date_create.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    date_create = '0000-00-00 00:00:00'

            try:
                cursor.execute('''
                    INSERT INTO starforce (
                        id, item_upgrade_result, before_starforce_count, after_starforce_count, 
                        character_name, world_name, target_item, date_create
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        item_upgrade_result = VALUES(item_upgrade_result),
                        before_starforce_count = VALUES(before_starforce_count),
                        after_starforce_count = VALUES(after_starforce_count),
                        character_name = VALUES(character_name),
                        world_name = VALUES(world_name),
                        target_item = VALUES(target_item),
                        date_create = VALUES(date_create)
                ''', (id, item_upgrade_result, before_starforce_count, after_starforce_count,
                      character_name, world_name, target_item, date_create))
            except mysql.connector.Error as e:
                print(f"쿼리 실행 오류: {e}")
                print(f"실패한 데이터: {result}")

        conn.commit()
        print("데이터가 성공적으로 데이터베이스에 저장되었습니다.")
    except mysql.connector.Error as e:
        print(f"MySQL 오류: {e}")
    finally:
        conn.close()

def main():
    # 데이터베이스 및 테이블 생성
    create_table()

    # 조회할 데이터의 개수와 기준일 설정
    count = 100
    date = "2024-07-24"

    # 스타포스 강화 결과 데이터 가져오기
    data_list = get_starforce_data(count, date)

    if data_list:
        print(f"가져온 데이터: {data_list}")  # 디버깅용 출력
        insert_data(data_list)
    else:
        print("데이터를 가져오는 데 실패했습니다.")

if __name__ == "__main__":
    main()
