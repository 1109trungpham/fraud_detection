from confluent_kafka import Consumer, KafkaError
import json
from colorama import Fore, Style, init

# Khởi tạo colorama để in màu cho terminal
init(autoreset=True)

def start_rule_engine():
    # Cấu hình Consumer
    # Chú ý:
    # Nếu chạy từ máy Mac (ngoài Docker), hãy dùng localhost:9092
    # Nếu chạy bên trong mạng Docker, hãy dùng kafka:9094
    conf = {
        'bootstrap.servers': 'localhost:9092', 
        'group.id': 'fraud-detector-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    consumer = Consumer(conf)
    
    # Đăng ký nhận dữ liệu từ topic enriched
    topic = 'transactions_enriched'
    consumer.subscribe([topic])

    print(f"{Fore.CYAN}=== FRAUD RULE ENGINE (CONFLUENT) IS RUNNING ==={Style.RESET_ALL}")
    print(f"Listening for enriched transactions on topic: {topic}...\n")

    try:
        while True:
            # Đợi tin nhắn mới trong vòng 1 giây
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Giải mã dữ liệu JSON
            try:
                tx = json.loads(msg.value().decode('utf-8'))
                tx_id = tx.get('tx_id')
                account_id = tx.get('account_id')
                
                alerts = []

                # --- HỆ THỐNG LUẬT (RULES) ---

                # RULE 1: Tài khoản có rủi ro cao thực hiện giao dịch lớn (>10 triệu)
                if tx.get('customer_risk') == 'HIGH' and tx.get('amount', 0) > 10000:
                    alerts.append(f"⚠️  High Risk Customer: {tx.get('customer_risk')} | Amount: {tx.get('amount')}")

                # RULE 2: Tần suất giao dịch quá nhanh (Velocity)
                if tx.get('tx_count_1m', 0) > 5:
                    alerts.append(f"🚨 High Velocity detected: {tx.get('tx_count_1m')} tx/min")

                # RULE 3: Vượt hạn mức chi tiêu 5 phút (hạn mức 50 triệu)
                if tx.get('amount_sum_5m', 0) > 50000:
                    alerts.append(f"💰 Spending Threshold Exceeded: {tx.get('amount_sum_5m')} in 5 mins")

                # RULE 4: Di chuyển bất khả thi (Impossible Travel)
                last_loc = tx.get('last_location')
                curr_loc = tx.get('location')
                if last_loc and curr_loc and last_loc != curr_loc:
                    alerts.append(f"✈️  Impossible Travel: {last_loc} -> {curr_loc}")

                # RULE 5: Phát hiện chuỗi giao dịch thất bại
                if tx.get('failed_tx_5m', 0) > 0:
                    alerts.append(f"❌ Recent failed transactions in window: {tx.get('failed_tx_5m')}")

                # --- XUẤT KẾT QUẢ ---
                if alerts:
                    print(f"{Fore.RED}[ALERT] TX_ID: {tx_id} | Account: {account_id}")
                    for a in alerts:
                        print(f"  - {a}")
                    print("-" * 60)
                else:
                    # In các giao dịch an toàn với màu xanh lá
                    print(f"{Fore.GREEN}[SAFE] TX_ID: {tx_id} | Account: {account_id} - Clean Transaction")
                    print("-" * 60)

            except json.JSONDecodeError:
                print(f"{Fore.YELLOW}Could not decode message: {msg.value()}")

    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Stopping Rule Engine...")
    finally:
        # Đóng consumer an toàn
        consumer.close()

if __name__ == "__main__":
    start_rule_engine()