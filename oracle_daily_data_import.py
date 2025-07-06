from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pendulum # Thư viện khuyên dùng cho múi giờ và ngày tháng trong Airflow

# Cấu hình Airflow DAG
DAG_ID = "oracle_daily_data_import"
DAG_DESCRIPTION = "DAG tự động import dữ liệu từ Excel/CSV vào Oracle Database mỗi ngày."
SCHEDULE_INTERVAL = "0 10 * * *" # Chạy vào 10:00 sáng hàng ngày (phút giờ ngày_tháng tháng ngày_trong_tuần)
START_DATE = days_ago(1) # Bắt đầu từ ngày hôm qua để lịch trình được kích hoạt ngay

# Định nghĩa các biến môi trường nếu cần thiết cho script Python của bạn
# Hoặc đảm bảo script của bạn tự đọc từ file cấu hình của nó
# Ở đây ta giả định script đã tự chứa các thông tin kết nối

# Đường dẫn đến script Python của bạn BÊN TRONG CONTAINER AIRFLOW.
# Đảm bảo thư mục dags cục bộ của bạn được ánh xạ vào /opt/airflow/dags trong Docker.
PYTHON_SCRIPT_PATH = "/opt/airflow/dags/data_import/import_to_oracle.py"

# Nếu bạn đang chạy trên Windows và Python của bạn nằm trong C:\Users\Admin\AppData\Local\Programs\Python\Python310\python.exe
# Bạn sẽ cần chỉ rõ đường dẫn đến executable của Python.
# Đối với Docker (Linux), đường dẫn này là đường dẫn trong container.
PYTHON_EXECUTABLE = "/usr/local/bin/python" # Đã sửa để chỉ định đường dẫn cụ thể trong container

with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False, # Không chạy lại các lượt bị bỏ lỡ trong quá khứ
    tags=["oracle", "import", "daily"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1, # Thử lại 1 lần nếu thất bại
        "retry_delay": pendulum.duration(minutes=5), # Đợi 5 phút trước khi thử lại
    }
) as dag:
    
    run_import_script = BashOperator(
        task_id="run_python_import_script",
        bash_command=f"{PYTHON_EXECUTABLE} {PYTHON_SCRIPT_PATH}",
        # Nếu script của bạn có in ra log, chúng sẽ xuất hiện trong log của Airflow Task
    )

    # Nếu bạn muốn thêm các bước khác, bạn có thể định nghĩa chúng ở đây
    # Ví dụ:
    # check_data_quality = PythonOperator(...)
    # run_import_script >> check_data_quality