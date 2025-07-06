import pandas as pd
import oracledb # Sử dụng thư viện oracledb trực tiếp
import os
import logging

# Cấu hình logging để in thông tin ra console và file (tùy chọn)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- CẤU HÌNH KẾT NỐI ORACLE ---
# Thay thế 'Duythang123' bằng mật khẩu của schema BAI_TEST.
# Thay thế 'ord' bằng SID/Service Name chính xác của Oracle Database của bạn.
# Dựa trên hình ảnh SQL Developer của bạn, SID là "ord" và host là "localhost".
ORACLE_USER = "BAI_TEST"
ORACLE_PASSWORD = "Duythang123" # <--- THAY THẾ MẬT KHẨU CỦA BẠN VÀO ĐÂY
ORACLE_HOST = "localhost"
ORACLE_PORT = 1521
ORACLE_SERVICE_NAME_OR_SID = "ORCL" # <--- THAY ĐỔI THÀNH SID THỰC TẾ CỦA DATABASE CỦA BẠN

# Tạo chuỗi DSN (Data Source Name)
# Vì đã xác nhận là SID, hãy sử dụng 'sid=' trong oracledb.makedsn()
ORACLE_DSN = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SERVICE_NAME_OR_SID)

# --- CẤU HÌNH ĐƯỜNG DẪN THƯ MỤC DỮ LIỆU ---
# Thay thế đường dẫn này bằng đường dẫn THỰC TẾ đến thư mục chứa các tệp Excel/CSV của bạn
# Ví dụ: DATA_DIRECTORY_PATH = 'C:\\Users\\Admin\\airflow\\data'
DATA_DIRECTORY_PATH = 'D:\INDA' # <--- THAY THẾ ĐƯỜNG DẪN THƯ MỤC DỮ LIỆU CHÍNH XÁC CỦA BẠN VÀO ĐÂY KPI Modeling and DAX.xlsx,\KPI Template.xlsx
DATA_DIRECTORY_PATH = 'D:\INDA'
# --- ĐỊNH NGHĨA CẤU TRÚC BẢNG CỤ THỂ CHO TỪNG SHEET/FILE (TÙY CHỌN) ---
# Nếu có cấu trúc cứng, sẽ dùng cái này. Nếu không, sẽ tự động suy luận.
TABLE_DEFINITIONS = {
    "KHÁCH HÀNG": {
        "Mã_KH": "VARCHAR2(10)",
        "Khách_hàng": "VARCHAR2(255)"
    },
    "SẢN PHẨM": {
        "Mã_Sản_phẩm": "VARCHAR2(10)",
        "Sản_phẩm": "VARCHAR2(400)",
        "Nhóm_sản_phẩm": "VARCHAR2(50)"
    },
    "NHÂN VIÊN": {
        "Mã_nhân_viên_bán": "VARCHAR2(10)",
        "Nhân_viên_bán": "VARCHAR2(255)"
    },
    "DỮ LIỆU BÁN HÀNG": {
        "Ngày_hạch_toán": "DATE",
        "Đơn_hàng": "VARCHAR2(20)",
        "Mã_KH": "VARCHAR2(10)",
        "Mã_Sản_Phẩm": "VARCHAR2(10)",
        "Số_lượng_bán": "NUMBER",
        "Đơn_giá": "NUMBER",
        "Doanh_thu": "NUMBER",
        "Giá_vốn_hàng_hóa": "NUMBER",
        "Mã_nhân_viên_bán": "VARCHAR2(10)",
        "Chi_nhánh": "VARCHAR2(10)"
    },
    "CHI NHÁNH": {
        "Mã_chi_nhánh": "VARCHAR2(10)",
        "Tên_chi_nhánh": "VARCHAR2(255)",
        "Tỉnh_thành_phố": "VARCHAR2(50)"
    },
    "KPI THEO NĂM": {
        "Năm": "NUMBER",
        "Chi_nhánh": "VARCHAR2(50)",
        "KPI": "NUMBER"
    }
}

# --- HÀM HỖ TRỢ CHUẨN HÓA TÊN ---
def _normalize_name(name: str, max_length: int = 30) -> str:
    """Chuẩn hóa tên (bảng hoặc cột) cho Oracle."""
    normalized = name.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_")
    # Thay thế các ký tự có dấu tiếng Việt
    normalized = normalized.replace("é", "e").replace("è", "e").replace("à", "a").replace("á", "a").replace("í", "i").replace("ù", "u").replace("ú", "u")
    normalized = normalized.upper() # Chuyển thành chữ hoa
    return normalized[:max_length] # Cắt bớt nếu quá dài


# --- HÀM XỬ LÝ IMPORT TỪ MỘT DATAFRAME VÀO ORACLE ---
def _process_dataframe_to_oracle(df: pd.DataFrame, target_table_name: str, predefined_cols: dict = None):
    """
    Hàm nội bộ để xử lý một Pandas DataFrame và ghi nó vào bảng Oracle được chỉ định.
    Sẽ DROP và CREATE lại bảng để đảm bảo cấu trúc và dữ liệu mới nhất.
    """
    if df.empty:
        log.warning(f"DataFrame rỗng cho bảng '{target_table_name}'. Bỏ qua.")
        return

    conn = None
    cursor = None
    try:
        # Kết nối CSDL Oracle
        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
        cursor = conn.cursor()
        log.info(f"Đã kết nối Oracle cho bảng '{target_table_name}'.")

        # Chuẩn hóa tên bảng
        oracle_table_name = _normalize_name(target_table_name, max_length=30)
        
        # Chuẩn hóa tên cột trong DataFrame để khớp với Oracle
        df.columns = [_normalize_name(col, max_length=30) for col in df.columns]

        # Xây dựng DDL (Data Definition Language)
        columns_ddl = []
        
        # Nếu có định nghĩa cột cứng, ưu tiên dùng cái đó
        if predefined_cols:
            log.info(f"Sử dụng định nghĩa cột cứng cho bảng '{oracle_table_name}'.")
            for col_name, col_type in predefined_cols.items():
                normalized_col_name = _normalize_name(col_name, max_length=30)
                columns_ddl.append(f'"{normalized_col_name}" {col_type}')
            # Cần đảm bảo DataFrame có đủ các cột theo định nghĩa cứng, thêm cột thiếu với NaN
            for defined_col in [ _normalize_name(k) for k in predefined_cols.keys() ]:
                if defined_col not in df.columns:
                    df[defined_col] = None # Thêm cột thiếu nếu cần
        else:
            # Nếu không có định nghĩa cứng, suy luận từ DataFrame
            log.info(f"Suy luận kiểu dữ liệu cột cho bảng '{oracle_table_name}'.")
            for col in df.columns:
                if pd.api.types.is_string_dtype(df[col]):
                    columns_ddl.append(f'"{col}" VARCHAR2(4000)') # VARCHAR2(4000) cho chuỗi dài
                elif pd.api.types.is_integer_dtype(df[col]):
                    columns_ddl.append(f'"{col}" NUMBER(10)')
                elif pd.api.types.is_float_dtype(df[col]):
                    columns_ddl.append(f'"{col}" NUMBER') # NUMBER cho số thập phân
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    columns_ddl.append(f'"{col}" DATE')
                else:
                    columns_ddl.append(f'"{col}" VARCHAR2(4000)') # Mặc định

        create_table_query = f"CREATE TABLE \"{oracle_table_name}\" ({', '.join(columns_ddl)})"
        
        # DROP TABLE nếu tồn tại, sau đó CREATE TABLE mới
        try:
            log.info(f"Đang cố gắng DROP TABLE \"{oracle_table_name}\" nếu tồn tại...")
            cursor.execute(f'DROP TABLE "{oracle_table_name}" PURGE')
            log.info(f"Đã DROP TABLE \"{oracle_table_name}\".")
        except oracledb.DatabaseError as e:
            error_obj, = e.args
            if error_obj.code == 942: # ORA-00942: table or view does not exist
                log.info(f"Bảng \"{oracle_table_name}\" không tồn tại. Tiếp tục.")
            else:
                log.error(f"Lỗi khi DROP TABLE \"{oracle_table_name}\": {error_obj.message}")
                raise # Ném lỗi nếu là lỗi khác

        try:
            log.info(f"Đang tạo bảng \"{oracle_table_name}\" với DDL: {create_table_query}")
            cursor.execute(create_table_query)
            log.info(f"Đã tạo bảng \"{oracle_table_name}\" thành công.")
        except oracledb.DatabaseError as e:
            log.error(f"Lỗi tạo bảng \"{oracle_table_name}\": {e}", exc_info=True)
            raise # Ném lỗi

        # Chuẩn bị insert dữ liệu
        # Đảm bảo thứ tự cột cho câu lệnh INSERT khớp với DataFrame
        # Các cột trong câu lệnh INSERT phải có dấu nháy kép
        columns_for_insert = ", ".join([f'"{col}"' for col in df.columns])
        placeholders = ", ".join([f":{i+1}" for i in range(len(df.columns))])
        insert_query = f'INSERT INTO \"{oracle_table_name}\" ({columns_for_insert}) VALUES ({placeholders})'

        data_to_insert = []
        for index, row in df.iterrows():
            row_list = row.tolist()

            # Xử lý ngày tháng cho cột "NGÀY_HẠCH_TOÁN" nếu có
            normalized_date_col_name = _normalize_name("Ngày_hạch_toán")
            if normalized_date_col_name in df.columns and pd.api.types.is_datetime64_any_dtype(df[normalized_date_col_name]):
                 date_col_index = df.columns.get_loc(normalized_date_col_name)
                 if pd.isna(row_list[date_col_index]):
                     row_list[date_col_index] = None
                 else:
                     try:
                         # Chuyển đổi thành đối tượng date của Python để oracledb xử lý
                         row_list[date_col_index] = pd.to_datetime(row_list[date_col_index]).date()
                     except Exception as convert_err:
                         log.warning(f"Không thể chuyển đổi ngày '{row_list[date_col_index]}' trong dòng {index} sang định dạng DATE: {convert_err}")
                         row_list[date_col_index] = None # Đặt là None nếu lỗi chuyển đổi

            # Xử lý các giá trị NaN/NaT thành None (NULL trong Oracle)
            for i in range(len(row_list)):
                if pd.isna(row_list[i]):
                    row_list[i] = None
            
            data_to_insert.append(tuple(row_list))

        if data_to_insert:
            log.info(f"Đang chèn {len(data_to_insert)} dòng vào bảng \"{oracle_table_name}\"...")
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            log.info(f"Đã chèn {len(data_to_insert)} dòng vào bảng \"{oracle_table_name}\" thành công.")
        else:
            log.warning(f"Không có dữ liệu để chèn vào bảng \"{oracle_table_name}\" từ DataFrame.")

    except Exception as e:
        log.error(f"Lỗi khi chèn dữ liệu vào bảng \"{target_table_name}\": {e}", exc_info=True)
        if conn:
            conn.rollback() # Rollback nếu có lỗi
        raise # Ném lỗi để dừng quá trình
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            log.info(f"Đã đóng kết nối Oracle cho bảng '{target_table_name}'.")

# --- HÀM CHÍNH ĐỂ DUYỆT VÀ XỬ LÝ TẤT CẢ CÁC FILE TRONG THƯ MỤC ---
# --- HÀM CHÍNH ĐỂ DUYỆT VÀ XỬ LÝ TẤT CẢ CÁC FILE TRONG THƯ MỤC ---
def import_all_excel_csv_files_to_oracle_schema():
    """
    Hàm chính để duyệt qua tất cả các file Excel/CSV trong thư mục dữ liệu
    và import chúng vào Oracle Database Schema "BAI_TEST".
    """
    log.info(f"Bắt đầu quá trình import dữ liệu tổng thể từ '{DATA_DIRECTORY_PATH}' vào Oracle Schema '{ORACLE_USER}'...")
    
    if not os.path.exists(DATA_DIRECTORY_PATH):
        log.error(f"Thư mục dữ liệu không tìm thấy: '{DATA_DIRECTORY_PATH}'. Vui lòng kiểm tra đường dẫn.")
        raise FileNotFoundError(f"Thư mục dữ liệu không tìm thấy: '{DATA_DIRECTORY_PATH}'")

    found_files = False
    for filename in os.listdir(DATA_DIRECTORY_PATH):
        file_path = os.path.join(DATA_DIRECTORY_PATH, filename)
        
        if os.path.isfile(file_path):
            if filename.lower().endswith(('.xlsx', '.xls')):
                found_files = True
                log.info(f"Tìm thấy file Excel để import: '{filename}'")
                excel_file_obj = pd.ExcelFile(file_path)
                for sheet_name in excel_file_obj.sheet_names:
                    log.info(f"Đang xử lý sheet '{sheet_name}' trong file '{filename}'.")
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                    
                    # --- VỊ TRÍ BẠN CẦN THÊM ĐOẠN CODE XỬ LÝ DỮ LIỆU ĐÂY ---
                    if sheet_name.upper() == "KPI THEO NĂM":
                        if 'Năm' in df.columns: # Tùy chọn, nếu bạn cũng lo ngại cột Năm có vấn đề
                            df['Năm'] = pd.to_numeric(df['Năm'], errors='coerce')
                        if 'KPI' in df.columns:
                            # Loại bỏ dấu phẩy trước khi chuyển đổi sang số
                            df['KPI'] = df['KPI'].astype(str).str.replace(',', '', regex=False)
                            df['KPI'] = pd.to_numeric(df['KPI'], errors='coerce')
                    # ------------------------------------------------------------------

                    # Tên bảng sẽ lấy từ tên sheet
                    target_table_name = sheet_name 
                    
                    # Kiểm tra xem có định nghĩa cứng cho sheet này không
                    predefined_cols = TABLE_DEFINITIONS.get(target_table_name.upper())
                    _process_dataframe_to_oracle(df, target_table_name, predefined_cols)

            elif filename.lower().endswith('.csv'):
                found_files = True
                log.info(f"Tìm thấy file CSV để import: '{filename}'")
                df = pd.read_csv(file_path)
                
                # Bạn cũng có thể muốn thêm xử lý tương tự cho các file CSV nếu cần
                # Ví dụ: if os.path.splitext(filename)[0].upper() == "TEN_FILE_CSV_CUA_BAN":
                #    df['COT_SO'] = pd.to_numeric(df['COT_SO'].astype(str).str.replace(',', '', regex=False), errors='coerce')

                # Tên bảng sẽ lấy từ tên file CSV (không có phần mở rộng)
                target_table_name = os.path.splitext(filename)[0]
                
                # Kiểm tra xem có định nghĩa cứng cho tên file này không
                predefined_cols = TABLE_DEFINITIONS.get(target_table_name.upper())
                _process_dataframe_to_oracle(df, target_table_name, predefined_cols)
            else:
                log.debug(f"Bỏ qua file không phải Excel/CSV: '{filename}'")
    
    if not found_files:
        log.warning(f"Không tìm thấy file Excel/CSV nào trong thư mục '{DATA_DIRECTORY_PATH}'.")

    log.info("Quá trình import dữ liệu tổng thể đã hoàn thành.")

# Điểm khởi chạy của script khi chạy trực tiếp
if __name__ == "__main__":
    try:
        import_all_excel_csv_files_to_oracle_schema()
        print("\nScript đã chạy xong. Vui lòng kiểm tra dữ liệu trong Oracle Database.")
    except Exception as final_e:
        print(f"\nScript gặp lỗi nghiêm trọng: {final_e}")