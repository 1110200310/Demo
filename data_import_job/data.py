import pandas as pd
import numpy as np
import re

# Đọc dữ liệu từ CSV
df = pd.read_csv(r'C:\Users\Admin\airflow\dags\data_import_job\data.csv')
print("Raw data:\n", df.head())

# Hàm xử lý salary
def parse_salary(s):
    if pd.isnull(s) or 'thoả thuận' in s.lower():
        return pd.Series([np.nan, np.nan, None])
    
    s = s.lower()
    unit = 'VND'
    if 'usd' in s:
        unit = 'USD'
    
    nums = re.findall(r'\d+', s.replace(',', ''))
    nums = [int(n) for n in nums]
    
    if 'trên' in s or 'từ' in s:
        return pd.Series([nums[0], np.nan, unit])
    elif 'tới' in s or 'đến' in s:
        return pd.Series([np.nan, nums[0], unit])
    elif '-' in s and len(nums) == 2:
        return pd.Series([nums[0], nums[1], unit])
    elif len(nums) == 1:
        return pd.Series([nums[0], nums[0], unit])
    else:
        return pd.Series([np.nan, np.nan, None])

# Áp dụng
df[['min_salary', 'max_salary', 'salary_unit']] = df['salary'].apply(parse_salary)

# Tách address
def parse_address(addr):
    if pd.isnull(addr):
        return pd.Series([None, None])
    addr = addr.strip()
    # Dùng re.split với các dấu phân cách thông dụng, loại bỏ các phần tử trống
    parts = [p.strip() for p in re.split(r'[,/-:]', addr) if p.strip()]
    
    city = None
    district = None

    if len(parts) >= 1:
        # Phần tử cuối cùng thường là Tỉnh/Thành phố
        city = parts[-1]
    if len(parts) >= 2:
        # Phần tử kế cuối thường là Quận/Huyện
        district = parts[-2]
    
    # Kiểm tra và điều chỉnh một số trường hợp đặc biệt
    common_cities = ["hồ chí minh", "hà nội", "đà nẵng", "hải phòng", "cần thơ", "huế", "hải dương", "hưng yên", "bắc ninh", "thái nguyên", "vĩnh phúc", "quảng ninh"] # Thêm một số thành phố/tỉnh phổ biến
    common_districts_in_hanoi = ["ba đình", "hoàn kiếm", "đống đa", "hà đông", "cầu giấy", "nam từ liêm", "hai bà trưng", "thanh xuân", "long biên"] # Thêm một số quận phổ biến
    common_districts_in_hcm = ["quận 1", "quận 2", "quận 3", "quận 4", "quận 5", "quận 6", "quận 7", "quận 8", "quận 9", "quận 10", "quận 11", "quận 12", "tân bình", "bình thạnh", "thủ đức", "gò vấp", "phú nhuận", "bình tân", "tân phú"] # Thêm một số quận phổ biến
    common_districts_in_hai_duong = ["thành phố hải dương", "cẩm giàng", "bình giang", "chí linh", "kinh môn"] # Thêm các quận/huyện của Hải Dương

    if city and district:
        city_lower = city.lower().replace("thành phố", "").strip() # Loại bỏ "thành phố" để so sánh tên
        district_lower = district.lower().replace("thành phố", "").strip()

        # Nếu district là một thành phố phổ biến VÀ city là một district phổ biến của thành phố đó
        if district_lower in common_cities and city_lower not in common_cities and \
           (city_lower in common_districts_in_hanoi or city_lower in common_districts_in_hcm or city_lower in common_districts_in_hai_duong or f"tp {city_lower}" == district_lower): # Thêm điều kiện TP Hải Dương
            city, district = district, city # Hoán đổi

        # Trường hợp "TP Hải Dương" (là một district của Hải Dương) lại nằm ở cột city
        if city_lower == "hải dương" and district_lower == "thành phố hải dương":
            pass # Giữ nguyên nếu city là Hải Dương và district là TP Hải Dương
        elif city_lower == "thành phố hải dương" and district_lower == "hải dương": # Đây là trường hợp bị nhầm
             city, district = district, city # Hoán đổi lại thành city=Hải Dương, district=TP Hải Dương

    return pd.Series([city, district])

df[['city', 'district']] = df['address'].apply(parse_address)

# Gom nhóm job_title
def normalize_job_title(title):
    if pd.isnull(title):
        return 'Khác'
    title = title.lower().strip()
    if any(keyword in title for keyword in ['senior', 'cao cấp']):
        level = 'Senior '
    elif any(keyword in title for keyword in ['junior', 'fresher', 'thực tập']):
        level = 'Junior '
    else:
        level = ''

    if any(keyword in title for keyword in ['developer', 'lập trình', '.net', 'dev', 'full-stack', 'frontend', 'backend', 'java', 'vuejs', 'c/embedded', 'engineer', 'programmer', 'coder']):
        return level + 'Lập trình viên'
    elif any(keyword in title for keyword in ['business analyst', 'ba', 'it business analyst']):
        return level + 'Business Analyst'
    elif any(keyword in title for keyword in ['project manager', 'pm', 'quản lý dự án', 'tech lead', 'product owner', 'scrum master']):
        return level + 'Project Manager'
    elif any(keyword in title for keyword in ['ai', 'machine learning', 'deep learning', 'ml']):
        return level + 'AI Engineer'
    elif any(keyword in title for keyword in ['tester', 'qa', 'kiểm thử']):
        return level + 'Tester / QA'
    elif any(keyword in title for keyword in ['devops', 'sre', 'system admin', 'infra', 'admin']):
        return level + 'DevOps / System Admin'
    elif any(keyword in title for keyword in ['data', 'bi', 'big data']):
        return level + 'Data / BI'
    elif any(keyword in title for keyword in ['it helpdesk', 'support', 'cộng tác viên it', 'technical support']):
        return level + 'IT Support'
    elif any(keyword in title for keyword in ['marketing', 'communication', 'designer']):
        return level + 'Marketing / Design'
    elif any(keyword in title for keyword in ['secretary']):
        return level + 'Administrative'
    else:
        return 'Khác'

df['job_group'] = df['job_title'].apply(normalize_job_title)
print("Normalized job titles:\n", df[['job_title', 'job_group']].head(20))
df['job_title'] = df['job_group']
df.drop(columns=['job_group'], inplace=True)

# Lưu kết quả
df.to_excel(r'C:\Users\Admin\airflow\dags\data_import_job\data_clean.xlsx', index=False, engine='openpyxl')
print(r"Data cleaned and saved successfully at C:\Users\Admin\airflow\dags\data_import_job\data_clean.xlsx!")