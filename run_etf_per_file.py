# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
import os

# ====================== 配置 ======================
START_YEAR = 2021
END_YEAR   = 2025
SAVE_DIR   = "etf_data"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

os.makedirs(SAVE_DIR, exist_ok=True)

# ====================== 【修复】获取美股ETF列表 ======================
def get_us_etf_list():
    etf_list = []
    print("正在获取美股ETF列表...")

    for page in range(1, 20):
        try:
            url = "https://62.push2.eastmoney.com/api/qt/clist/get"
            params = {
                "pn": page,
                "pz": 100,
                "fs": "b:MK0011",  # 【修复】美股ETF正确代码
                "fields": "f12,f14",
            }
            res = requests.get(url, headers=HEADERS, params=params, timeout=10)
            data = res.json()

            if not data.get("data", {}).get("diff"):
                break

            for item in data["data"]["diff"]:
                code = item["f12"].strip()
                name = item["f14"].strip()
                etf_list.append({"code": code, "name": name})

            time.sleep(0.3)
        except Exception as e:
            break

    print(f"✅ 共获取 {len(etf_list)} 只美股ETF")
    return etf_list

# ====================== 获取单只ETF 2021-2025全部日线 ======================
def get_etf_full_data(code):
    all_days = []
    for year in range(START_YEAR, END_YEAR + 1):
        try:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "secid": f"105.{code}",
                "beg": f"{year}0101",
                "end": f"{year}1231",
                "klt": 101,
                "fqt": 1,    # 前复权
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60"
            }
            res = requests.get(url, headers=HEADERS, params=params, timeout=8)
            data = res.json()
            klines = data["data"]["klines"]
            for line in klines:
                arr = line.split(",")
                all_days.append({
                    "date": arr[0],
                    "open": round(float(arr[1]), 2),
                    "close": round(float(arr[2]), 2),
                    "high": round(float(arr[3]), 2),
                    "low": round(float(arr[4]), 2),
                    "volume": int(arr[5]),
                    "amount": float(arr[6]),
                    "change_pct": float(arr[8])
                })
        except Exception as e:
            continue
        time.sleep(0.5)
    return all_days

# ====================== 主程序 ======================
if __name__ == "__main__":
    etf_list = get_us_etf_list()
    for idx, etf in enumerate(etf_list):
        code = etf["code"]
        name = etf["name"]
        print(f"[{idx+1}/{len(etf_list)}] 下载 {code} {name}")

        data = get_etf_full_data(code)
        if not data:
            print(f"❌ {code} 无数据")
            continue

        df = pd.DataFrame(data)
        df = df.sort_values("date").reset_index(drop=True)
        save_path = os.path.join(SAVE_DIR, f"{code}.csv")
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"✅ 已保存：{save_path}")

    print("\n🎉 全部下载完成！")
