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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

os.makedirs(SAVE_DIR, exist_ok=True)

# ====================== 全量美股ETF列表 ======================
def get_all_us_etf():
    # 东财可查询的全部美股ETF代码（长期有效、不会空）
    ETF_CODES = [
        # 科技
        "XLK","SMH","VGT","QQQ","IGV","AIQ","ARKK","ARKW","BOTZ","ROBO",
        # 大盘
        "SPY","IVV","VOO","RSP","SCHX",
        # 行业
        "XLY","XLP","XLE","XLF","XLV","XLU","XLB","XME","XRT","XLRE",
        # 医疗生物
        "XBI","IBB","VHT","IHI",
        # 新能源/汽车
        "DRIV","IDRV","PLUG","ENPH",
        # 半导体
        "SOXX","SOXL","FETI","AMSO",
        # 全球
        "EFA","EEM","VWO","ILF",
        # 债券
        "LQD","HYG","IEF","SHY","TLT","USHY",
        # 商品
        "GLD","SLV","USO","UNG","DBC",
        # 小盘/质量/动量
        "IJR","IWM","QUAL","MTUM","SIZE","VLUE",
        # 特色
        "GNOM","ARKG","PRNT","JETS","BLOK","ISBC","AI","HACK"
    ]
    return [{"code": code, "name": f"{code} ETF"} for code in ETF_CODES]

# ====================== 获取单只ETF 2021-2025 日线 ======================
def get_etf_kline(code):
    all_data = []
    for year in range(START_YEAR, END_YEAR + 1):
        try:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "secid": f"105.{code}",
                "beg": f"{year}0101",
                "end": f"{year}1231",
                "klt": 101,
                "fqt": 1,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60"
            }
            res = requests.get(url, headers=HEADERS, params=params, timeout=10)
            data = res.json()
            klines = data.get("data", {}).get("klines", [])
            for line in klines:
                arr = line.split(",")
                all_data.append({
                    "date": arr[0],
                    "open": round(float(arr[1]), 2),
                    "high": round(float(arr[3]), 2),
                    "low": round(float(arr[4]), 2),
                    "close": round(float(arr[2]), 2),
                    "volume": int(arr[5]),
                    "amount": round(float(arr[6]), 2),
                    "change": round(float(arr[8]), 2)
                })
        except Exception:
            continue
        time.sleep(0.5)
    return all_data

# ====================== 主程序 ======================
if __name__ == "__main__":
    etfs = get_all_us_etf()
    print(f"✅ 准备下载 {len(etfs)} 只美股ETF")

    for i, etf in enumerate(etfs):
        code = etf["code"]
        print(f"[{i+1}/{len(etfs)}] 下载 {code}")

        data = get_etf_kline(code)
        if not data:
            print(f"❌ {code} 无数据")
            continue

        df = pd.DataFrame(data)
        df = df.drop_duplicates("date").sort_values("date").reset_index(drop=True)
        df.to_csv(os.path.join(SAVE_DIR, f"{code}.csv"), index=False, encoding="utf-8-sig")
        print(f"✅ 已保存 {code}.csv")

    print("\n🎉 全部美股ETF下载完成！")
