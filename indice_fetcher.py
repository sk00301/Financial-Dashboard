import pandas as pd
import time
import os
from datetime import datetime, timedelta
from nselib import capital_market

# --- DEFINING THE LISTS ---
INDIAN_INDICES = {
    'NIFTY 50': '^NSEI', 'NIFTY NEXT 50': '^NSMIDCP', 'NIFTY 100': '^CNX100', 
    'NIFTY 200': '^CNX200', 'NIFTY 500': '^CRSLDX', 'NIFTY MIDCAP 50': '^NSEMDCP50', 
    'NIFTY MIDCAP 100': '^CNXMID', 'NIFTY MIDCAP 150': 'NIFTY_MIDCAP_150.NS', 
    'NIFTY SMALLCAP 50': 'NIFTY_SMALLCAP_50.NS', 'NIFTY SMALLCAP 100': '^CNXSC', 
    'NIFTY SMALLCAP 250': 'NIFTY_SMALLCAP_250.NS', 'NIFTY MIDSMALLCAP 400': 'NIFTY_MIDSML_400.NS', 
    'NIFTY LARGEMIDCAP 250': 'NIFTY_LARGEMID_250.NS', 'NIFTY TOTAL MARKET': 'NIFTY_TOTAL_MKT.NS', 
    'NIFTY MICROCAP 250': 'NIFTY_MICROCAP_250.NS', 'NIFTY BANK': '^NSEBANK', 
    'NIFTY AUTO': '^CNXAUTO', 'NIFTY FINANCIAL SERVICES': 'NIFTY_FIN_SERVICE.NS', 
    'NIFTY FMCG': '^CNXFMCG', 'NIFTY IT': '^CNXIT', 'NIFTY MEDIA': '^CNXMEDIA', 
    'NIFTY METAL': '^CNXMETAL', 'NIFTY PHARMA': '^CNXPHARMA', 'NIFTY PSU BANK': '^CNXPSUBANK', 
    'NIFTY PRIVATE BANK': 'NIFTY_PVT_BANK.NS', 'NIFTY REALTY': '^CNXREALTY', 
    'NIFTY HEALTHCARE': 'NIFTY_HEALTHCARE.NS', 'NIFTY CONSUMER DURABLES': 'NIFTY_CONSR_DURBL.NS', 
    'NIFTY OIL & GAS': 'NIFTY_OIL_GAS.NS', 'NIFTY COMMODITIES': '^CNXCMDT', 
    'NIFTY INDIA CONSUMPTION': '^CNXCONSUM', 'NIFTY CPSE': '^CPSE', 
    'NIFTY ENERGY': '^CNXENERGY', 'NIFTY INFRASTRUCTURE': '^CNXINFRA', 
    'NIFTY MNC': '^CNXMNC', 'NIFTY PSE': '^CNXPSE', 'NIFTY SERVICES SECTOR': '^CNXSERVICE', 
    'NIFTY INDIA DIGITAL': 'NIFTY_IND_DIGITAL.NS', 'NIFTY INDIA MANUFACTURING': 'NIFTY_IND_MFG.NS', 
    'NIFTY INDIA DEFENCE': 'NIFTY_INDIA_DEFENCE.NS', 'NIFTY TRANSPORTATION & LOGISTICS': 'NIFTY_TRNSP_LOGST.NS', 
    'NIFTY HOUSING': 'NIFTY_HOUSING.NS', 'NIFTY MOBILITY': 'NIFTY_MOBILITY.NS', 
    'NIFTY EV & NEW AGE AUTOMOTIVE': 'NIFTYEV.NS', 'NIFTY100 ESG': 'NIFTY100ESG.NS', 
    'NIFTY CORE HOUSING': 'NIFTYCOREHOUSING.NS'
}

def fetch_index_data(index_name, start_year, end_year):
    all_data = []
    current_start = datetime(start_year, 1, 1)
    final_end = min(datetime(end_year, 12, 31), datetime.now())
    chunk_size = 60 # Days per request

    while current_start <= final_end:
        current_end = current_start + timedelta(days=chunk_size)
        if current_end > final_end:
            current_end = final_end

        str_start = current_start.strftime('%d-%m-%Y')
        str_end = current_end.strftime('%d-%m-%Y')

        try:
            df_chunk = capital_market.index_data(index=index_name, from_date=str_start, to_date=str_end)
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)
                print(f"  ✓ {index_name} | {str_start} to {str_end} | +{len(df_chunk)} rows")
            time.sleep(0.6) 
        except Exception as e:
            print(f"  ✗ Error for {index_name}: {e}")

        current_start = current_end + timedelta(days=1)

    if not all_data: return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    final_df['TIMESTAMP'] = pd.to_datetime(final_df['TIMESTAMP'], dayfirst=True, errors='coerce')
    final_df = final_df.dropna(subset=['TIMESTAMP']).drop_duplicates(subset=['TIMESTAMP']).sort_values('TIMESTAMP')
    return final_df

# ---------------- EXECUTION ---------------- #

output_folder = "NSE_Indices_Data"
if not os.path.exists(output_folder): os.makedirs(output_folder)

# We loop through the dictionary keys
for idx_name in INDIAN_INDICES.keys():
    print(f"\n🚀 Fetching Data for: {idx_name}")
    df = fetch_index_data(idx_name, 2000, 2026)
    
    if not df.empty:
        # Format date for CSV and save
        df['TIMESTAMP'] = df['TIMESTAMP'].dt.strftime('%Y-%m-%d')
        safe_name = idx_name.replace(" ", "_").replace("/", "_")
        df.to_csv(f"{output_folder}/{safe_name}.csv", index=False)
        print(f"✅ Saved to {output_folder}/{safe_name}.csv")
    else:
        print(f"⚠ No data found for {idx_name}")

print("\n✨ Process Complete!")