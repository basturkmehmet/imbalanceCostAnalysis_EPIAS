import pandas as pd
import matplotlib.pyplot as plt
import requests
import json

df = pd.read_excel('Gain Enerji Intern Analyst Case Study Data_2024.xlsx')
df.fillna(value=0, inplace=True)
#df.drop(['gereksiz_sütun1', 'gereksiz_sütun2'], axis=1, inplace=True)
# Tarih-saat sütununu pandas datetime objesine çevirme
df['Tarih'] = pd.to_datetime(df['Tarih'])

# Base URL
base_url = "https://seffaflik.epias.com.tr/electricity-service"

# PTF ve SMF için endpointler
ptf_endpoint = "/v1/markets/dam/data/mcp"
smf_endpoint = "/v1/markets/bpm/data/system-marginal-price"

# İstek için header ve body bilgileri
headers = {"Content-Type": "application/json"}
body = {
    "startDate": "2023-01-01T00:00:00+03:00",
    "endDate": "2023-12-31T00:00:00+03:00",
    "page": {
        "number": 1,
        "size": 8761,  # İhtiyacınıza göre ayarlayabilirsiniz
        "total": 100,  # İhtiyacınıza göre ayarlayabilirsiniz
        "sort": {
            "field": "date",
            "direction": "ASC"
        }
    }
}

# PTF verisini çekmek için POST isteği
ptf_response = requests.post(base_url + ptf_endpoint, headers=headers, data=json.dumps(body))
# Yanıtı JSON formatında al
ptf_data = ptf_response.json()

# SMF verisini çekmek için POST isteği
smf_response = requests.post(base_url + smf_endpoint, headers=headers, data=json.dumps(body))
# Yanıtı JSON formatında al
smf_data = smf_response.json()

print("PTF Data:", json.dumps(ptf_data, indent=4))
print("SMF Data:", json.dumps(smf_data, indent=4))

ptf_df = pd.DataFrame(ptf_data['items'])
smf_df = pd.DataFrame(smf_data['items'])

with pd.ExcelWriter('EPİAŞ_Data_Analysis.xlsx', engine='openpyxl') as writer:
    ptf_df.to_excel(writer, sheet_name='PTF Verileri', index=False)
    smf_df.to_excel(writer, sheet_name='SMF Verileri', index=False)

print('Veriler Excel dosyasına başarıyla kaydedildi.')
# separate sheets
with pd.ExcelWriter('Dengesizlik_Analizi_Sonuclari.xlsx') as writer:
    df_ptf = pd.read_excel('EPİAŞ_Data_Analysis.xlsx', sheet_name='PTF Verileri')
    df_smf = pd.read_excel('EPİAŞ_Data_Analysis.xlsx', sheet_name='SMF Verileri')
    price_ptf = df_ptf['price']
    price_smf = df_smf['systemMarginalPrice']

    # dengesizlik fiyatı hesabı
    ptf_min_smf = pd.concat([price_ptf, price_smf], axis=1).min(axis=1)
    ptf_max_smf = pd.concat([price_ptf, price_smf], axis=1).max(axis=1)

    for sheet_name in ["Wind_1", "Wind_2", "Hydro_1", "Hydro_2"]:

        df_prod = pd.read_excel('Gain Enerji Intern Analyst Case Study Data_2024.xlsx', sheet_name=sheet_name)

        df_ptf['date'] = pd.to_datetime(df_ptf['date']).dt.tz_localize(None)

        # Align PTF prices with the production data index
        df_prod = df_prod.merge(df_ptf[['date', 'price']], left_on='Tarih', right_on='date', how='left')

        # Convert 'Tarih' column to datetime without timezone
        df_prod['Tarih'] = pd.to_datetime(df_prod['Tarih']).dt.tz_localize(None) + pd.to_timedelta(df_prod['Saat'], unit='h')


        df_ptf['Pozitif_Dengesizlik_Fiyati'] = ptf_min_smf * 0.97
        df_ptf['Negatif_Dengesizlik_Fiyati'] = ptf_max_smf * 1.03


        df_prod['Tarih'] = pd.to_datetime(df_prod['Tarih']).dt.tz_localize(None)
        df_ptf['date'] = pd.to_datetime(df_ptf['date']).dt.tz_localize(None)

        # Merge the PTF data with the production data based on the 'Tarih' column
        df_prod = df_prod.merge(df_ptf[['date', 'Pozitif_Dengesizlik_Fiyati', 'Negatif_Dengesizlik_Fiyati']], left_on='Tarih', right_on='date', how='left')
        df_prod['Tarih'] = pd.to_datetime(df_prod['Tarih']) + pd.to_timedelta(df_prod['Saat'], unit='h')

        # Adjusting the column names according to the provided structure for imbalance calculation
        df_prod['Dengesizlik'] = df_prod['Gerçekleşen Üretim  (MWh)'] + df_prod['Gün İçi Üretim Tahmini Revizesi (MWh)'] - (df_prod['Gün Öncesi Üretim Tahmini (MWh)'] )

        # dengesizlik tutarı hesabı
        df_prod['Dengesizlik_Tutari'] = 0.0

        df_prod.loc[df_prod['Dengesizlik'] > 0, 'Dengesizlik_Tutari'] = df_prod['Dengesizlik'] * df_prod['Pozitif_Dengesizlik_Fiyati']
        df_prod.loc[df_prod['Dengesizlik'] < 0, 'Dengesizlik_Tutari'] = df_prod['Dengesizlik'] * df_prod['Negatif_Dengesizlik_Fiyati']

        # max pot gelir
        df_prod['Maksimum_Potansiyel_Gelir'] = price_ptf * df_prod["Gün Öncesi Üretim Tahmini (MWh)"]

        df_prod['Gün_İçi_Piyasası_Geliri'] = df_prod["Gün İçi Üretim Tahmini Revizesi (MWh)"] * price_smf

        # Toplam Üretim (Satış) Gelirini doğru bir şekilde hesaplamak için:
        df_prod['Toplam_Uretim_Satis_Geliri'] = df_prod['Maksimum_Potansiyel_Gelir'] + df_prod['Gün_İçi_Piyasası_Geliri'] + df_prod['Dengesizlik_Tutari']

        df_prod['Birim_Üretim_Geliri'] = df_prod.apply(
            lambda row: row['Toplam_Uretim_Satis_Geliri'] / row['Gerçekleşen Üretim  (MWh)'] if row['Gerçekleşen Üretim  (MWh)'] != 0 else 0,axis=1)

        # Dengesizlik Maliyeti
        df_prod['Dengesizlik_Maliyeti'] = (df_prod['Maksimum_Potansiyel_Gelir'] - df_prod['Toplam_Uretim_Satis_Geliri'])

        # Birim Dengesizlik Maliyeti
        df_prod['Birim_Dengesizlik_Maliyeti'] = df_prod.apply(
            lambda row: row['Dengesizlik_Maliyeti'] / row['Gerçekleşen Üretim  (MWh)'] if row[ 'Gerçekleşen Üretim  (MWh)'] != 0 else 0,axis=1)



        df_prod['Tarih'] = pd.to_datetime(df_prod['Tarih'])  # Convert 'Tarih' to datetime if it's not already
        df_prod.set_index('Tarih', inplace=True, drop=False)  # Set 'Tarih' as the index
        monthly_data = df_prod.resample('ME').mean()
        annual_data = df_prod.resample('YE').mean()

        # Plotting 'Birim_Üretim_Geliri'

        # Convert DataFrame index to numpy datetime64 array and column values to numpy array for plotting
        monthly_dates = monthly_data.index.to_numpy()
        monthly_values = monthly_data['Birim_Üretim_Geliri'].to_numpy()
        annual_dates = annual_data.index.to_numpy()
        annual_values = annual_data['Birim_Üretim_Geliri'].to_numpy()
        latest_date = df_prod.index.max()
        if latest_date.year == 2024:
            df_prod = df_prod[df_prod.index.year <= 2023]

        plt.plot(monthly_dates, monthly_values, label='Monthly Avg', marker='o')
        plt.plot(annual_dates, annual_values, label='Annual Avg', linestyle='--', marker='x')
        plt.title(f'Birim Üretim Geliri - {sheet_name}')
        plt.xlabel('Date')
        plt.ylabel('Birim Üretim Geliri')
        plt.xlim(pd.Timestamp('2023-01-01'), latest_date)
        plt.legend()
        plt.show()

        # Plotting 'Birim_Dengesizlik_Maliyeti'
        plt.figure(figsize=(10, 7))
        # Convert DataFrame index to numpy datetime64 array and column values to numpy array for plotting
        monthly_imbalance_values = monthly_data['Birim_Dengesizlik_Maliyeti'].to_numpy()
        annual_imbalance_values = annual_data['Birim_Dengesizlik_Maliyeti'].to_numpy()

        plt.plot(monthly_dates, monthly_imbalance_values, label='Monthly Avg', marker='o')
        plt.plot(annual_dates, annual_imbalance_values, label='Annual Avg', linestyle='--', marker='x')
        plt.title(f'Birim Dengesizlik Maliyeti - {sheet_name}')
        plt.xlabel('Date')
        plt.ylabel('Birim Dengesizlik Maliyeti')
        plt.xlim(pd.Timestamp('2023-01-01'), latest_date)
        plt.legend()
        plt.show()

        df_prod['Çeyrek'] = df_prod.index.to_period('Q')

        # Çeyreklik hesaplama
        ceyreklik_ortalama = df_prod.groupby('Çeyrek')['Dengesizlik_Maliyeti'].mean()

        plt.figure(figsize=(10, 6))
        ceyreklik_ortalama.plot(kind='bar')
        plt.title(f'Çeyreklik Ortalama Dengesizlik Maliyeti -{sheet_name}')
        plt.xlabel('Çeyrek')
        plt.ylabel('Ortalama Dengesizlik Maliyeti')
        plt.xticks(rotation=45)

        plt.show()


        # 'Toplam_Üretim_Satış_Geliri' için çeyreklik ortalama hesaplama
        df_prod['Çeyrek'] = df_prod.index.to_period('Q')
        toplam_gelir_ceyreklik_ortalama = df_prod.groupby('Çeyrek')['Toplam_Uretim_Satis_Geliri'].mean()

        # Görselleştirme
        plt.figure(figsize=(10, 6))
        toplam_gelir_ceyreklik_ortalama.plot(kind='bar')
        plt.title(f'Çeyreklik Ortalama Toplam Üretim Satış Geliri -{sheet_name}')
        plt.xlabel('Çeyrek')
        plt.ylabel('Ortalama Toplam Üretim Satış Geliri')
        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='--')
        plt.show()

        df_prod.drop(['price', 'date_x', 'date_y'], axis=1, inplace=True)
        df_prod.to_excel(writer, sheet_name=sheet_name, index=False)