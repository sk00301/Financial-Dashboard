# Financial Dashboard

A powerful **interactive financial analysis dashboard** built with **Streamlit** that provides real-time NSE stock insights, moving average analysis, index ratio comparisons, and advanced chart visualizations.

---

## 🚀 Overview

This project is a **financial analytics dashboard** designed for traders, analysts, and financial enthusiasts who want to quickly analyze market data from the **National Stock Exchange (NSE)** of India.

It provides:

✔ Deep technical analysis using moving averages  
✔ Identification of stocks trading below key averages  
✔ Interactive TradingView-style charts  
✔ Index ratio comparison (NSE indices, global indices or monetary data)  
✔ Advanced visualization with Plotly & lightweight charts  

---

## 🧠 Key Features

### 📈 Stock Analysis

- 📊 Analyze NSE stocks trading below DMA / WMA
- 📉 Daily (DMA) & Weekly (WMA) moving averages
- 📅 Multi-period analysis
- 📁 Local caching for faster performance
- 📌 Export filtered results to CSV

### 📊 Interactive Charts

- 📉 TradingView-style candlestick charts
- 📈 Overlay multiple moving averages
- 📅 Custom timeframe selection
- 🔍 Zoom & pan functionality

### 📉 Index Ratio Analysis

- 📌 Compare two indices or macro data series
- 📊 Calculate historical ratio
- 📈 Z-score & statistical analysis
- 📉 Interactive Plotly visualizations

---

## 🛠 Built With

| Technology | Purpose |
|------------|---------|
| Streamlit | Dashboard UI |
| yfinance | NSE data fetching |
| Pandas | Data processing |
| Plotly | Advanced charts |
| Lightweight Charts | TradingView-style charts |
| ThreadPoolExecutor | Parallel analysis |

---

## 📦 Installation

### 1️⃣ Clone Repository

```bash
git clone https://github.com/sk00301/Financial-Dashboard.git
cd Financial-Dashboard
```

2️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```

3️⃣ Run Application
```bash
streamlit run dashboard.py
```

Then open:
```bash
http://localhost:8501
```