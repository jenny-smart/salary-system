# 中央 GAS Web App

此目錄由 `scripts/build_central_gas.py` 將三套既有 GAS 隔離為單一 Web App。

## 部署

1. 執行 `python3 scripts/build_central_gas.py`。
2. 將 `gas_central/` 內 `.gs`、`.html`、`appsscript.json` 加入中控試算表的 Apps Script 專案。
3. 設定 Script Property：`MASTER_SHEET_ID`＝中控試算表 ID。
4. 部署為 Web App：執行身分選「我」，存取權選公司使用者或任何人。
5. 將 `/exec` URL 設為 Streamlit Secret `GAS_SCHEDULER_WEB_APP_URL`。

網址格式：

`/exec?app=cleaning&spreadsheetId=目標期別試算表ID`

## 安全回退

確認中央版本完成測試前，不刪除任何期別檔內原 GAS。
