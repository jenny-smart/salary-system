# 中央 GAS Web App

此目錄由 `scripts/build_central_gas.py` 將三套既有 GAS 隔離為單一 Web App。
此專案就是綁定在 `LemonSalarySystem` 主控試算表的 GAS；「中央 GAS」
與「主控檔 GAS」是同一個專案，不需要再把 PDF 程式留在每個期別執行檔。

所有日期、排程及打卡均使用 `Asia/Taipei`。若以手動方式貼上程式，請一併
更新 `appsscript.json`，或在 Apps Script「專案設定」將時區設為台北。

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
