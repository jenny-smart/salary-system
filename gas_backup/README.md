# GAS 原始備份

此處保存 2026-07-28 提供的三套既有 GAS/HTML 原始碼：

- `payment/`：金流對帳
- `cleaning/`：清潔承攬
- `other/`：其他承攬
- `scheduler/`：中控排程 Web App
- `master_existing/`：中控檔原有 PDF 產出程式

這些檔案是回退基準，不直接部署；中央版本由
`scripts/build_central_gas.py` 產生到 `gas_central/`。
