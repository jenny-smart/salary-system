# 期別資料夾與檔案排程更新包

這個更新包的目的：

- 排程時間到時，直接呼叫手動建立期別使用的 `create_period()`
- 手動與排程共用同一套 GAS `createPeriod` 流程
- 避免只儲存排程設定，但時間到沒有真正建立資料夾與檔案

## 檔案說明

- `modules/scheduler_service.py`：新增的排程服務主程式
- `modules/payment_reconciliation.py`：你提供的原本模組，保留手動建立期別核心
- `schedule_config.example.json`：排程設定範例
- `streamlit_schedule_patch_example.py`：Streamlit 整合範例

## 安裝方式

把 `modules/scheduler_service.py` 放進你專案的 `modules/` 資料夾。

如果你原本已有 `modules/payment_reconciliation.py`，不用覆蓋，因為這次主要新增的是 scheduler。

## Streamlit 整合

在主程式啟動處加入：

```python
from modules.scheduler_service import start_scheduler_once
start_scheduler_once()
```

在「儲存排程設定」按鈕中，改呼叫：

```python
from modules.scheduler_service import save_config_from_ui
```

可參考 `streamlit_schedule_patch_example.py`。

## 最穩定部署方式

如果你的 Streamlit 會休眠，請不要只靠背景 thread。

請用 cron 每分鐘檢查一次：

```bash
* * * * * cd /你的專案路徑 && python -m modules.scheduler_service --run-once
```

測試立即執行：

```bash
python -m modules.scheduler_service --force
```

## 注意

`schedule_config.json` 的 root_folder_id 必須換成各地區真實 Google Drive 根資料夾 ID。
