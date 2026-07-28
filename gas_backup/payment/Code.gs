// ✅ 金流對帳面板 v2026
// - 保留面板 / 商業名稱中文
// - 程式實作採正常工程命名
// - 修正前後端 API 命名與回傳格式不一致問題
// - 排程設定、排程狀態查詢、立即執行排程皆可正常運作
// - BUS 進度輪詢可用

// =========================
// 應用程式入口
// 負責建立試算表選單、開啟側邊欄，以及提供 Web 入口。
// =========================
function onOpen() {
  try {
    ensureScheduleHealth_();

    SpreadsheetApp.getUi()
      .createMenu("🧩 金流面板")
      .addItem("打開側邊欄", "showSidebar")
      .addItem("初始化系統", "initializeSystem")
      .addToUi();
  } catch (error) {
    console.warn("Failed to build custom menu:", error.message);
  }
}

function showSidebar() {
  try {
    const html = HtmlService.createHtmlOutputFromFile("面板")
      .setTitle("金流對帳面板")
      .setWidth(500);

    SpreadsheetApp.getUi().showSidebar(html);
    return true;
  } catch (error) {
    console.error("Failed to show sidebar:", error);

    const fallback = HtmlService.createHtmlOutput(`
      <html>
        <body style="font-family: Arial, sans-serif; padding: 12px;">
          <h3>⚠️ 無法開啟側邊欄</h3>
          <p>錯誤訊息: ${error.message}</p>
          <button onclick="google.script.run.showSidebar()">重試</button>
        </body>
      </html>
    `)
      .setTitle("錯誤")
      .setWidth(400);

    SpreadsheetApp.getUi().showSidebar(fallback);
    return false;
  }
}

function doGet() {
  return HtmlService.createHtmlOutputFromFile("面板").setTitle("金流對帳面板");
}

// =========================
// 全域設定
// 集中管理腳本屬性鍵名、BUS 快取存活時間，以及系統共用常數。
// =========================
const scriptProperties = PropertiesService.getScriptProperties();

const PROPERTY_KEYS = {
  ROOT_FOLDER_ID: "ROOT_FOLDER_ID",
  REGION_NAME: "REGION_NAME",
  LINE_TOKEN: "LINE_TOKEN",
  LINE_USERS: "LINE_USERS"
};

const BUS_TTL_SECONDS = 60 * 30;

// =========================
// BUS 進度快取
// 負責建立、更新與完成任務進度，提供前端側邊欄輪詢顯示執行狀態。
// 流程：startBus_ → appendToBus_ → finishBus_
// =========================
function startBus_(title) {
  try {
    const runId = Utilities.getUuid();
    const state = {
      runId,
      title: title || "",
      steps: [{
        timestamp: Date.now(),
        stage: "START",
        message: `🚀 ${title || "開始執行"}`
      }],
      done: false,
      success: null,
      progress: 0,
      timestamp: Date.now()
    };

    CacheService.getScriptCache().put(runId, JSON.stringify(state), BUS_TTL_SECONDS);
    return runId;
  } catch (error) {
    console.error("[BUS] Failed to initialize:", error);
    return `error-${Date.now()}`;
  }
}

function appendToBus_(runId, stage, message, progress) {
  try {
    const cache = CacheService.getScriptCache();
    const raw = cache.get(runId);
    if (!raw) return;

    const state = JSON.parse(raw);

    if (state.steps.length > 100) {
      state.steps = state.steps.slice(-50);
    }

    state.steps.push({
      timestamp: Date.now(),
      stage,
      message
    });

    state.timestamp = Date.now();

    if (typeof progress === "number") {
      state.progress = Math.max(0, Math.min(100, progress));
    }

    cache.put(runId, JSON.stringify(state), BUS_TTL_SECONDS);
  } catch (error) {
    console.error("[BUS] Failed to append state:", error);
  }
}

function finishBus_(runId, success, message) {
  try {
    const cache = CacheService.getScriptCache();
    const raw = cache.get(runId);
    if (!raw) return;

    const state = JSON.parse(raw);
    state.done = true;
    state.success = !!success;
    state.progress = 100;

    if (message) {
      state.steps.push({
        timestamp: Date.now(),
        stage: success ? "SUCCESS" : "ERROR",
        message
      });
    }

    cache.put(runId, JSON.stringify(state), 60);
  } catch (error) {
    console.warn("finishBus_ failed:", error.message);
  }
}

function pollBusStatus(runId) {
  const raw = CacheService.getScriptCache().get(runId);
  if (!raw) return null;
  return JSON.parse(raw);
}

// =========================
// 共用工具函式
// 提供日期標準化、貼上位置判斷、觸發器清理等共用輔助功能。
// =========================
function cleanupTriggersByHandler_(handlers) {
  const set = new Set(handlers);
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (set.has(trigger.getHandlerFunction())) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function normalizeDateOnly_(value) {
  if (value instanceof Date) {
    return new Date(value.getFullYear(), value.getMonth(), value.getDate(), 12, 0, 0, 0);
  }

  if (typeof value === "number" && !isNaN(value)) {
    const ms = Math.round((value - 25569) * 86400 * 1000);
    const date = new Date(ms);
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 12, 0, 0, 0);
  }

  if (typeof value === "string") {
    const text = value.trim();
    const match = text.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
    if (match) {
      return new Date(+match[1], +match[2] - 1, +match[3], 12, 0, 0, 0);
    }
  }

  return value;
}

const DATE_COLUMNS = [2, 3, 7]; // C, D, H（0-based index）

function normalizeSafeColumns_(row) {
  DATE_COLUMNS.forEach(i => {
    if (i < 0 || i >= row.length) return;

    const value = row[i];

    if (value instanceof Date) {
      row[i] = normalizeDateOnly_(value);
    } else if (typeof value === "number" && !isNaN(value) && value > 30000 && value < 60000) {
      row[i] = normalizeDateOnly_(value);
    } else if (
      typeof value === "string" &&
      /^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}$/.test(value.trim())
    ) {
      row[i] = normalizeDateOnly_(value);
    }
  });

  return row;
}

function normalizeSelectedColumnsOnly_(row) {
  [2, 3, 7].forEach(index => {
    const value = row[index];
    if (value instanceof Date) {
      row[index] = normalizeDateOnly_(value);
    } else if (typeof value === "number" && !isNaN(value) && value > 30000 && value < 60000) {
      row[index] = normalizeDateOnly_(value);
    } else if (typeof value === "string" && /^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}/.test(value.trim())) {
      row[index] = normalizeDateOnly_(value);
    }
  });
  return row;
}

// 相容舊程式呼叫名稱
function normalizeRowDates_(row) {
  return normalizeSafeColumns_(row);
}

function toDateStringIfNeeded_(value) {
  if (!(value instanceof Date)) return value;
  return Utilities.formatDate(value, "Asia/Taipei", "yyyy/MM/dd");
}

// 🔥 v2026 SAFE: 只統一日期顯示格式，不改變原始日期值。
// 支援 Date、Excel 數字日期、yyyy/m/d、yyyy-m-d。
function formatDateToSlash_(value) {
  if (value === "" || value == null) return value;

  let date = null;

  if (value instanceof Date) {
    date = value;
  } else if (typeof value === "number" && !isNaN(value) && value > 30000 && value < 60000) {
    const ms = Math.round((value - 25569) * 86400 * 1000);
    date = new Date(ms);
  } else if (typeof value === "string") {
    const text = value.trim();
    const match = text.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
    if (!match) return value;
    date = new Date(+match[1], +match[2] - 1, +match[3], 12, 0, 0, 0);
  }

  if (!date || isNaN(date.getTime())) return value;
  return Utilities.formatDate(date, "Asia/Taipei", "yyyy/M/d");
}

function preparePasteRow_(sheet, period) {
  const isFirstHalf = String(period || "").endsWith("-1");

  if (isFirstHalf) {
    sheet.getRange("A2:BJ").clearContent();
    sheet.getRange("A2:BJ").clearFormat();
    return 2;
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return 2;

  const values = sheet.getRange(2, 2, lastRow - 1, 1).getValues();
  let lastNonEmpty = 1;

  for (let i = values.length - 1; i >= 0; i--) {
    if (values[i][0] !== "" && values[i][0] != null) {
      lastNonEmpty = i + 2;
      break;
    }
  }

  return lastNonEmpty + 1;
}

function writeCleanIncomeExecutionLog_(spreadsheet, period, count) {
  const execSheet = spreadsheet.getSheetByName("執行");
  if (!execSheet) return;

  const isFirstHalf = String(period).endsWith("-1");
  const countCell = isFirstHalf ? "C8" : "D8";
  const timeCell = isFirstHalf ? "C9" : "D9";

  execSheet.getRange(countCell).setValue(count);
  execSheet.getRange(timeCell).setValue(
    Utilities.formatDate(new Date(), "Asia/Taipei", "yyyy/MM/dd HH:mm:ss")
  );
}

// =========================
// 設定管理
// 負責讀取與儲存根目錄、區域、LINE 通知等系統設定。
// =========================
const SettingsManager = {
  saveAllSettings({ rootFolderId, region, lineToken, lineUsers }) {
    rootFolderId = String(rootFolderId || "").trim();
    region = String(region || "").trim();

    if (!rootFolderId || !region) {
      throw new Error("請填寫根目錄 Folder ID 與 區域名稱");
    }

    try {
      DriveApp.getFolderById(rootFolderId);
    } catch (error) {
      throw new Error("根目錄 Folder ID 無效或不存在");
    }

    scriptProperties.setProperties({
      [PROPERTY_KEYS.ROOT_FOLDER_ID]: rootFolderId,
      [PROPERTY_KEYS.REGION_NAME]: region,
      [PROPERTY_KEYS.LINE_TOKEN]: String(lineToken || "").trim(),
      [PROPERTY_KEYS.LINE_USERS]: String(lineUsers || "").trim()
    }, true);

    return {
      success: true,
      message: "✅ 設定已儲存",
      rootFolderId,
      region
    };
  },

  getCurrentSettings() {
    return {
      rootFolderId: scriptProperties.getProperty(PROPERTY_KEYS.ROOT_FOLDER_ID) || "",
      region: scriptProperties.getProperty(PROPERTY_KEYS.REGION_NAME) || "",
      lineToken: scriptProperties.getProperty(PROPERTY_KEYS.LINE_TOKEN) || "",
      lineUsers: scriptProperties.getProperty(PROPERTY_KEYS.LINE_USERS) || ""
    };
  },

  copySettingsToSpreadsheet(fileId) {
    try {
      const settings = this.getCurrentSettings();
      const spreadsheet = SpreadsheetApp.openById(fileId);

      let sheet = spreadsheet.getSheetByName("系統設定");
      if (!sheet) {
        sheet = spreadsheet.insertSheet("系統設定");
        sheet.hideSheet();
      }

      const rows = [
        ["ROOT_FOLDER_ID", settings.rootFolderId],
        ["REGION_NAME", settings.region],
        ["LINE_TOKEN", settings.lineToken],
        ["LINE_USERS", settings.lineUsers],
        ["COPIED_AT", new Date().toISOString()]
      ];

      sheet.clear();
      sheet.getRange(1, 1, rows.length, 2).setValues(rows);
      return true;
    } catch (error) {
      console.warn("Failed to copy settings to spreadsheet:", error.message);
      return false;
    }
  },

  saveLineSettingsInBackground(runId, params) {
    const token = String(params.token || params.令牌 || "").trim();
    const users = String(params.users || params.用戶 || "").trim();

    if (!token || !users) {
      throw new Error("令牌/用戶 不可空白");
    }

    scriptProperties.setProperties({
      [PROPERTY_KEYS.LINE_TOKEN]: token,
      [PROPERTY_KEYS.LINE_USERS]: users
    }, true);

    appendToBus_(runId, "SUCCESS", "✅ 已儲存 LINE 設定", 100);
  }
};

// =========================
// 排程管理
// 負責儲存排程設定、安裝每日與每小時檢查器，並回報目前排程狀態。
// 這裡已修正前端 HTML 與後端 API 命名 / 回傳格式不一致問題。
// =========================
const StableScheduler = {
  setSchedule({ date, time, allRegions = false }) {
    const dayList = String(date)
      .split(",")
      .map(text => parseInt(text.trim(), 10))
      .filter(day => !isNaN(day) && day >= 1 && day <= 31);

    if (!dayList.length) {
      throw new Error("請輸入有效日期，例如：10,25");
    }

    const [hour, minute] = String(time).split(":").map(Number);
    if (isNaN(hour) || hour < 0 || hour > 23 || isNaN(minute) || minute < 0 || minute > 59) {
      throw new Error("時間格式錯誤，請使用 HH:mm (24小時制)");
    }

    scriptProperties.setProperty("SCHEDULE_SETTINGS", JSON.stringify({
      date: dayList,
      time: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
      allRegions: !!allRegions,
      timezone: "Asia/Taipei",
      createdAt: new Date().toISOString()
    }));

    this.installHealthTriggers();

    return {
      success: true,
      schedule: {
        date: dayList,
        time: `${hour}:${String(minute).padStart(2, "0")}`,
        allRegions: !!allRegions
      }
    };
  },

  installHealthTriggers() {
    this.removeSchedulerTriggers();

    ScriptApp.newTrigger("dailyScheduleCheck")
      .timeBased()
      .everyDays(1)
      .atHour(0)
      .nearMinute(10)
      .create();

    ScriptApp.newTrigger("hourlyScheduleCheck")
      .timeBased()
      .everyHours(1)
      .create();
  },

  removeSchedulerTriggers() {
    cleanupTriggersByHandler_(["dailyScheduleCheck", "hourlyScheduleCheck", "runScheduledJobNow"]);
  },

  getScheduleSettings() {
    const raw = scriptProperties.getProperty("SCHEDULE_SETTINGS");
    if (!raw) return null;

    try {
      return JSON.parse(raw);
    } catch (error) {
      return null;
    }
  },

  checkScheduleStatus() {
    const settings = this.getScheduleSettings();
    if (!settings) {
      return {
        hasSchedule: false,
        message: "尚未設定排程"
      };
    }

    const triggers = ScriptApp.getProjectTriggers();
    const hasDailyCheck = triggers.some(trigger => trigger.getHandlerFunction() === "dailyScheduleCheck");
    const hasHourlyCheck = triggers.some(trigger => trigger.getHandlerFunction() === "hourlyScheduleCheck");

    return {
      hasSchedule: true,
      settings,
      triggers: {
        dailyCheck: hasDailyCheck,
        hourlyCheck: hasHourlyCheck,
        total: triggers.length
      }
    };
  },

  runNow() {
    return dailyScheduleCheck();
  }
};

// =========================
// 排程檢查主流程
// 負責檢查今天是否為排程日、時間是否到達，若符合條件則將任務加入佇列。
// 執行流程：
// 1️⃣ 讀取排程設定
// 2️⃣ 判斷今天是否為排程日期
// 3️⃣ 判斷是否接近排程時間
// 4️⃣ 檢查今天是否已執行
// 5️⃣ 將建立期別任務加入佇列
// =========================
function dailyScheduleCheck() {
  return checkAndRunSchedule_(false);
}

function hourlyScheduleCheck() {
  return checkAndRunSchedule_(true);
}

function checkAndRunSchedule_(isHourlyCheck) {
  try {
    const settings = StableScheduler.getScheduleSettings();
    if (!settings) return;

    const now = new Date();
    const today = now.getDate();
    const currentHour = now.getHours();
    const currentMinute = now.getMinutes();
    const [scheduledHour, scheduledMinute] = settings.time.split(":").map(Number);

    if (!settings.date.includes(today)) {
      return;
    }

    const currentTotalMinutes = currentHour * 60 + currentMinute;
    const scheduledTotalMinutes = scheduledHour * 60 + scheduledMinute;
    const diff = currentTotalMinutes - scheduledTotalMinutes;

    if (Math.abs(diff) > 5) {
      return;
    }

    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, "0");
    const half = today <= 15 ? "1" : "2";
    const period = `${year}${month}-${half}`;

    const executedKey = `SCHEDULE_EXECUTED_${period}`;
    const executedAt = scriptProperties.getProperty(executedKey);
    if (executedAt) {
      const executedDate = new Date(parseInt(executedAt, 10));
      if (executedDate.getDate() === today) return;
    }

    enqueueScheduledPeriodCreation_(period, settings.allRegions);
    scriptProperties.setProperty(executedKey, String(Date.now()));
  } catch (error) {
    console.error("Schedule check failed:", error);
  }
}

function enqueueScheduledPeriodCreation_(period, allRegions) {
  const runId = startBus_(`排程建立 ${period}`);
  const queue = JSON.parse(scriptProperties.getProperty("TASK_QUEUE") || "[]");

  queue.push({
    runId,
    taskName: "建立期別",
    params: {
      period,
      isScheduled: true,
      allRegions: !!allRegions
    }
  });

  scriptProperties.setProperty("TASK_QUEUE", JSON.stringify(queue));
  ScriptApp.newTrigger("workerDispatcher").timeBased().after(500).create();
}

// =========================
// 通知管理
// 負責將排程建立結果或指定訊息批次發送到 LINE 使用者。
// =========================
const NotificationManager = {
  sendBatch(message, onlyForScheduledCreation = false) {
    const token = scriptProperties.getProperty(PROPERTY_KEYS.LINE_TOKEN);
    const userText = scriptProperties.getProperty(PROPERTY_KEYS.LINE_USERS);
    if (!token || !userText) return;

    if (onlyForScheduledCreation && !message.includes("排程建立") && !message.includes("期別檔案")) {
      return;
    }

    const users = userText.split(",").map(text => text.trim()).filter(Boolean);
    if (!users.length) return;

    const timeText = Utilities.formatDate(new Date(), "Asia/Taipei", "yyyy/MM/dd HH:mm:ss");
    const fullMessage = `${message}\n\n⏰ ${timeText}`;

    users.forEach(userId => {
      try {
        UrlFetchApp.fetch("https://api.line.me/v2/bot/message/push", {
          method: "post",
          contentType: "application/json",
          headers: { Authorization: "Bearer " + token },
          payload: JSON.stringify({
            to: userId,
            messages: [{ type: "text", text: fullMessage }]
          }),
          muteHttpExceptions: true
        });
      } catch (error) {
        console.error("Failed to send LINE message:", error);
      }
    });
  }
};

// =========================
// 期別管理
// 負責從檔名判斷目前期別，並推算上一期別供複製檔案使用。
// =========================
const PeriodManager = {
  getCurrentPeriodFromFilename() {
    const name = SpreadsheetApp.getActiveSpreadsheet().getName();
    const match = name.match(/^\d{6}-[12]/);
    return match ? match[0] : "";
  },

  getPreviousHalfPeriod(currentPeriod) {
    const yearMonth = currentPeriod.slice(0, 6);
    const half = currentPeriod.slice(7);
    let year = parseInt(yearMonth.slice(0, 4), 10);
    let month = parseInt(yearMonth.slice(4, 6), 10);

    if (half === "2") return `${yearMonth}-1`;

    month -= 1;
    if (month < 1) {
      month = 12;
      year -= 1;
    }

    return `${year}${("0" + month).slice(-2)}-2`;
  }
};

// =========================
// 檔案管理
// 負責建立、覆蓋、轉換期別檔案，並同步複製系統設定到新檔案。
// =========================
const FileManager = {
  createOrReplacePeriodFile(folder, fileName, templateId, runId) {
    const files = folder.getFilesByName(fileName);
    while (files.hasNext()) {
      files.next().setTrashed(true);
    }

    if (templateId) {
      const newFile = DriveApp.getFileById(templateId).makeCopy(fileName, folder);
      SettingsManager.copySettingsToSpreadsheet(newFile.getId());
      if (runId) appendToBus_(runId, "INFO", `📋 已建立檔案：${fileName}`);
      return newFile;
    }

    const spreadsheet = SpreadsheetApp.create(fileName);
    const file = DriveApp.getFileById(spreadsheet.getId());
    file.moveTo(folder);
    SettingsManager.copySettingsToSpreadsheet(spreadsheet.getId());
    if (runId) appendToBus_(runId, "INFO", `📋 已建立空白檔案：${fileName}`);
    return file;
  },

  convertFileToGoogleSheet(file, destinationFolder, runId, forcedName) {
  const name = file.getName();
  const ext = name.split(".").pop().toLowerCase();
  if (!["xls", "xlsx", "csv"].includes(ext)) return null;

  if (typeof Drive === "undefined" || !Drive.Files) {
    throw new Error("Drive 未定義（請啟用進階 Drive 服務：Drive API）");
  }

  // ✅ 可指定轉檔後 Google Sheet 名稱
  // 沒指定才使用原始檔名去副檔名
  const newName = forcedName || name.replace(/\.(xlsx?|csv)$/i, "");

  const existing = destinationFolder.getFilesByName(newName);
  while (existing.hasNext()) {
    const duplicate = existing.next();
    if (duplicate.getMimeType() === MimeType.GOOGLE_SHEETS) {
      duplicate.setTrashed(true);
    }
  }

  const blob = file.getBlob();
  const resource = {
    title: newName,
    mimeType: MimeType.GOOGLE_SHEETS,
    parents: [{ id: destinationFolder.getId() }]
  };

  const created = Drive.Files.insert(resource, blob, { convert: true });
  SettingsManager.copySettingsToSpreadsheet(created.id);

  if (runId) appendToBus_(runId, "INFO", `✅ 已轉為 Google Sheet：${newName}`);

  return SpreadsheetApp.openById(created.id);
}
};

// =========================
// 資料處理
// 提供尋找空白列、最後非空列與資料排序等共用資料處理邏輯。
// =========================
const DataProcessor = {
  findFirstEmptyRowInColumn(sheet, column) {
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return 2;
    const values = sheet.getRange(2, column, lastRow - 1, 1).getValues();
    for (let i = 0; i < values.length; i++) {
      if (!values[i][0]) return i + 2;
    }
    return values.length + 2;
  },

  findLastNonEmptyRow(sheet, column) {
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return 1;
    const values = sheet.getRange(2, column, lastRow - 1, 1).getValues();
    for (let i = values.length - 1; i >= 0; i--) {
      if (values[i][0]) return i + 2;
    }
    return 1;
  },

  sortData(rows) {
    const COL_E = 4;
    const COL_H = 7;
    const COL_M = 12;

    return rows.sort((a, b) => {
      const eA = String(a[COL_E] || "");
      const eB = String(b[COL_E] || "");
      if (eA !== eB) return eA.localeCompare(eB);

      const hA = normalizeDateOnly_(a[COL_H]) instanceof Date ? normalizeDateOnly_(a[COL_H]).getTime() : 0;
      const hB = normalizeDateOnly_(b[COL_H]) instanceof Date ? normalizeDateOnly_(b[COL_H]).getTime() : 0;
      if (hA !== hB) return hA - hB;

      return String(a[COL_M] || "").localeCompare(String(b[COL_M] || ""));
    });
  }
};

// =========================
// 執行紀錄
// 負責在「執行」工作表寫入筆數、時間與檔案 ID，供流程追蹤使用。
// =========================
const ExecutionLogger = {
  recordExecutionLog(period, label, value) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("執行");
    if (!sheet) return;

    const isFirstHalf = period.endsWith("-1");
    const valueCol = isFirstHalf ? 2 : 4;
    const timeCol = valueCol + 1;
    const rowIndex = sheet.getRange("A:A").getValues().findIndex(row => row[0] === label);
    if (rowIndex < 0) return;

    sheet.getRange(rowIndex + 1, valueCol).setValue(value);
    sheet.getRange(rowIndex + 1, timeCol).setValue(
      Utilities.formatDate(new Date(), "Asia/Taipei", "yyyy/MM/dd HH:mm:ss")
    );
  },

  recordExecutionId(label, id) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("執行");
    if (!sheet || !id) return;

    const rowIndex = sheet.getRange("A:A").getValues().findIndex(row => row[0] === label);
    if (rowIndex < 0) return;
    sheet.getRange(rowIndex + 1, 2).setValue(id);
  },

  recordExecutionLogs(period, map) {
    Object.keys(map).forEach(label => this.recordExecutionLog(period, label, map[label]));
  },

  getExecutionRowCount(period, label) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("執行");
    if (!sheet) return 0;

    const isFirstHalf = period.endsWith("-1");
    const valueCol = isFirstHalf ? 2 : 4;
    const rowIndex = sheet.getRange("A:A").getValues().findIndex(row => row[0] === label);
    if (rowIndex < 0) return 0;

    return Number(sheet.getRange(rowIndex + 1, valueCol).getValue()) || 0;
  }
};

// =========================
// 主程式入口
// 負責接收側邊欄任務指令並啟動 BUS 進度系統。
// 依任務名稱分派到對應的業務處理模組。
// 執行流程：
// 1️⃣ 建立 BUS 任務進度 (runId)
// 2️⃣ 驗證必要設定
// 3️⃣ 依任務名稱分派對應模組
// 4️⃣ 更新 BUS 狀態
// 5️⃣ 任務完成後回寫成功或失敗
// =========================
function startTask(taskName, params) {
  const titleMap = {
    設定LINE: "設定 LINE 通知",
    建立期別: "建立期別資料夾與檔案",
    轉檔期別訂單: "轉檔期別訂單",
    複製到範本: "期別訂單搬運到範本",
    處理範本: "範本加工",
    複製分類: "分類搬運到明細",
    轉換所有金流檔案: "金流檔案轉檔",
    搬運退款預收: "搬運已退款＋預收",
    搬運發票藍新: "搬運發票＋藍新"
  };

  const runId = startBus_(titleMap[taskName] || taskName);
  appendToBus_(runId, "INFO", "⏳ 準備執行任務...", 5);

  try {
    const rootFolderId = scriptProperties.getProperty(PROPERTY_KEYS.ROOT_FOLDER_ID);
    const region = scriptProperties.getProperty(PROPERTY_KEYS.REGION_NAME);

    if (taskName !== "設定LINE" && (!rootFolderId || !region)) {
      throw new Error("尚未設定根目錄與區域（請先在側邊欄儲存系統設定）");
    }

    if (region) {
      appendToBus_(runId, "INFO", `📍 區域：${region}`, 10);
    }

    const handlers = {
      設定LINE: () => SettingsManager.saveLineSettingsInBackground(runId, params || {}),
      建立期別: () => BusinessLogic.createPeriodFolderAndFilesInBackground(runId, params || {}),
      轉檔期別訂單: () => BusinessLogic.convertPeriodOrderInBackground(runId),
      複製到範本: () => BusinessLogic.copyPeriodOrderToTemplateInBackground(runId),
      處理範本: () => BusinessLogic.processTemplateInBackground(runId),
      複製分類: () => BusinessLogic.copyClassifiedDataInBackground(runId),
      轉換所有金流檔案: () => BusinessLogic.convertAllPaymentFilesInBackground(runId),
      搬運退款預收: () => RefundDataProcessor.processRefundAndPrepaidDataInBackground(runId),
      搬運發票藍新: () => BusinessLogic.moveInvoiceAndBluePayDataInBackground(runId)
    };

    if (!handlers[taskName]) {
      throw new Error(`未知任務名稱：${taskName}`);
    }

    appendToBus_(runId, "RUNNING", `▶️ 開始執行：${taskName}`, 20);
    handlers[taskName]();
    finishBus_(runId, true, "✅ 任務完成");
  } catch (error) {
    console.error(`Task failed: ${taskName}`, error);
    appendToBus_(runId, "ERROR", `❌ 錯誤: ${error.message}`);
    finishBus_(runId, false, `❌ 任務失敗：${error.message || error}`);
  }

  return { runId };
}

function workerDispatcher() {
  const queueText = scriptProperties.getProperty("TASK_QUEUE");
  if (!queueText) {
    cleanupTriggersByHandler_(["workerDispatcher"]);
    return;
  }

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(2000)) return;

  let runId = null;

  try {
    const queue = JSON.parse(queueText);
    if (!queue.length) {
      scriptProperties.deleteProperty("TASK_QUEUE");
      return;
    }

    const currentTask = queue.shift();
    scriptProperties.setProperty("TASK_QUEUE", JSON.stringify(queue));

    runId = currentTask.runId;
    appendToBus_(runId, "RUNNING", `▶️ 開始處理: ${currentTask.taskName}`, 15);

    const handlers = {
      設定LINE: () => SettingsManager.saveLineSettingsInBackground(runId, currentTask.params || {}),
      建立期別: () => BusinessLogic.createPeriodFolderAndFilesInBackground(runId, currentTask.params || {}),
      轉檔期別訂單: () => BusinessLogic.convertPeriodOrderInBackground(runId),
      複製到範本: () => BusinessLogic.copyPeriodOrderToTemplateInBackground(runId),
      處理範本: () => BusinessLogic.processTemplateInBackground(runId),
      複製分類: () => BusinessLogic.copyClassifiedDataInBackground(runId),
      轉換所有金流檔案: () => BusinessLogic.convertAllPaymentFilesInBackground(runId),
      搬運退款預收: () => RefundDataProcessor.processRefundAndPrepaidDataInBackground(runId),
      搬運發票藍新: () => BusinessLogic.moveInvoiceAndBluePayDataInBackground(runId)
    };

    if (!handlers[currentTask.taskName]) {
      throw new Error(`未知任務名稱：${currentTask.taskName}`);
    }

    handlers[currentTask.taskName]();
    finishBus_(runId, true, "✅ 任務完成");
  } catch (error) {
    console.error("workerDispatcher failed:", error);
    if (runId) {
      appendToBus_(runId, "ERROR", `❌ 錯誤: ${error.message}`);
      finishBus_(runId, false, `❌ 任務失敗：${error.message || error}`);
    }
  } finally {
    try {
      const remaining = JSON.parse(scriptProperties.getProperty("TASK_QUEUE") || "[]");
      if (remaining.length) {
        ScriptApp.newTrigger("workerDispatcher").timeBased().after(2000).create();
      } else {
        cleanupTriggersByHandler_(["workerDispatcher"]);
      }
    } finally {
      lock.releaseLock();
    }
  }
}

// =========================
// 主要業務邏輯
// 負責建立期別、轉檔訂單、搬運範本、分類資料與發票藍新等核心流程。
// =========================

const BusinessLogic = {
  /**
   * 建立期別資料夾與檔案
   *
   * 執行流程：
   * 1️⃣ 驗證期別格式
   * 2️⃣ 判斷是否全區模式
   * 3️⃣ 單區則建立單一區域；全區則逐區建立
   * 4️⃣ 依上一期資料夾複製四類檔案
   * 5️⃣ 若為元大 xlsx 則轉為 Google Sheet
   * 6️⃣ 更新 BUS 與執行紀錄
   */
  createPeriodFolderAndFilesInBackground(runId, params) {
    const period = String(params.period || params.期別 || "").trim();
    const isScheduled = !!(params.isScheduled || params.是否排程);
    const allRegions = !!(params.allRegions || params.所有區域);

    if (!/^\d{6}-[12]$/.test(period)) {
      throw new Error("期別格式錯誤（需 YYYYMM-1 / YYYYMM-2）");
    }

    appendToBus_(runId, "RUNNING", `📅 處理期別: ${period}`, 10);

    if (allRegions) {
      this.createPeriodForAllRegions_(runId, period, isScheduled);
    } else {
      const settings = SettingsManager.getCurrentSettings();
      this.createPeriodForSingleRegion_(runId, period, settings.rootFolderId, settings.region, isScheduled, true);
    }
  },

  createPeriodForAllRegions_(runId, period, isScheduled) {
    appendToBus_(runId, "INFO", "🌍 全區域執行模式", 15);

    const rootFolderId = scriptProperties.getProperty(PROPERTY_KEYS.ROOT_FOLDER_ID);
    if (!rootFolderId) throw new Error("尚未設定根目錄");

    const rootFolder = DriveApp.getFolderById(rootFolderId);
    const regions = [];
    const folders = rootFolder.getFolders();

    while (folders.hasNext()) {
      const folder = folders.next();
      if (!/^\d+$/.test(folder.getName())) {
        regions.push(folder.getName());
      }
    }

    if (!regions.length) {
      throw new Error("根目錄下找不到區域資料夾");
    }

    appendToBus_(runId, "INFO", `找到 ${regions.length} 個區域`, 20);

    let completed = 0;
    const errors = [];

    regions.forEach((region, index) => {
      try {
        const folderIter = rootFolder.getFoldersByName(region);
        if (!folderIter.hasNext()) {
          errors.push(`${region}: 找不到資料夾`);
          appendToBus_(runId, "WARNING", `找不到區域資料夾: ${region}`);
          return;
        }

        const regionFolder = folderIter.next();
        appendToBus_(runId, "RUNNING", `處理區域: ${region}`, 20 + Math.floor((index / regions.length) * 60));
        this.createPeriodForSingleRegion_(runId, period, regionFolder.getId(), region, isScheduled, false);
        completed++;
      } catch (error) {
        errors.push(`${region}: ${error.message}`);
        appendToBus_(runId, "WARNING", `區域 ${region} 失敗: ${error.message}`);
      }
    });

    if (isScheduled) {
      if (!errors.length && completed === regions.length) {
        NotificationManager.sendBatch(`✅【全區域】${period} 期別檔案已建立完成\n✅ 全部 ${completed} 個區域成功完成`, true);
      } else if (completed > 0) {
        NotificationManager.sendBatch(
          `⚠️【全區域】${period} 期別檔案建立部分完成\n✅ 成功: ${completed} 個區域\n❌ 失敗: ${errors.length} 個區域\n${errors.length ? "失敗區域: " + errors.join("; ") : ""}`,
          true
        );
      } else {
        NotificationManager.sendBatch(
          `❌【全區域】${period} 期別檔案建立失敗\n❌ 全部 ${regions.length} 個區域均失敗\n錯誤詳情: ${errors.join("; ")}`,
          true
        );
      }
    }

    appendToBus_(runId, "SUCCESS", `✅ 全區完成: ${completed}/${regions.length} 區域`, 95);
  },

  createPeriodForSingleRegion_(runId, period, rootFolderId, region, isScheduled, sendNotification) {
    try {
      appendToBus_(runId, "INFO", `📍 區域：${region}`, 25);

      const baseFolder = DriveApp.getFolderById(rootFolderId);
      const previousPeriod = PeriodManager.getPreviousHalfPeriod(period);
      const labels = ["金流對帳", "清潔承攬", "其他承攬", "元大帳戶"];

      appendToBus_(runId, "INFO", `📅 上一期：${previousPeriod}`, 30);

      const currentFolderIter = baseFolder.getFoldersByName(period);
      const periodFolder = currentFolderIter.hasNext() ? currentFolderIter.next() : baseFolder.createFolder(period);

      ExecutionLogger.recordExecutionLog(period, isScheduled ? "排程期別資料夾" : "手動期別資料夾", 1);
      ExecutionLogger.recordExecutionId(isScheduled ? "排程期別資料夾" : "手動期別資料夾", periodFolder.getId());

      const previousFolderIter = baseFolder.getFoldersByName(previousPeriod);
      if (!previousFolderIter.hasNext()) {
        const message = `找不到上一期資料夾：${previousPeriod}`;
        if (isScheduled && sendNotification) {
          NotificationManager.sendBatch(`❌【${region}】${period} 期別檔案建立失敗\n錯誤: ${message}`, true);
        }
        throw new Error(message);
      }

      const previousFolder = previousFolderIter.next();
      appendToBus_(runId, "RUNNING", "📄 開始複製上期檔案…", 40);

      labels.forEach((label, index) => {
        const progress = 40 + Math.round((index / labels.length) * 45);
        appendToBus_(runId, "RUNNING", `📄 處理：${label}`, progress);

        const executionLabel = `${isScheduled ? "排程期別" : "手動期別"}${label}`;

        if (label === "元大帳戶") {
          let oldNameXlsx = `${previousPeriod}${label}-${region}.xlsx`;
          let oldNameSheet = `${previousPeriod}${label}-${region}`;
          let files = previousFolder.getFilesByName(oldNameXlsx);
          if (!files.hasNext()) {
            files = previousFolder.getFilesByName(oldNameSheet);
          }

          if (!files.hasNext()) {
            appendToBus_(runId, "WARNING", `找不到上期元大帳戶：${previousPeriod}${label}-${region}`, progress);
            return;
          }

          const sourceFile = files.next();
          const newName = `${period}${label}-${region}`;
          let finalFile;

          if (sourceFile.getMimeType() === MimeType.GOOGLE_SHEETS) {
            finalFile = FileManager.createOrReplacePeriodFile(periodFolder, newName, sourceFile.getId(), runId);
          } else {
            const copiedXlsx = sourceFile.makeCopy(`${newName}.xlsx`, periodFolder);
            appendToBus_(runId, "RUNNING", "🔄 轉換元大 xlsx → Google Sheet", progress + 3);
            let forcedName = null;

            if (file.getName().includes("發票")) {
              forcedName = `${period}發票-${scriptProperties.getProperty(PROPERTY_KEYS.REGION_NAME)}`;
            }

            const convertedSheet = FileManager.convertFileToGoogleSheet(file, folder, runId, forcedName);

            if (!convertedSheet) {
              appendToBus_(runId, "WARNING", "元大帳戶轉檔失敗", progress);
              return;
            }
            finalFile = DriveApp.getFileById(convertedSheet.getId());
          }

          ExecutionLogger.recordExecutionLog(period, executionLabel, 1);
          ExecutionLogger.recordExecutionId(executionLabel, finalFile.getId());
        } else {
          const oldName = `${previousPeriod}${label}-${region}`;
          const files = previousFolder.getFilesByName(oldName);
          if (!files.hasNext()) {
            appendToBus_(runId, "WARNING", `找不到上期 ${label}：${oldName}`, progress);
            return;
          }

          const sourceFile = files.next();
          const newName = `${period}${label}-${region}`;
          const finalFile = FileManager.createOrReplacePeriodFile(periodFolder, newName, sourceFile.getId(), runId);

          ExecutionLogger.recordExecutionLog(period, executionLabel, 1);
          ExecutionLogger.recordExecutionId(executionLabel, finalFile.getId());
        }
      });

      appendToBus_(runId, "SUCCESS", "✅ 期別資料夾與檔案建立完成", 95);

      if (isScheduled && sendNotification) {
        NotificationManager.sendBatch(`✅【${region}】${period} 期別檔案已建立完成`, true);
      }
    } catch (error) {
      appendToBus_(runId, "ERROR", `❌ 區域 ${region} 失敗: ${error.message}`);
      if (isScheduled && sendNotification) {
        NotificationManager.sendBatch(`❌【${region}】${period} 期別檔案建立失敗\n錯誤: ${error.message}`, true);
      }
      throw error;
    }
  },

  /**
   * 轉檔期別訂單
   */
  convertPeriodOrderInBackground(runId) {
    appendToBus_(runId, "RUNNING", "📂 尋找期別訂單檔案…", 10);

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const folder = DriveApp.getFileById(spreadsheet.getId()).getParents().next();
    const period = PeriodManager.getCurrentPeriodFromFilename();
    if (!period) throw new Error("無法從檔名取得期別");

    const filesForCleanup = folder.getFiles();
    while (filesForCleanup.hasNext()) {
      const file = filesForCleanup.next();
      const name = file.getName();
      if (name.startsWith(period) && name.includes("訂單") && file.getMimeType() === MimeType.GOOGLE_SHEETS) {
        file.setTrashed(true);
      }
    }

    let sourceFile = null;
    const files = folder.getFiles();
    while (files.hasNext()) {
      const file = files.next();
      const name = file.getName().toLowerCase();
      if (name.startsWith(period.toLowerCase()) && name.includes("訂單") && name.match(/\.(xlsx?|csv)$/)) {
        sourceFile = file;
        break;
      }
    }

    if (!sourceFile) {
      ExecutionLogger.recordExecutionLog(period, "期別訂單轉檔", 0);
      throw new Error("找不到期別訂單原始檔（xls/xlsx/csv）");
    }

    appendToBus_(runId, "RUNNING", `🔄 轉檔：${sourceFile.getName()}`, 35);
    const convertedSheet = FileManager.convertFileToGoogleSheet(sourceFile, folder, runId);
    Utilities.sleep(500);

    const sheet = convertedSheet.getSheets()[0];
    const rowCount = Math.max(0, sheet.getLastRow() - 1);
    ExecutionLogger.recordExecutionLog(period, "期別訂單轉檔", rowCount);
    appendToBus_(runId, "SUCCESS", `✅ 轉檔完成（${rowCount} 筆）`, 95);
  },

  /**
   * 複製期別訂單到範本
   */
  copyPeriodOrderToTemplateInBackground(runId) {
    appendToBus_(runId, "RUNNING", "準備搬運訂單資料...", 5);

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const period = PeriodManager.getCurrentPeriodFromFilename();
    if (!period) throw new Error("無法從檔名取得期別");

    const isFirstHalf = period.endsWith("-1");
    const folder = DriveApp.getFileById(spreadsheet.getId()).getParents().next();

    appendToBus_(runId, "RUNNING", "🔎 尋找最新的訂單 Google Sheet…", 10);

    let latestFile = null;
    let latestTime = null;
    const files = folder.getFiles();

    while (files.hasNext()) {
      const file = files.next();
      const name = file.getName();
      if (name.startsWith(period) && name.includes("訂單") && file.getMimeType() === MimeType.GOOGLE_SHEETS) {
        const updatedAt = file.getLastUpdated();
        if (!latestTime || updatedAt > latestTime) {
          latestTime = updatedAt;
          latestFile = file;
        }
      }
    }

    if (!latestFile) throw new Error("找不到期別訂單 Google Sheet，請先轉檔");

    SpreadsheetApp.flush();
    const sourceSpreadsheet = SpreadsheetApp.openById(latestFile.getId());
    const sourceSheet = sourceSpreadsheet.getSheets()[0];
    const targetSheet = spreadsheet.getSheetByName("範本");
    if (!targetSheet) throw new Error("找不到範本工作表");

    appendToBus_(runId, "RUNNING", "📥 讀取來源資料 A2:BJ…", 30);
    const range = sourceSheet.getRange("A2:BJ");
    const values = range.getValues();
    const backgrounds = range.getBackgrounds();

    let rowCount = 0;
    for (let i = 0; i < values.length; i++) {
      if (values[i].join("") !== "") rowCount++;
    }

    if (!rowCount) {
      ExecutionLogger.recordExecutionLog(period, "複製期別訂單", 0);
      throw new Error("來源訂單無資料");
    }

    // 🔥 v2026 SAFE: 搬運階段不轉換 C/D/H 日期，避免時區或格式漂移。

    appendToBus_(runId, "RUNNING", "📤 寫入範本…", 60);

    if (isFirstHalf) {
      targetSheet.getRange("A2:BJ").clearContent();
      targetSheet.getRange("A2:BJ").clearFormat();
      targetSheet.getRange(2, 1, rowCount, values[0].length).setValues(values.slice(0, rowCount));
      targetSheet.getRange(2, 1, rowCount, values[0].length).setBackgrounds(backgrounds.slice(0, rowCount));
    } else {
      const startRow = preparePasteRow_(targetSheet, period);
      targetSheet.getRange(startRow, 1, rowCount, values[0].length).setValues(values.slice(0, rowCount));
      targetSheet.getRange(startRow, 1, rowCount, values[0].length).setBackgrounds(backgrounds.slice(0, rowCount));
    }

    ExecutionLogger.recordExecutionLog(period, "複製期別訂單", rowCount);
    appendToBus_(runId, "SUCCESS", `✅ 搬運完成（${rowCount} 筆）`, 95);
  },

  /**
   * 範本加工
   */
  processTemplateInBackground(runId) {
    appendToBus_(runId, "RUNNING", "🧩 開始範本加工...", 10);

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = spreadsheet.getSheetByName("範本");
    const period = PeriodManager.getCurrentPeriodFromFilename();

    if (!sheet || !period) {
      throw new Error("缺少範本工作表或無法取得期別");
    }

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) throw new Error("範本工作表沒有資料（至少需要 2 行）");

    let rowCount = ExecutionLogger.getExecutionRowCount(period, "複製期別訂單");
    if (!rowCount) {
      appendToBus_(runId, "WARNING", "⚠️ 從記錄找不到搬入筆數，改用工作表計算...", 20);
      rowCount = Math.max(0, lastRow - 1);
    }

    if (!rowCount) throw new Error("沒有資料需要處理");

    const startRow = this.getSourceStartRowByPeriod_(sheet, period, rowCount);

    appendToBus_(runId, "RUNNING", `加工範圍：period=${period} startRow=${startRow} rowCount=${rowCount}`, 25);

    const phase1Result = new ReconciliationProcessor(sheet, startRow, rowCount, runId).process();
    const splitResult = this.expandFGRowsInProcessedRange_(sheet, startRow, rowCount);

    ExecutionLogger.recordExecutionLogs(period, {
      "加工-排序": splitResult.totalProcessedCount || phase1Result.sortCount || rowCount || 0,
      "加工-K欄標註異常標橘底": phase1Result.markCount || 0,
      "加工-水洗加工": splitResult.washProcessCount || 0,
      "加工-家電加工": splitResult.applianceProcessCount || 0,
      "加工-收納加工": splitResult.storageProcessCount || 0,
      "加工-座椅加工": splitResult.seatProcessCount || 0,
      "加工-地毯加工": splitResult.carpetProcessCount || 0,
      "加工-其他服務加工": splitResult.otherProcessCount || 0
    });

    appendToBus_(
      runId,
      "SUCCESS",
      `✅ 加工完成：排序 ${splitResult.totalProcessedCount || phase1Result.sortCount || 0} 筆，異常 ${phase1Result.markCount || 0} 筆，水洗 ${splitResult.washProcessCount || 0}，家電 ${splitResult.applianceProcessCount || 0}，收納 ${splitResult.storageProcessCount || 0}，座椅 ${splitResult.seatProcessCount || 0}，地毯 ${splitResult.carpetProcessCount || 0}，其他服務 ${splitResult.otherProcessCount || 0}`,
      95
    );

    return { ...phase1Result, ...splitResult };
  },

  /**
   * 分類搬運到明細表
   */
  copyClassifiedDataInBackground(runId) {
    appendToBus_(runId, "RUNNING", "開始分類搬運...", 5);

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const period = PeriodManager.getCurrentPeriodFromFilename();
    if (!period) throw new Error("無法從檔名取得期別");

    const sourceSheet = spreadsheet.getSheetByName("範本");
    if (!sourceSheet) throw new Error("找不到範本工作表");

    const folder = DriveApp.getFileById(spreadsheet.getId()).getParents().next();
    const files = folder.getFiles();

    let cleanFile = null;
    let otherFile = null;
    while (files.hasNext()) {
      const file = files.next();
      const name = file.getName();
      if (name.includes("清潔承攬")) cleanFile = file;
      if (name.includes("其他承攬")) otherFile = file;
    }

    if (!cleanFile || !otherFile) {
      throw new Error("找不到清潔承攬或其他承攬檔案（請確認檔名）");
    }

    const cleanSpreadsheet = SpreadsheetApp.openById(cleanFile.getId());
    const otherSpreadsheet = SpreadsheetApp.openById(otherFile.getId());
    
    // ===== 新增：寫入金流對帳ID =====
    let paymentFileId = "";

    const paymentFiles = folder.getFiles();
    while (paymentFiles.hasNext()) {
      const file = paymentFiles.next();

      if (
        file.getName().includes("金流對帳") &&
        file.getMimeType() === MimeType.GOOGLE_SHEETS
      ) {
        paymentFileId = file.getId();
        break;
      }
    }

    const cleanExecSheet = cleanSpreadsheet.getSheetByName("執行");

    if (cleanExecSheet && paymentFileId) {
      cleanExecSheet.getRange("C5").setValue(paymentFileId);
    }
    // ===== 新增結束 =====
    
    const cleanSheet = cleanSpreadsheet.getSheetByName("清潔營收明細");
    if (!cleanSheet) throw new Error("清潔承攬缺少工作表：清潔營收明細");

    const targetSheets = {
      收納: otherSpreadsheet.getSheetByName("收納營收明細"),
      水洗: otherSpreadsheet.getSheetByName("水洗營收明細"),
      座椅: otherSpreadsheet.getSheetByName("座椅營收明細"),
      地毯: otherSpreadsheet.getSheetByName("地毯營收明細"),
      家電: otherSpreadsheet.getSheetByName("家電營收明細")
    };

    let processedCount = ExecutionLogger.getExecutionRowCount(period, "加工-排序");
    if (!processedCount) {
      processedCount = ExecutionLogger.getExecutionRowCount(period, "複製期別訂單");
    }
    if (!processedCount) {
      throw new Error("找不到加工或搬入筆數，請先執行期別複製 / 加工");
    }

    const startRow = this.getSourceStartRowByPeriod_(sourceSheet, period, processedCount);

    appendToBus_(runId, "RUNNING", `搬運來源範圍：period=${period} startRow=${startRow} processedCount=${processedCount}`, 10);

    const rawValues = sourceSheet.getRange(startRow, 1, processedCount, 62).getValues();
    const rawBackgrounds = sourceSheet.getRange(startRow, 1, processedCount, 62).getBackgrounds();

    const cleanRows = [];
    const cleanBackgrounds = [];
    const buckets = {
      收納: { rows: [], backgrounds: [] },
      水洗: { rows: [], backgrounds: [] },
      座椅: { rows: [], backgrounds: [] },
      地毯: { rows: [], backgrounds: [] },
      家電: { rows: [], backgrounds: [] }
    };

    rawValues.forEach((row, index) => {
      const type = String(row[4] || "").trim();

      // 🔥 v2026 SAFE: 分類搬運輸出時只統一顯示格式，不改日期值。
      row[2] = formatDateToSlash_(row[2]);
      row[3] = formatDateToSlash_(row[3]);
      row[7] = formatDateToSlash_(row[7]);

      if (type.includes("家電")) {
        buckets.家電.rows.push(row);
        buckets.家電.backgrounds.push(rawBackgrounds[index]);
      } else if (type.includes("水洗")) {
        buckets.水洗.rows.push(row);
        buckets.水洗.backgrounds.push(rawBackgrounds[index]);
      } else if (type.includes("收納")) {
        buckets.收納.rows.push(row);
        buckets.收納.backgrounds.push(rawBackgrounds[index]);
      } else if (type.includes("座椅")) {
        buckets.座椅.rows.push(row);
        buckets.座椅.backgrounds.push(rawBackgrounds[index]);
      } else if (type.includes("地毯")) {
        buckets.地毯.rows.push(row);
        buckets.地毯.backgrounds.push(rawBackgrounds[index]);
      } else if (type.includes("1專業清潔") || type === "清潔" || type.startsWith("清潔")) {
        cleanRows.push(row);
        cleanBackgrounds.push(rawBackgrounds[index]);
      }
    });

    const movedCategories = [];
    if (cleanRows.length) movedCategories.push("清潔");
    Object.keys(buckets).forEach(key => {
      if (buckets[key].rows.length) movedCategories.push(key);
    });

    appendToBus_(runId, "RUNNING", `本次搬運類別：${movedCategories.length ? movedCategories.join("、") : "無"}`, 15);

    if (cleanRows.length) {
      const start = this.prepareCleanPasteRowByPeriod_(cleanSheet, period);
      cleanSheet.getRange(start, 1, cleanRows.length, cleanRows[0].length).setValues(cleanRows);
      cleanSheet.getRange(start, 1, cleanRows.length, cleanRows[0].length).setBackgrounds(cleanBackgrounds);
    }

    writeCleanIncomeExecutionLog_(cleanSpreadsheet, period, cleanRows.length);

    Object.keys(targetSheets).forEach(key => {
      const targetSheet = targetSheets[key];
      if (!targetSheet) return;

      const rows = buckets[key].rows;
      const backgrounds = buckets[key].backgrounds;
      if (!rows.length) return;

      const start = this.prepareOtherIncomePasteRowByPeriod_(targetSheet, period);
      targetSheet.getRange(start, 1, rows.length, rows[0].length).setValues(rows);
      targetSheet.getRange(start, 1, rows.length, rows[0].length).setBackgrounds(backgrounds);
    });

    ExecutionLogger.recordExecutionLogs(period, {
      "複製清潔訂單": cleanRows.length,
      "複製收納訂單": buckets.收納.rows.length,
      "複製水洗訂單": buckets.水洗.rows.length,
      "複製座椅訂單": buckets.座椅.rows.length,
      "複製地毯訂單": buckets.地毯.rows.length,
      "複製家電訂單": buckets.家電.rows.length
    });

    appendToBus_(
      runId,
      "SUCCESS",
      `✅ 分類搬運完成（${period}）：${movedCategories.length ? movedCategories.join("、") : "無"}｜清潔${cleanRows.length} 收納${buckets.收納.rows.length} 水洗${buckets.水洗.rows.length} 座椅${buckets.座椅.rows.length} 地毯${buckets.地毯.rows.length} 家電${buckets.家電.rows.length}`,
      95
    );
  },
  

  /**
   * 唯一拆解入口
   */
  expandFGRowsInProcessedRange_(sheet, startRow, rowCount) {
    if (!sheet) throw new Error("expandFGRowsInProcessedRange_: sheet 不存在");
    if (!startRow || startRow < 2) {
      throw new Error(`expandFGRowsInProcessedRange_: startRow 無效 (${startRow})`);
    }
    if (!rowCount || rowCount < 1) {
      throw new Error(`expandFGRowsInProcessedRange_: rowCount 無效 (${rowCount})`);
    }

    const range = sheet.getRange(startRow, 1, rowCount, 62);
    const values = range.getValues();
    const backgrounds = range.getBackgrounds();

    const expandedValues = [];
    const expandedBackgrounds = [];

    // 🔥 v2026 SAFE: 記住本次加工範圍內所有已存在的訂單編號。
    // 已存在子單：更新 F/G，不重複新增。
    // 不存在子單：自動新增。
    const existingIds = new Set(
      values.map(row => String(row[1] || "").trim()).filter(Boolean)
    );

    // 母單拆解後，先暫存「既有子單應更新的 F/G」。
    const childItemMap = {};

    let washProcessCount = 0;
    let applianceProcessCount = 0;
    let seatProcessCount = 0;
    let storageProcessCount = 0;
    let carpetProcessCount = 0;
    let otherProcessCount = 0;

    const countByType_ = flags => {
      if (flags.isWash) washProcessCount++;
      else if (flags.isAppliance) applianceProcessCount++;
      else if (flags.isSeat) seatProcessCount++;
      else if (flags.isStorage) storageProcessCount++;
      else if (flags.isCarpet) carpetProcessCount++;
      else if (flags.isOther) otherProcessCount++;
    };

    values.forEach((row, index) => {
      const type = String(row[4] || "").trim();
      const fValue = String(row[5] || "").trim();
      const id = String(row[1] || "").trim();

      const flags = {
        isWash: type.includes("水洗"),
        isAppliance: type.includes("家電"),
        isSeat: type.includes("座椅"),
        isStorage: type.includes("收納"),
        isCarpet: type.includes("地毯"),
        isOther: type.includes("其他") || type.includes("其他服務")
      };

      const isExpandableType =
        flags.isWash || flags.isAppliance || flags.isSeat || flags.isStorage || flags.isCarpet || flags.isOther;

      const isChildRow = /\-\d+$/.test(id);

      // 既有子單：如果母單已經指定拆解內容，就更新 F/G；否則原樣保留。
      if (isChildRow) {
        if (childItemMap[id]) {
          const item = childItemMap[id];
          const newRow = row.slice();
          newRow[5] = item.name;
          newRow[6] = item.qty || "1";
          expandedValues.push(newRow);
          expandedBackgrounds.push(backgrounds[index]);
          countByType_(item.flags || flags);
        } else {
          expandedValues.push(row);
          expandedBackgrounds.push(backgrounds[index]);
        }
        return;
      }

      // 非拆解類型或 F 欄空白：原樣保留。
      if (!isExpandableType || !fValue) {
        expandedValues.push(row);
        expandedBackgrounds.push(backgrounds[index]);
        return;
      }

      const parsedItems = this.parseMultiLineServiceItems_(fValue);

      if (!parsedItems.length) {
        expandedValues.push(row);
        expandedBackgrounds.push(backgrounds[index]);
        return;
      }

      parsedItems.forEach((item, itemIndex) => {
        const newRow = row.slice();

        // 第 1 個服務項目：留在母單列，F/G 改成第一項。
        if (itemIndex === 0) {
          newRow[5] = item.name;
          newRow[6] = item.qty || "1";
          expandedValues.push(newRow);
          expandedBackgrounds.push(backgrounds[index]);
          countByType_(flags);
          return;
        }

        const childId = `${id}-${itemIndex}`;

        // 子單已存在：不新增，等跑到子單列時更新 F/G。
        if (existingIds.has(childId)) {
          childItemMap[childId] = { ...item, flags };
          return;
        }

        // 子單不存在：新增一列。
        newRow[1] = childId;
        newRow[5] = item.name;
        newRow[6] = item.qty || "1";

        expandedValues.push(newRow);

        const bg = backgrounds[index].slice();
        while (bg.length < 62) bg.push("");
        bg.fill("#FFFF66");
        expandedBackgrounds.push(bg);

        countByType_(flags);
      });
    });

    range.clearContent().clearFormat();

    const extraRows = expandedValues.length - rowCount;
    if (extraRows > 0) {
      sheet.insertRowsAfter(startRow + rowCount - 1, extraRows);
    }

    const writeRange = sheet.getRange(startRow, 1, expandedValues.length, 62);
    writeRange.setValues(expandedValues);
    writeRange.setBackgrounds(expandedBackgrounds);

    return {
      totalProcessedCount: expandedValues.length,
      washProcessCount,
      applianceProcessCount,
      seatProcessCount,
      storageProcessCount,
      carpetProcessCount,
      otherProcessCount
    };
  },

  /**
   * 解析 F欄一格內的服務項目
   */
  parseMultiLineServiceItems_(text) {
    const raw = String(text || "").replace(/　/g, " ").replace(/Ｘ/g, "X").trim();
    if (!raw) return [];

    const lines = raw
      .split(/\r?\n|、|,|\/|；|;/)
      .map(s => String(s || "").replace(/^"+|"+$/g, "").trim())
      .filter(Boolean);

    if (!lines.length) return [];

    return lines.map(line => {
      const match = line.match(/^(.*?)[\s]*[Xx×＊*]\s*([0-9]+)\s*$/);
      if (!match) {
        return { name: line, qty: "1" };
      }
      return { name: match[1].trim(), qty: match[2].trim() };
    });
  },

  getSourceStartRowByPeriod_(sheet, period, rowCount) {
    if (!sheet) throw new Error("getSourceStartRowByPeriod_: sheet 不存在");
    if (!rowCount || rowCount < 1) throw new Error("getSourceStartRowByPeriod_: rowCount 無效");

    if (this.isFirstHalfByPeriod_(period)) {
      return 2;
    }

    const lastNonEmpty = DataProcessor.findLastNonEmptyRow(sheet, 2);
    if (!lastNonEmpty || lastNonEmpty < 2) {
      throw new Error("找不到範本最後一筆有效資料");
    }

    const startRow = lastNonEmpty - rowCount + 1;
    if (startRow < 2) {
      throw new Error(`來源資料範圍異常：period=${period}, lastNonEmpty=${lastNonEmpty}, rowCount=${rowCount}, startRow=${startRow}`);
    }

    return startRow;
  },

  prepareCleanPasteRowByPeriod_(sheet, period) {
    if (!sheet) throw new Error("prepareCleanPasteRowByPeriod_: sheet 不存在");

    if (this.isFirstHalfByPeriod_(period)) {
      sheet.getRange("A2:BJ").clearContent();
      sheet.getRange("A2:BJ").clearFormat();
      return 2;
    }

    const lastRow = this.findLastNonEmptyRowInColumn_(sheet, 2); // B欄
    return Math.max(lastRow + 1, 2);
  },

  prepareOtherPasteRowByPeriod_(sheet, period) {
  if (!sheet) throw new Error("prepareOtherPasteRowByPeriod_: sheet 不存在");

  if (this.isFirstHalfByPeriod_(period)) {
    sheet.getRange("A2:BJ").clearContent();
    sheet.getRange("A2:BJ").clearFormat();
    return 2;
  }

  const lastRow = this.findLastNonEmptyRowInColumn_(sheet, 2); // B欄
  return Math.max(lastRow + 1, 2);
},

prepareOtherIncomePasteRowByPeriod_(sheet, period) {
  if (!sheet) {
    throw new Error("prepareOtherIncomePasteRowByPeriod_: sheet 不存在");
  }

  // ✅ 上半月：清空後從第 2 列開始
  if (this.isFirstHalfByPeriod_(period)) {
    sheet.getRange("A2:BJ").clearContent();
    sheet.getRange("A2:BJ").clearFormat();
    return 2;
  }

  // ✅ 下半月：不清空，找到最後一筆，空一列再貼
  const lastRow = this.findLastNonEmptyRowInColumn_(sheet, 2); // B欄
  return Math.max(lastRow + 2, 2);
},

  isFirstHalfByPeriod_(period) {
    const text = String(period || "").trim();
    return text.endsWith("-1");
  },

  findLastNonEmptyRowInColumn_(sheet, column) {
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return 1;

    const values = sheet.getRange(1, column, lastRow, 1).getValues();
    for (let i = values.length - 1; i >= 0; i--) {
      if (String(values[i][0]).trim() !== "") {
        return i + 1;
      }
    }
    return 1;
  },

/**
 * 金流檔案轉檔
 */
convertAllPaymentFilesInBackground(runId) {
  appendToBus_(runId, "RUNNING", "📦 掃描並轉換金流檔案...", 10);

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const folder = DriveApp.getFileById(spreadsheet.getId()).getParents().next();
  const period = PeriodManager.getCurrentPeriodFromFilename();
  const region = scriptProperties.getProperty(PROPERTY_KEYS.REGION_NAME);

  if (!period) throw new Error("無法從檔名取得期別");
  if (!region) throw new Error("無法取得區域名稱，請先確認系統設定");

  const zipFiles = [];
  let invoiceZipFound = false;

  const files = folder.getFiles();

  while (files.hasNext()) {
    const file = files.next();
    const name = file.getName();

    if (name.includes(period) && name.includes("發票") && name.toLowerCase().endsWith(".zip")) {
      invoiceZipFound = true;
      appendToBus_(runId, "RUNNING", `📦 解壓發票：${name}`, 25);

      const blobs = Utilities.unzip(file.getBlob());

      if (!blobs.length) {
        throw new Error(`發票 ZIP 內沒有檔案：${name}`);
      }

      blobs.forEach((blob, index) => {
        const baseName = `${period}發票-${region}`;
        const contentType = String(blob.getContentType() || "").toLowerCase();
        const blobName = String(blob.getName() || "").toLowerCase();

        let ext = ".xlsx";

        if (contentType.includes("csv") || blobName.endsWith(".csv")) {
          ext = ".csv";
        } else if (
          contentType.includes("excel") ||
          contentType.includes("spreadsheet") ||
          blobName.endsWith(".xls") ||
          blobName.endsWith(".xlsx")
        ) {
          ext = blobName.endsWith(".xls") && !blobName.endsWith(".xlsx") ? ".xls" : ".xlsx";
        }

        const outputName = `${baseName}${index > 0 ? "-" + (index + 1) : ""}${ext}`;

        const existing = folder.getFilesByName(outputName);
        while (existing.hasNext()) {
          existing.next().setTrashed(true);
        }

        folder.createFile(blob).setName(outputName);
        appendToBus_(runId, "INFO", `📄 已解壓發票檔：${outputName}`);
      });

    } else if (name.includes(period) && name.toLowerCase().endsWith(".zip")) {
      zipFiles.push(file);
    }
  }

  zipFiles.forEach((zipFile, index) => {
    const progress = 25 + Math.round((index / Math.max(1, zipFiles.length)) * 20);
    appendToBus_(runId, "RUNNING", `📦 解壓：${zipFile.getName()}`, progress);

    const zipName = zipFile.getName().replace(/\.zip$/i, "");
    const blobs = Utilities.unzip(zipFile.getBlob());

    blobs.forEach((blob, subIndex) => {
      const contentType = String(blob.getContentType() || "").toLowerCase();
      const blobName = String(blob.getName() || "").toLowerCase();

      let ext = ".xls";
      if (contentType.includes("csv") || blobName.endsWith(".csv")) {
        ext = ".csv";
      } else if (blobName.endsWith(".xlsx")) {
        ext = ".xlsx";
      } else if (blobName.endsWith(".xls")) {
        ext = ".xls";
      }

      const outputName = `${zipName}${subIndex > 0 ? "-" + (subIndex + 1) : ""}${ext}`;

      const existing = folder.getFilesByName(outputName);
      while (existing.hasNext()) {
        existing.next().setTrashed(true);
      }

      folder.createFile(blob).setName(outputName);
      appendToBus_(runId, "INFO", `📄 已解壓檔案：${outputName}`);
    });
  });

  if (!invoiceZipFound) {
    appendToBus_(runId, "WARNING", `⚠️ 找不到發票 ZIP（${period}發票-${region}.zip 或含「發票」的 ZIP）`);
  }

  appendToBus_(runId, "RUNNING", "🔄 批次轉檔為 Google Sheet…", 55);

  const allowedKeywords = [
    "發票",
    "已退款全部加收",
    "已退款全部退款",
    "預收",
    "藍新收款",
    "藍新退款"
  ];

  const filesToConvert = [];
  const allFiles = folder.getFiles();

  while (allFiles.hasNext()) {
    const file = allFiles.next();
    const name = file.getName();
    const lowerName = name.toLowerCase();
    const ext = lowerName.split(".").pop();
    const matched = allowedKeywords.find(keyword => name.includes(keyword));

    if (matched && ["xls", "xlsx", "csv"].includes(ext)) {
      filesToConvert.push(file);
    }
  }

  if (!filesToConvert.length) {
    throw new Error("找不到需要轉檔的檔案");
  }

  let convertedCount = 0;

  filesToConvert.forEach((file, index) => {
    const progress = 55 + Math.round((index / filesToConvert.length) * 35);
    const fileName = file.getName();

    appendToBus_(runId, "RUNNING", `🔄 轉檔：${fileName}`, progress);

    let forcedName = null;

    if (fileName.includes("發票")) {
      forcedName = `${period}發票-${region}`;
    }

    const convertedSheet = FileManager.convertFileToGoogleSheet(file, folder, runId, forcedName);

    if (!convertedSheet) {
      appendToBus_(runId, "WARNING", `⚠️ 轉檔失敗或略過：${fileName}`);
      return;
    }

    const sheet = convertedSheet.getSheets()[0];
    const rowCount = Math.max(0, sheet.getLastRow() - 1);

    const label = getExecutionLabelFromFilename_(fileName);
    if (label) {
      ExecutionLogger.recordExecutionLog(period, label, rowCount);
    }

    convertedCount++;
  });

  appendToBus_(runId, "SUCCESS", `✅ 轉檔完成：${convertedCount} 個檔案`, 95);
},

  /**
   * 搬運發票與藍新資料
   */
  moveInvoiceAndBluePayDataInBackground(runId) {
    appendToBus_(runId, "RUNNING", "📄 搬運發票/藍新資料...", 10);

    const period = PeriodManager.getCurrentPeriodFromFilename();
    const rootFolderId = scriptProperties.getProperty(PROPERTY_KEYS.ROOT_FOLDER_ID);
    if (!rootFolderId) throw new Error("尚未設定根目錄");

    const rootFolder = DriveApp.getFolderById(rootFolderId);
    const folderIter = rootFolder.getFoldersByName(period);
    if (!folderIter.hasNext()) throw new Error(`找不到期別資料夾：${period}`);

    const folder = folderIter.next();
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();

    const targets = [
      { name: "00發票", keyword: "發票", range: "A2:S" },
      { name: "01藍新收款", keyword: "藍新收款", range: "A2:U" },
      { name: "02藍新退款", keyword: "藍新退款", range: "A2:W" }
    ];

    targets.forEach((target, index) => {
      const progress = 15 + Math.round((index / targets.length) * 70);
      const targetSheet = spreadsheet.getSheetByName(target.name);
      if (!targetSheet) throw new Error(`找不到工作表：${target.name}`);

      const files = folder.getFiles();
      let sourceFile = null;

      while (files.hasNext()) {
        const file = files.next();
        if (file.getName().includes(target.keyword) && file.getMimeType() === MimeType.GOOGLE_SHEETS) {
          sourceFile = file;
          break;
        }
      }

      if (!sourceFile) {
        appendToBus_(runId, "WARNING", `⚠️ 找不到 ${target.keyword} 試算表，略過`, progress);
        return;
      }

      appendToBus_(runId, "RUNNING", `📄 搬運：${target.keyword}`, progress);

      const sourceSheet = SpreadsheetApp.openById(sourceFile.getId()).getSheets()[0];
      const range = sourceSheet.getRange(target.range);
      const values = range.getValues().filter(row => row.join("") !== "");
      const backgrounds = range.getBackgrounds().slice(0, values.length);

      targetSheet.getRange(target.range).clearContent();

      if (values.length) {
        targetSheet.getRange(2, 1, values.length, values[0].length).setValues(values);
        targetSheet.getRange(2, 1, values.length, values[0].length).setBackgrounds(backgrounds);
      }

      ExecutionLogger.recordExecutionLog(period, `複製${target.keyword}`, values.length);
    });

    appendToBus_(runId, "SUCCESS", "✅ 發票與藍新搬運完成", 95);
  }
};


// =========================
// 退款與預收資料處理
// 負責搬運已退款與預收資料，並依 A、B、Y 欄組合進行去重。
// =========================
const RefundDataProcessor = {
  processRefundAndPrepaidDataInBackground(runId) {
  appendToBus_(runId, "RUNNING", "🔄 處理退款和預收資料...", 10);

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("範本");
  if (!sheet) throw new Error("找不到範本工作表");

  appendToBus_(runId, "RUNNING", "🔵 先排序範本（E→H→M）", 15);
  this.sortTemplateSheet_(sheet);

  const config = this.getConfiguration_();
  appendToBus_(runId, "RUNNING", "🔎 掃描已退款/預收檔案…", 25);
  const fileMap = this.getFileMapping_(config.folder);

  appendToBus_(runId, "RUNNING", "📥 搬運已退款（加收/退款）…", 35);
  const refundResult = this.moveRefundFiles_(runId, config, fileMap);

  // 如果完全沒有退款資料，就直接處理預收
  if (!refundResult.totalRows) {
    appendToBus_(runId, "INFO", "⚠️ 沒有已退款全部加收/已退款全部退款，直接處理預收", 60);

    this.recordRefundExecution_(config.period, fileMap, {
      "已退款全部加收": 0,
      "已退款全部退款": 0
    });

    appendToBus_(runId, "RUNNING", "📥 搬運預收…", 80);
    this.processPrepaidData_(runId, config, fileMap);

    appendToBus_(runId, "SUCCESS", "✅ 已完成（無退款資料，僅處理預收）", 95);
    return;
  }

  appendToBus_(runId, "RUNNING", "🧹 去重（A+B+Y）…", 60);
  const dedupeResult = this.deduplicateRefundData_(
    config.targetSheet,
    refundResult.startRow,
    refundResult.totalRows,
    refundResult.originalCounts
  );

  this.recordRefundExecution_(config.period, fileMap, dedupeResult.finalCounts);

  appendToBus_(runId, "RUNNING", "📥 搬運預收…", 80);
  this.processPrepaidData_(runId, config, fileMap);

  appendToBus_(runId, "SUCCESS", "✅ 已退款＋預收搬運完成", 95);
},

  sortTemplateSheet_(sheet) {
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return;
    const range = sheet.getRange(2, 1, lastRow - 1, 62);
    const values = range.getValues();
    const sorted = DataProcessor.sortData(values);
    range.setValues(sorted);
  },

  getConfiguration_() {
    const period = PeriodManager.getCurrentPeriodFromFilename();
    const rootFolderId = scriptProperties.getProperty(PROPERTY_KEYS.ROOT_FOLDER_ID);
    if (!rootFolderId) throw new Error("尚未設定根目錄");

    const rootFolder = DriveApp.getFolderById(rootFolderId);
    const folders = rootFolder.getFoldersByName(period);
    if (!folders.hasNext()) throw new Error(`找不到期別資料夾：${period}`);

    return {
      period,
      folder: folders.next(),
      targetSheet: SpreadsheetApp.getActiveSpreadsheet().getSheetByName("範本")
    };
  },

  getFileMapping_(folder) {
    const map = {};
    const files = folder.getFiles();
    while (files.hasNext()) {
      const file = files.next();
      const name = file.getName();
      if (file.getMimeType() === MimeType.GOOGLE_SHEETS) {
        if (name.includes("已退款全部加收")) map["已退款全部加收"] = file.getId();
        if (name.includes("已退款全部退款")) map["已退款全部退款"] = file.getId();
        if (name.includes("預收")) map["預收"] = file.getId();
      }
    }
    return map;
  },

  moveRefundFiles_(runId, config, fileMap) {
    const list = [
      { keyword: "已退款全部加收", label: "複製已退款全部加收" },
      { keyword: "已退款全部退款", label: "複製已退款全部退款" }
    ];

    let startRow = null;
    let totalRows = 0;
    const originalCounts = {
      "已退款全部加收": 0,
      "已退款全部退款": 0
    };

    list.forEach(item => {
      const fileId = fileMap[item.keyword];
      if (!fileId) {
        appendToBus_(runId, "INFO", `⚠️ 找不到 ${item.keyword}，略過`);
        return;
      }

      const result = this.moveFileData_(runId, fileId, item.keyword, config.targetSheet);
      if (result) {
        originalCounts[item.keyword] = result.rowCount;
        totalRows += result.rowCount;
        if (item.keyword === "已退款全部加收") startRow = result.startRow;
      }
    });

    return { startRow, totalRows, originalCounts };
  },

  moveFileData_(runId, fileId, keyword, targetSheet) {
    appendToBus_(runId, "RUNNING", `📥 讀取：${keyword}`, 40);

    const sourceSheet = SpreadsheetApp.openById(fileId).getSheets()[0];
    const rawValues = sourceSheet.getRange("A2:BJ").getValues();
    const rawBackgrounds = sourceSheet.getRange("A2:BJ").getBackgrounds();

    const values = [];
    const backgrounds = [];

    rawValues.forEach((row, index) => {
      if (row.join("").trim() !== "") {
        normalizeRowDates_(row);
        values.push(row);
        backgrounds.push(rawBackgrounds[index]);
      }
    });

    if (!values.length) return null;

    const startRow = DataProcessor.findFirstEmptyRowInColumn(targetSheet, 2);
    const neededRows = startRow + values.length - 1;
    if (neededRows > targetSheet.getMaxRows()) {
      targetSheet.insertRowsAfter(targetSheet.getMaxRows(), neededRows - targetSheet.getMaxRows());
    }

    targetSheet.getRange(startRow, 1, values.length, values[0].length).setValues(values);
    targetSheet.getRange(startRow, 1, values.length, values[0].length).setBackgrounds(backgrounds);
    appendToBus_(runId, "INFO", `✅ ${keyword} 搬運 ${values.length} 筆`);

    return { startRow, rowCount: values.length };
  },

  deduplicateRefundData_(targetSheet, startRow, totalRows, originalCounts) {
    if (!startRow || totalRows === 0) {
      return { finalCounts: originalCounts };
    }

    const values = targetSheet.getRange(startRow, 1, totalRows, 62).getValues();
    const backgrounds = targetSheet.getRange(startRow, 1, totalRows, 62).getBackgrounds();
    const seen = new Set();
    const uniqueRows = [];
    const uniqueBackgrounds = [];

    values.forEach((row, index) => {
      const key = `${row[0] || ""}|${row[1] || ""}|${row[24] || ""}`;
      if (seen.has(key)) return;
      seen.add(key);
      uniqueRows.push(row);
      uniqueBackgrounds.push(backgrounds[index]);
    });

    if (uniqueRows.length !== values.length) {
      targetSheet.getRange(startRow, 1, totalRows, 62).clearContent();
      if (uniqueRows.length) {
        targetSheet.getRange(startRow, 1, uniqueRows.length, 62).setValues(uniqueRows);
        targetSheet.getRange(startRow, 1, uniqueRows.length, 62).setBackgrounds(uniqueBackgrounds);
      }
    }

    const finalCounts = {
      "已退款全部加收": 0,
      "已退款全部退款": 0
    };

    uniqueRows.forEach(row => {
      const eText = String(row[4] || "");
      if (eText.includes("已退款全部加收")) finalCounts["已退款全部加收"]++;
      if (eText.includes("已退款全部退款")) finalCounts["已退款全部退款"]++;
    });

    return { finalCounts };
  },

  recordRefundExecution_(period, fileMap, counts) {
    if (fileMap["已退款全部加收"]) {
      ExecutionLogger.recordExecutionLog(period, "複製已退款全部加收", counts["已退款全部加收"]);
    }
    if (fileMap["已退款全部退款"]) {
      ExecutionLogger.recordExecutionLog(period, "複製已退款全部退款", counts["已退款全部退款"]);
    }
  },

  processPrepaidData_(runId, config, fileMap) {
    const fileId = fileMap["預收"];
    if (!fileId) {
      appendToBus_(runId, "INFO", "⚠️ 找不到預收，略過");
      ExecutionLogger.recordExecutionLog(config.period, "複製預收", 0);
      return;
    }

    const result = this.moveFileData_(runId, fileId, "預收", config.targetSheet);
    ExecutionLogger.recordExecutionLog(config.period, "複製預收", result ? result.rowCount : 0);
  }
};

// =========================
// 執行記錄標籤輔助
// 依檔名對應執行表標籤名稱。
// =========================
function getExecutionLabelFromFilename_(filename) {
  const map = [
    { keyword: "發票", label: "期別發票轉檔" },
    { keyword: "已退款全部加收", label: "期別已退款全部加收轉檔" },
    { keyword: "已退款全部退款", label: "期別已退款全部退款轉檔" },
    { keyword: "預收", label: "期別預收轉檔" },
    { keyword: "藍新收款", label: "期別藍新收款轉檔" },
    { keyword: "藍新退款", label: "期別藍新退款轉檔" }
  ];

  for (const item of map) {
    if (filename.includes(item.keyword)) return item.label;
  }
  return null;
}

// =========================
// 對帳加工處理器
// 負責範本資料排序、異常標記、水洗拆分、VIP券標記與結果回寫。
// =========================


// =========================
// 對帳加工處理器
// 第一階段只做排序、異常、儲值金，不做拆解。
// 拆解統一由 BusinessLogic.expandFGRowsInProcessedRange_ 處理。
// =========================

class ReconciliationProcessor {
  constructor(sheet, startRow, rowCount, runId) {
    this.sheet = sheet;
    this.startRow = startRow;
    this.rowCount = rowCount;
    this.runId = runId;
    this.keywords = ["異動", "請假", "補做", "加時", "減時", "遲到", "薪資", "未服務", "加洗", "加收", "退款"];
  }

  /**
   * 第一階段加工
   * 只做：
   * 1. 排序
   * 2. 標註異常
   * 3. 水洗類別文字去重
   * 4. 儲值金註記
   * 5. 寫回結果
   *
   * 不做：
   * - F/G 拆解
   * - 多列展開
   */
  process() {
    const range = this.sheet.getRange(this.startRow, 1, this.rowCount, 62);
    const data = range.getValues();
    const fontColors = range.getFontColors();
    const backgrounds = range.getBackgrounds();

    if (!data.length) {
      throw new Error("無法讀取資料：資料範圍為空");
    }

    appendToBus_(this.runId, "RUNNING", `📝 讀取 ${data.length} 筆資料`, 25);
    // 🔥 v2026 SAFE: 加工階段不轉換 C/D/H 日期，避免日期被改動。

    const zipped = data.map((row, index) => ({
      row,
      fontColors: fontColors[index] || [],
      backgrounds: backgrounds[index] || []
    }));

    appendToBus_(this.runId, "RUNNING", "🔵 排序中（E→H→M）", 30);
    this.sortRows_(zipped);

    appendToBus_(this.runId, "RUNNING", "🟠 標註異常…", 45);

    let sortCount = 0;
    let markCount = 0;
    const outputRows = [];
    const outputFonts = [];
    const outputBackgrounds = [];
    const orangeRows = [];

    zipped.forEach((entry, index) => {
      const row = entry.row;
      if (row[4] && row[7] && row[12]) sortCount++;

      if (this.markAbnormal_(row)) {
        markCount++;
        orangeRows.push(outputRows.length);
      }

      const eText = String(row[4] || "");
      if (eText.includes("3水洗：")) {
        row[4] = this.dedupeWashCategoryText_(eText);
      }

      outputRows.push(row);
      outputFonts.push(entry.fontColors || []);
      outputBackgrounds.push(entry.backgrounds || []);

      if (index % 10 === 0) {
        const progress = 45 + Math.floor((index / zipped.length) * 40);
        appendToBus_(this.runId, "RUNNING", `處理中 ${index + 1}/${zipped.length}`, progress);
      }
    });

    appendToBus_(this.runId, "RUNNING", "🟡 標記 VIP券/儲值金…", 85);
    this.markStoredValue_(outputRows);

    appendToBus_(this.runId, "RUNNING", "🟢 寫回結果…", 90);
    this.writeResults_(outputRows, outputFonts, outputBackgrounds, orangeRows);

    return {
      sortCount,
      markCount,
      totalRows: outputRows.length
    };
  }

  sortRows_(zipped) {
    zipped.sort((a, b) => {
      const eA = String(a.row[4] || "").trim();
      const eB = String(b.row[4] || "").trim();
      if (eA !== eB) return eA.localeCompare(eB);

      const hA = normalizeDateOnly_(a.row[7]);
      const hB = normalizeDateOnly_(b.row[7]);
      const tA = hA instanceof Date ? hA.getTime() : 0;
      const tB = hB instanceof Date ? hB.getTime() : 0;
      if (tA !== tB) return tA - tB;

      return String(a.row[12] || "").trim().localeCompare(String(b.row[12] || "").trim());
    });
  }

  markAbnormal_(row) {
    const apText = String(row[41] || "");
    const ayText = String(row[50] || "");
    if (this.keywords.some(keyword => apText.includes(keyword) || ayText.includes(keyword))) {
      row[10] = `${apText} ${ayText}`.trim();
      return true;
    }
    return false;
  }

  markStoredValue_(rows) {
    rows.forEach(row => {
      const eText = String(row[4] || "");
      if (eText.includes("VIP券") || eText.includes("儲值金")) {
        row[0] = "儲值金";
      }
    });
  }

  writeResults_(rows, fonts, backgrounds, orangeRows) {
    const writeRowCount = Math.min(rows.length, this.rowCount);
    if (!writeRowCount) return;

    const range = this.sheet.getRange(
      this.startRow,
      1,
      writeRowCount,
      Math.min(rows[0]?.length || 62, 62)
    );
    range.setValues(rows.slice(0, writeRowCount));

    if (fonts[0] && fonts[0].length) {
      range.setFontColors(fonts.slice(0, writeRowCount));
    }
    if (backgrounds[0] && backgrounds[0].length) {
      range.setBackgrounds(backgrounds.slice(0, writeRowCount));
    }

    if (rows.length < this.rowCount) {
      const extraStartRow = this.startRow + rows.length;
      const extraRows = this.rowCount - rows.length;
      this.sheet.getRange(extraStartRow, 1, extraRows, 62).clearContent().clearFormat();
    }

    for (let i = 0; i < Math.min(writeRowCount, rows.length); i++) {
      const absoluteRow = this.startRow + i;
      if (orangeRows.includes(i)) {
        this.sheet.getRange(absoluteRow, 1, 1, rows[i].length).setBackground("#FFCC99");
      }
    }
  }

  dedupeWashCategoryText_(text) {
    const prefix = "3水洗：";
    if (!text.includes(prefix)) return text;

    const idx = text.indexOf(prefix);
    const head = text.slice(0, idx + prefix.length);
    const tail = text.slice(idx + prefix.length).trim();

    if (tail.length % 2 === 0) {
      const half = tail.length / 2;
      const a = tail.slice(0, half);
      const b = tail.slice(half);
      if (a === b) return head + a;
    }

    return text.replace(/(噴抽水洗＋除蟎)\1/g, "$1");
  }
}


// =========================
// 系統初始化
// 負責重建每日與每小時排程檢查器。
// =========================
function initializeSystem() {
  try {
    cleanupTriggersByHandler_(ScriptApp.getProjectTriggers().map(trigger => trigger.getHandlerFunction()));

    ScriptApp.newTrigger("dailyScheduleCheck")
      .timeBased()
      .everyDays(1)
      .atHour(0)
      .nearMinute(10)
      .create();

    ScriptApp.newTrigger("hourlyScheduleCheck")
      .timeBased()
      .everyHours(1)
      .create();

    return {
      success: true,
      message: "系統初始化完成",
      newTriggerCount: 2
    };
  } catch (error) {
    return {
      success: false,
      error: error.message
    };
  }
}

function ensureScheduleHealth_() {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    const hasDaily = triggers.some(trigger => trigger.getHandlerFunction() === "dailyScheduleCheck");
    const hasHourly = triggers.some(trigger => trigger.getHandlerFunction() === "hourlyScheduleCheck");

    if (!hasDaily || !hasHourly) {
      initializeSystem();
    }
  } catch (error) {
    console.error("ensureScheduleHealth_ failed:", error);
  }
}

// =========================
// 設定 / 診斷 / 測試 API
// 提供前端側邊欄呼叫，並確保回傳格式與 HTML 完整一致。
// =========================
function saveBaseSettings(rootFolderId, region) {
  return SettingsManager.saveAllSettings({ rootFolderId, region });
}

function saveLineSettings(token, users) {
  if (!String(token || "").trim() || !String(users || "").trim()) {
    throw new Error("請填寫 LINE Token 和 User IDs");
  }

  scriptProperties.setProperties({
    [PROPERTY_KEYS.LINE_TOKEN]: String(token || "").trim(),
    [PROPERTY_KEYS.LINE_USERS]: String(users || "").trim()
  }, true);

  return {
    success: true,
    message: "✅ LINE 設定已儲存"
  };
}

function clearAllTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => ScriptApp.deleteTrigger(trigger));
  return {
    success: true,
    deletedCount: triggers.length,
    message: `已刪除 ${triggers.length} 個觸發器`
  };
}

function api_testTemplateProcessing() {
  try {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = spreadsheet.getSheetByName("範本");
    const period = PeriodManager.getCurrentPeriodFromFilename();

    if (!sheet) throw new Error("找不到 '範本' 工作表");
    if (!period) throw new Error("無法從檔名取得期別");

    const testRows = [
      ["", "TEST001", "2024/01/01", "2024/01/01", "1專業清潔", "項目1", 1, "2024/01/01", "", "", "", "", "A001", "", "", "", "", "", "", ""],
      ["", "TEST002", "2024/01/02", "2024/01/02", "3水洗：噴抽水洗＋除蟎", "單人床 X 2\n雙人床 X 1", "", "2024/01/02", "", "", "測試異常", "", "A002", "", "", "", "", "", "", ""]
    ];

    if (sheet.getLastRow() < 2) {
      sheet.getRange(2, 1, testRows.length, testRows[0].length).setValues(testRows);
    }

    const runId = startBus_("測試範本加工");
    const processor = new ReconciliationProcessor(sheet, 2, Math.min(2, sheet.getLastRow() - 1), runId);
    const result = processor.process();
    finishBus_(runId, true, "測試完成");

    return {
      success: true,
      period,
      result,
      message: "測試完成"
    };
  } catch (error) {
    return {
      success: false,
      error: error.message,
      stack: error.stack
    };
  }
}

function api_diagnoseSystem() {
  try {
    const result = {
      timestamp: new Date().toISOString(),
      triggers: ScriptApp.getProjectTriggers().map(trigger => ({
        handler: trigger.getHandlerFunction(),
        eventType: String(trigger.getEventType())
      })),
      properties: Object.keys(scriptProperties.getProperties()).map(key => ({
        key,
        hasValue: !!scriptProperties.getProperty(key)
      })),
      busStatus: "未測試"
    };

    const runId = startBus_("系統診斷測試");
    appendToBus_(runId, "INFO", "測試 BUS 系統...", 50);
    finishBus_(runId, true, "測試完成");
    result.busStatus = "正常";

    return { success: true, data: result };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

function api_checkEnvironment() {
  const issues = [];

  try { DriveApp.getRootFolder(); } catch (error) { issues.push(`DriveApp 錯誤: ${error.message}`); }
  try { CacheService.getScriptCache(); } catch (error) { issues.push(`CacheService 錯誤: ${error.message}`); }
  try { PropertiesService.getScriptProperties(); } catch (error) { issues.push(`PropertiesService 錯誤: ${error.message}`); }
  try {
    if (!SpreadsheetApp.getActiveSpreadsheet()) {
      issues.push("沒有活躍的試算表");
    }
  } catch (error) {
    issues.push(`SpreadsheetApp 錯誤: ${error.message}`);
  }

  return {
    timestamp: new Date().toISOString(),
    issues,
    ok: issues.length === 0
  };
}

// =========================
// API 對外函式
// 提供側邊欄前端呼叫的介面，讓 HTML 可讀取設定、啟動任務與查詢進度。
// 已與 HTML 完整對齊。
// =========================
function api_getCurrentSettings() {
  return SettingsManager.getCurrentSettings();
}

function api_setSchedule(payload) {
  return StableScheduler.setSchedule(payload);
}

function api_getScheduleSettings() {
  return StableScheduler.getScheduleSettings();
}

function api_checkScheduleStatus() {
  return StableScheduler.checkScheduleStatus();
}

function api_runScheduleNow() {
  return StableScheduler.runNow();
}

function api_initializeSystem() {
  return initializeSystem();
}

function api_detectCurrentPeriod() {
  return detectCurrentPeriod();
}

function api_startTask(taskName, params) {
  return startTask(taskName, params);
}

function api_pollBusStatus(runId) {
  return pollBusStatus(runId);
}
