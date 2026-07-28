/************************************************************
 * ScheduleTrigger.gs
 * Lemon Clean 主控排程觸發器
 *
 * 功能：
 * 1. 讀取主控檔「排程設定」工作表
 * 2. 儲存排程後重新建立 GAS Trigger
 * 3. 到時間後呼叫 GitHub Actions workflow_dispatch
 ************************************************************/

const MASTER_SHEET_ID = "1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA";
const SCHEDULE_SHEET_NAME = "排程設定";
const TRIGGER_HANDLER = "runScheduledGithubAction";

function handleRequest_(e) {
  try {
    const action = (e && e.parameter && e.parameter.action) || "";

    if (action === "syncTriggers") {
      const result = syncScheduleTriggers();
      return jsonOutput_({
        success: true,
        action,
        result
      });
    }

    if (action === "dispatchNow") {
      const result = dispatchGithubWorkflowFromSheet_();
      return jsonOutput_({
        success: true,
        action,
        result
      });
    }

    return jsonOutput_({
      success: false,
      message: "未知 action，請使用 syncTriggers 或 dispatchNow"
    });

  } catch (error) {
    return jsonOutput_({
      success: false,
      message: error.message,
      stack: error.stack
    });
  }
}

function getScheduleSettings_() {
  const ss = SpreadsheetApp.openById(MASTER_SHEET_ID);
  const sheet = ss.getSheetByName(SCHEDULE_SHEET_NAME);

  if (!sheet) {
    throw new Error("找不到工作表：" + SCHEDULE_SHEET_NAME);
  }

  const values = sheet.getDataRange().getValues();
  const settings = {};

  for (let i = 1; i < values.length; i++) {
    const key = String(values[i][0] || "").trim();
    const value = values[i][1];

    if (key) {
      settings[key] = value;
    }
  }

  return settings;
}

function updateScheduleSetting_(key, value) {
  const ss = SpreadsheetApp.openById(MASTER_SHEET_ID);
  const sheet = ss.getSheetByName(SCHEDULE_SHEET_NAME);

  if (!sheet) {
    throw new Error("找不到工作表：" + SCHEDULE_SHEET_NAME);
  }

  const values = sheet.getDataRange().getValues();

  for (let i = 1; i < values.length; i++) {
    const k = String(values[i][0] || "").trim();

    if (k === key) {
      sheet.getRange(i + 1, 2).setValue(value);
      return;
    }
  }

  const nextRow = sheet.getLastRow() + 1;
  sheet.getRange(nextRow, 1).setValue(key);
  sheet.getRange(nextRow, 2).setValue(value);
}

function syncScheduleTriggers() {
  const settings = getScheduleSettings_();

  deleteScheduleTriggers_();

  const enabled = parseBoolean_(settings.enabled);

  if (!enabled) {
    const msg = "排程未啟用，已刪除既有 Trigger";
    writeScheduleResult_(msg);
    return {
      enabled: false,
      message: msg
    };
  }

  const days = parseDays_(settings.days);
  const timeText = String(settings.time || "").trim();
  const timezone = String(settings.timezone || "Asia/Taipei").trim();

  if (!days.length) {
    throw new Error("days 未設定，例如：10,30");
  }

  if (!timeText || !timeText.includes(":")) {
    throw new Error("time 格式錯誤，請使用 HH:MM，例如 17:10");
  }

  const parts = timeText.split(":");
  const hour = Number(parts[0]);
  const minute = Number(parts[1]);

  if (isNaN(hour) || hour < 0 || hour > 23) {
    throw new Error("小時格式錯誤：" + timeText);
  }

  if (isNaN(minute) || minute < 0 || minute > 59) {
    throw new Error("分鐘格式錯誤：" + timeText);
  }

  const created = [];

  days.forEach(function(day) {
    const trigger = ScriptApp.newTrigger(TRIGGER_HANDLER)
      .timeBased()
      .onMonthDay(day)
      .atHour(hour)
      .nearMinute(minute)
      .inTimezone(timezone)
      .create();

    created.push({
      day: day,
      time: timeText,
      timezone: timezone,
      triggerId: trigger.getUniqueId()
    });
  });

  const msg = "已建立 " + created.length + " 個 GAS Trigger：" +
    days.join(",") + " 日 " + timeText;

  writeScheduleResult_(msg);

  return {
    enabled: true,
    days: days,
    time: timeText,
    timezone: timezone,
    created: created,
    message: msg
  };
}

function deleteScheduleTriggers_() {
  const triggers = ScriptApp.getProjectTriggers();

  triggers.forEach(function(trigger) {
    if (trigger.getHandlerFunction() === TRIGGER_HANDLER) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function runScheduledGithubAction() {
  try {
    const settings = getScheduleSettings_();

    if (!parseBoolean_(settings.enabled)) {
      writeScheduleResult_("Trigger 執行時發現 enabled=false，略過");
      return;
    }

    const result = dispatchGithubWorkflowFromSheet_();

    writeScheduleResult_("GitHub Actions 已觸發：" + JSON.stringify(result));

  } catch (error) {
    writeScheduleResult_("Trigger 執行失敗：" + error.message);
    throw error;
  }
}

function dispatchGithubWorkflowFromSheet_() {
  const settings = getScheduleSettings_();

  const action = normalizeAction_(settings.action || settings.task || "create_period");

  const allRegions = parseBoolean_(settings.all_regions);
  const region = allRegions ? "" : String(settings.region || "").trim();

  const period = String(settings.period || "").trim();

  return dispatchGithubWorkflow_({
    period: period,
    region: region,
    action: action
  });
}

function dispatchGithubWorkflow_(payload) {
  const props = PropertiesService.getScriptProperties();

  const token = props.getProperty("GITHUB_TOKEN");
  const owner = props.getProperty("GITHUB_OWNER") || "jenny-smart";
  const repo = props.getProperty("GITHUB_REPO") || "salary-system";
  const workflow = props.getProperty("GITHUB_WORKFLOW") || "scheduler.yml";
  const ref = props.getProperty("GITHUB_REF") || "main";

  if (!token) {
    throw new Error("尚未設定 Script Properties：GITHUB_TOKEN");
  }

  const url =
    "https://api.github.com/repos/" +
    owner + "/" +
    repo +
    "/actions/workflows/" +
    workflow +
    "/dispatches";

  const body = {
    ref: ref,
    inputs: {
      period: payload.period || "",
      region: payload.region || "",
      action: payload.action || "create_period"
    }
  };

  const response = UrlFetchApp.fetch(url, {
    method: "post",
    muteHttpExceptions: true,
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + token,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28"
    },
    payload: JSON.stringify(body)
  });

  const code = response.getResponseCode();
  const text = response.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error("GitHub workflow_dispatch 失敗：" + code + " " + text);
  }

  return {
    success: true,
    statusCode: code,
    workflow: workflow,
    ref: ref,
    inputs: body.inputs
  };
}

function normalizeAction_(value) {
  const text = String(value || "").trim();

  if (
    text === "create_period" ||
    text === "建立期別資料夾與檔案" ||
    text === "期別資料夾" ||
    text === "建立期別"
  ) {
    return "create_period";
  }

  return text || "create_period";
}

function parseBoolean_(value) {
  const text = String(value || "").trim().toLowerCase();
  return text === "true" || text === "1" || text === "yes" || text === "y" || text === "是" || text === "啟用";
}

function parseDays_(value) {
  if (Array.isArray(value)) {
    return value.map(Number).filter(function(n) {
      return n >= 1 && n <= 31;
    });
  }

  return String(value || "")
    .split(",")
    .map(function(x) {
      return Number(String(x).trim());
    })
    .filter(function(n) {
      return !isNaN(n) && n >= 1 && n <= 31;
    });
}

function writeScheduleResult_(message) {
  const now = Utilities.formatDate(
    new Date(),
    "Asia/Taipei",
    "yyyy/MM/dd HH:mm:ss"
  );

  updateScheduleSetting_("last_run", now);
  updateScheduleSetting_("last_result", message);
}

function jsonOutput_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
