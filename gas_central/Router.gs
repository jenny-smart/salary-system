var CentralContext = {
  setRequest: function (spreadsheetId, region, period) {
    if (!spreadsheetId) throw new Error("缺少 spreadsheetId");
    SpreadsheetApp.openById(spreadsheetId);
    var props = PropertiesService.getUserProperties();
    props.setProperties({
      CENTRAL_TARGET_SPREADSHEET_ID: spreadsheetId,
      CENTRAL_REGION: String(region || ""),
      CENTRAL_PERIOD: String(period || "")
    });

    var cfg = CentralMaster.getRegionConfig(region);
    PropertiesService.getScriptProperties().setProperties({
      ROOT_FOLDER_ID: cfg.root_folder_id || "",
      REGION_NAME: cfg.name || region || "",
      PDF_ROOT_FOLDER_ID: cfg.root_folder_id || "",
      OTHER_CONTRACT_ROOT_FOLDER_ID: cfg.root_folder_id || "",
      OTHER_CONTRACT_REGION_NAME: cfg.name || region || ""
    });
  },

  getSpreadsheet: function () {
    var spreadsheetId = PropertiesService.getUserProperties()
      .getProperty("CENTRAL_TARGET_SPREADSHEET_ID");
    if (!spreadsheetId) throw new Error("尚未指定目標期別檔");
    return SpreadsheetApp.openById(spreadsheetId);
  },

  getRegion: function () {
    return PropertiesService.getUserProperties().getProperty("CENTRAL_REGION") || "";
  },

  getPeriod: function () {
    var props = PropertiesService.getUserProperties();
    var period = props.getProperty("CENTRAL_PERIOD") || "";
    if (period) return period;
    var match = this.getSpreadsheet().getName().match(/\d{6}-[12]/);
    if (!match) throw new Error("無法判斷執行期別");
    return match[0];
  }
};

var CentralMaster = {
  getSpreadsheet: function () {
    var id = PropertiesService.getScriptProperties().getProperty("MASTER_SHEET_ID")
      || (typeof MASTER_SHEET_ID !== "undefined" ? MASTER_SHEET_ID : "");
    if (!id) throw new Error("尚未設定 Script Property：MASTER_SHEET_ID");
    return SpreadsheetApp.openById(id);
  },

  getRegionConfig: function (region) {
    var sheet = this.getSpreadsheet().getSheetByName("地區設定");
    if (!sheet) throw new Error("中控表缺少「地區設定」");
    var values = sheet.getDataRange().getDisplayValues();
    var headers = values[0] || [];
    var index = {};
    headers.forEach(function (header, i) { index[String(header).trim()] = i; });
    for (var r = 1; r < values.length; r++) {
      if (String(values[r][index.name] || "").trim() === String(region || "").trim()) {
        return {
          name: values[r][index.name],
          root_folder_id: values[r][index.root_folder_id] || "",
          allowance_id: values[r][index.allowance_id] || "",
          salary_id: values[r][index.salary_id] || "",
          roster_id: values[r][index.roster_id] || "",
          mail_id: values[r][index.mail_id] || ""
        };
      }
    }
    throw new Error("中控表找不到地區設定：" + region);
  },

  recordExecution: function (task, count, period) {
    var region = CentralContext.getRegion();
    period = period || CentralContext.getPeriod();
    var sheet = this.getSpreadsheet().getSheetByName(region);
    if (!sheet) throw new Error("中控表找不到地區工作表：" + region);

    var tasks = sheet.getRange(1, 1, Math.max(sheet.getLastRow(), 1), 1)
      .getDisplayValues().map(function (row) { return String(row[0]).trim(); });
    var row = tasks.indexOf(String(task).trim()) + 1;
    if (!row) throw new Error("中控表找不到打卡項目：" + task);

    var headers = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), 1))
      .getDisplayValues()[0];
    var col = headers.indexOf(period) + 1;
    if (!col) throw new Error("中控表找不到期別欄：" + period);

    if (count !== undefined && count !== null) sheet.getRange(row, col).setValue(count);
    sheet.getRange(row, col + 1).setValue(
      Utilities.formatDate(new Date(), "Asia/Taipei", "yyyy/MM/dd HH:mm:ss")
    );
    return true;
  },

  getExecutionValue: function (task, period) {
    var region = CentralContext.getRegion();
    var sheet = this.getSpreadsheet().getSheetByName(region);
    if (!sheet) return 0;
    var tasks = sheet.getRange(1, 1, Math.max(sheet.getLastRow(), 1), 1)
      .getDisplayValues().map(function (row) { return String(row[0]).trim(); });
    var row = tasks.indexOf(String(task).trim()) + 1;
    var headers = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), 1))
      .getDisplayValues()[0];
    var col = headers.indexOf(period || CentralContext.getPeriod()) + 1;
    return row && col ? Number(sheet.getRange(row, col).getValue()) || 0 : 0;
  }
};

function doGet(e) {
  return routeCentralRequest_(e);
}

function doPost(e) {
  var action = e && e.parameter && e.parameter.action;
  if (action === "syncTriggers" || action === "dispatchNow") {
    return handleRequest_(e);
  }
  if (action === "generatePdf") {
    return handleCentralPdfRequest_(e);
  }
  if (action === "runYuanta") {
    return handleCentralYuantaRequest_(e);
  }
  return routeCentralRequest_(e);
}

function handleCentralYuantaRequest_(e) {
  try {
    var p = (e && e.parameter) || {};
    CentralContext.setRequest(p.spreadsheetId, p.region, p.period);
    var result = cleaning_runBankAccountUpdate(
      String(p.period || "").slice(-2) === "-1"
    );
    return ContentService.createTextOutput(JSON.stringify({
      success: true, result: result
    })).setMimeType(ContentService.MimeType.JSON);
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({
      success: false, message: error.message || String(error)
    })).setMimeType(ContentService.MimeType.JSON);
  }
}

function handleCentralPdfRequest_(e) {
  try {
    var p = (e && e.parameter) || {};
    CentralContext.setRequest(p.spreadsheetId, p.region, p.period);
    var kind = String(p.kind || "CLEANING").toUpperCase();
    var result;
    if (kind === "OTHER") {
      result = other_generateAllSalaryPDFs_v2025();
    } else {
      result = cleaning_generateSalaryPDFsByConfigAndFile_(
        kind === "PROJECT" ? "PROJECT" : "CLEANING",
        p.spreadsheetId,
        false
      );
    }
    return ContentService.createTextOutput(JSON.stringify({
      success: true,
      result: result
    })).setMimeType(ContentService.MimeType.JSON);
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({
      success: false,
      message: error.message || String(error)
    })).setMimeType(ContentService.MimeType.JSON);
  }
}

function routeCentralRequest_(e) {
  var params = (e && e.parameter) || {};
  if (params.action === "syncTriggers" || params.action === "dispatchNow") {
    return handleRequest_(e);
  }

  var app = String(params.app || "").toLowerCase();
  var files = {
    payment: ["PaymentPanel", "金流對帳中央面板"],
    cleaning: ["CleaningPanel", "清潔承攬中央面板"],
    other: ["OtherPanel", "其他承攬中央面板"]
  };
  if (!files[app]) {
    return HtmlService.createHtmlOutput(
      "<h3>LemonSalarySystem 中央 GAS</h3><p>請從薪資系統選擇 GAS 執行。</p>"
    );
  }

  CentralContext.setRequest(
    params.spreadsheetId,
    params.region,
    params.period
  );
  return HtmlService.createHtmlOutputFromFile(files[app][0])
    .setTitle(files[app][1]);
}
