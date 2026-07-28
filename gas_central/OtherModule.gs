// Generated from gas_backup/other. Do not edit directly.
var OtherApp = (function () {
/*************************************************
 * 其他承攬 PDF 系統｜下半月分段清空版
 *
 * 前置邏輯：
 * 1. 上半月：清空訂單表 A2:BJ，從「營收明細」第 2 列開始搬資料。
 * 2. 下半月：
 *    - 不清空上半月資料。
 *    - 找「營收明細」B 欄第一個空白列，從下一列開始抓下半月資料。
 *    - 找「訂單表」B 欄第一個空白列，從該列往下清空 A:BJ，避免舊資料殘留。
 *    - 再把下半月資料貼到訂單表第一個空白列。
 * 3. 不使用「xxx下半月營收明細」。
 *************************************************/

var OTHER_CONTRACT_PDF_CONFIG = {
  MENU_NAME: "📄 其他承攬",
  SIDEBAR_FILE_NAME: "面板",
  CONFIG_SHEET_NAME: "_系統設定",
  ROOT_FOLDER_ID_KEY: "OTHER_CONTRACT_ROOT_FOLDER_ID",
  REGION_NAME_KEY: "OTHER_CONTRACT_REGION_NAME",
  PERIOD_OVERRIDE_KEY: "OTHER_CONTRACT_PERIOD_OVERRIDE",
  PDF_OUTPUT_SHEET_NAME: "PDF產出",
  PDF_OUTPUT_START_ROW: 2,
  PDF_OUTPUT_NAME_COL: 2,
  PDF_OUTPUT_TIME_COL: 4,
  PDF_OUTPUT_LINK_COL: 5,
  PDF_OUTPUT_FLAG_COL: 8,
  PDF_OUTPUT_SERVICE_COL: 9,
  TIMEZONE: "Asia/Taipei",
  EXPORT_START_COL: 28,
  EXPORT_START_COL_A1: "AB",
  EXPORT_END_COL_A1: "AH",
  NOTE_MIN_HEIGHT: 20,
  CLEAR_MIN_ROWS: 50,
  PDF_MIN_BYTES: 1000,
  ORDER_START_ROW: 2,
  ORDER_START_COL: 1,
  ORDER_END_COL: 62
};

var SERVICE_TYPES = ["水洗", "家電", "收納", "地毯", "座椅"];

var SERVICE_CONFIG = {
  "水洗": {
    salarySheetName: "水洗薪資單",
    salaryTableName: "水洗薪資表",
    orderSheetName: "水洗訂單",
    incomeSheetName: "水洗營收明細",
    settlementRow: 285,
    noteCell: "AC43",
    noteRow: 43,
    detailStartRow: 46,
    detailTitleRow: 45,
    detailColCount: 5,
    titleRow: ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務項目"]
  },
  "家電": {
    salarySheetName: "家電薪資單",
    salaryTableName: "家電薪資表",
    orderSheetName: "家電訂單",
    incomeSheetName: "家電營收明細",
    settlementRow: 254,
    noteCell: "AC36",
    noteRow: 36,
    detailStartRow: 38,
    detailTitleRow: 37,
    detailColCount: 5,
    titleRow: ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務人"]
  },
  "收納": {
    salarySheetName: "收納薪資單",
    salaryTableName: "收納薪資表",
    orderSheetName: "收納訂單",
    incomeSheetName: "收納營收明細",
    settlementRow: 254,
    noteCell: "",
    noteRow: 0,
    detailStartRow: 30,
    detailTitleRow: 29,
    detailColCount: 5,
    titleRow: ["", "服務日期（星期）", "客戶姓名", "服務時數", "服務項目"]
  },
  "地毯": {
    salarySheetName: "地毯薪資單",
    salaryTableName: "地毯薪資表",
    orderSheetName: "地毯訂單",
    incomeSheetName: "地毯營收明細",
    settlementRow: 254,
    noteCell: "",
    noteRow: 0,
    detailStartRow: 30,
    detailTitleRow: 29,
    detailColCount: 5,
    titleRow: ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務項目"]
  },
  "座椅": {
    salarySheetName: "座椅薪資單",
    salaryTableName: "座椅薪資表",
    orderSheetName: "座椅訂單",
    incomeSheetName: "座椅營收明細",
    settlementRow: 254,
    noteCell: "",
    noteRow: 0,
    detailStartRow: 30,
    detailTitleRow: 29,
    detailColCount: 5,
    titleRow: ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務項目"]
  }
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu(OTHER_CONTRACT_PDF_CONFIG.MENU_NAME)
    .addItem("打開面板", "showSidebar")
    .addSeparator()
    .addItem("執行前置作業", "runAllPreprocess")
    .addItem("執行全部結算", "runAllSettlement")
    .addItem("產出全部薪資單", "generateAllSalaryPDFs_v2025")
    .addToUi();
}

function showSidebar() {
  var html = HtmlService
    .createHtmlOutputFromFile(OTHER_CONTRACT_PDF_CONFIG.SIDEBAR_FILE_NAME)
    .setTitle("其他承攬面板")
    .setWidth(430);
  SpreadsheetApp.getUi().showSidebar(html);
}

function api_getOtherContractSettings() {
  var props = PropertiesService.getScriptProperties();
  return {
    rootFolderId: props.getProperty(OTHER_CONTRACT_PDF_CONFIG.ROOT_FOLDER_ID_KEY) || "",
    regionName: props.getProperty(OTHER_CONTRACT_PDF_CONFIG.REGION_NAME_KEY) || "",
    detectedPeriod: detectCurrentPeriod_()
  };
}

function api_saveOtherContractSettings(rootFolderId, regionName) {
  rootFolderId = String(rootFolderId || "").trim();
  regionName = String(regionName || "").trim();
  if (!rootFolderId) throw new Error("請輸入區域根目錄 Folder ID");
  if (!regionName) throw new Error("請輸入區域名稱");

  try {
    DriveApp.getFolderById(rootFolderId);
  } catch (error) {
    throw new Error("區域根目錄 Folder ID 無效或無權限存取");
  }

  PropertiesService.getScriptProperties().setProperties({
    [OTHER_CONTRACT_PDF_CONFIG.ROOT_FOLDER_ID_KEY]: rootFolderId,
    [OTHER_CONTRACT_PDF_CONFIG.REGION_NAME_KEY]: regionName
  }, true);

  writeSettingsToHiddenSheet_(rootFolderId, regionName);

  return {
    success: true,
    message: "✅ 設定已儲存",
    rootFolderId: rootFolderId,
    regionName: regionName
  };
}

function api_detectCurrentPeriod() {
  return { success: true, period: detectCurrentPeriod_() };
}

function api_runOtherContractJob(jobName, periodCode) {
  jobName = String(jobName || "").trim();
  periodCode = String(periodCode || "").trim();

  if (!/^\d{6}-[12]$/.test(periodCode)) {
    throw new Error("期別格式錯誤，請使用 YYYYMM-1 或 YYYYMM-2");
  }

  return withPeriodOverride_(periodCode, function() {
    var result;
    if (jobName === "前置作業") {
      result = runAllPreprocessCore_();
    } else if (jobName === "全部結算") {
      result = runAllSettlementCore_();
    } else if (jobName === "產出全部薪資單") {
      result = generateAllSalaryPDFsCore_();
    } else {
      throw new Error("不支援的作業：" + jobName);
    }

    return {
      success: true,
      jobName: jobName,
      periodCode: periodCode,
      message: result && result.message ? result.message : "✅ 作業完成",
      detail: result || {}
    };
  });
}

function withPeriodOverride_(periodCode, callback) {
  var props = PropertiesService.getScriptProperties();
  props.setProperty(OTHER_CONTRACT_PDF_CONFIG.PERIOD_OVERRIDE_KEY, periodCode);
  try {
    return callback();
  } finally {
    props.deleteProperty(OTHER_CONTRACT_PDF_CONFIG.PERIOD_OVERRIDE_KEY);
  }
}

function writeSettingsToHiddenSheet_(rootFolderId, regionName) {
  var ss = CentralContext.getSpreadsheet();
  var sheet = ss.getSheetByName(OTHER_CONTRACT_PDF_CONFIG.CONFIG_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(OTHER_CONTRACT_PDF_CONFIG.CONFIG_SHEET_NAME);
    sheet.hideSheet();
  }

  sheet.clear();
  sheet.getRange(1, 1, 4, 2).setValues([
    ["區域根目錄ID", rootFolderId],
    ["區域名稱", regionName],
    ["更新時間", Utilities.formatDate(new Date(), OTHER_CONTRACT_PDF_CONFIG.TIMEZONE, "yyyy/MM/dd HH:mm:ss")],
    ["說明", "PDF路徑：區域根目錄/期別資料夾/期別資料夾"]
  ]);
}

function getConfiguredRootFolderId_() {
  var props = PropertiesService.getScriptProperties();
  var rootFolderId = String(props.getProperty(OTHER_CONTRACT_PDF_CONFIG.ROOT_FOLDER_ID_KEY) || "").trim();
  if (rootFolderId) return rootFolderId;

  var sheet = CentralContext.getSpreadsheet().getSheetByName(OTHER_CONTRACT_PDF_CONFIG.CONFIG_SHEET_NAME);
  if (sheet) {
    rootFolderId = String(sheet.getRange("B1").getValue() || "").trim();
    if (rootFolderId) return rootFolderId;
  }

  throw new Error("尚未設定區域根目錄 Folder ID，請先到面板儲存設定");
}

function getConfiguredRegionName_() {
  var props = PropertiesService.getScriptProperties();
  var regionName = String(props.getProperty(OTHER_CONTRACT_PDF_CONFIG.REGION_NAME_KEY) || "").trim();
  if (regionName) return regionName;

  var sheet = CentralContext.getSpreadsheet().getSheetByName(OTHER_CONTRACT_PDF_CONFIG.CONFIG_SHEET_NAME);
  if (sheet) {
    regionName = String(sheet.getRange("B2").getValue() || "").trim();
    if (regionName) return regionName;
  }

  return "";
}

function runAllPreprocess() {
  var result = runAllPreprocessCore_();
  SpreadsheetApp.getUi().alert(result.message);
}

function runAllSettlement() {
  var result = runAllSettlementCore_();
  SpreadsheetApp.getUi().alert(result.message);
}

function generateAllSalaryPDFs_v2025() {
  var result = generateAllSalaryPDFsCore_();
  SpreadsheetApp.getUi().alert(result.message);
}

function runAllPreprocessCore_() {
  var result = preprocessOtherContractSheets_();
  return {
    message:
      "✅ 前置作業完成\n\n" +
      "期別：" + result.periodDisplay + "\n" +
      "已搬運：" + (result.updatedServices.length ? result.updatedServices.join("、") : "無") + "\n" +
      "略過：" + (result.skippedServices.length ? result.skippedServices.join("、") : "無"),
    result: result
  };
}

function preprocessOtherContractSheets_() {
  var info = getPeriodInfo();
  preprocessSalaryTablesByPeriod_(info.isFirstHalf);

  if (info.isFirstHalf) {
    clearAllOrderSheets_();
  } else {
    clearAllOrderSheetsFromFirstBlankRow_();
  }

  var syncResult = syncAllOrdersFromIncomeSheets_(info.isFirstHalf);

  return {
    periodDisplay: info.display,
    updatedServices: syncResult.updatedServices,
    skippedServices: syncResult.skippedServices
  };
}

function preprocessSalaryTablesByPeriod_(isFirstHalf) {
  preprocessWashingSalaryTable_(isFirstHalf);
  preprocessApplianceSalaryTable_(isFirstHalf);
}

function preprocessWashingSalaryTable_(isFirstHalf) {
  var sheet = getSheetByName(SERVICE_CONFIG["水洗"].salaryTableName);
  if (isFirstHalf) {
    clearValuesFromJToO_(sheet, 284);
    clearValuesFromJToO_(sheet, 280);
    return;
  }
  copyValuesFromJToO_(sheet, 285, 284);
  copyValuesFromJToO_(sheet, 279, 280);
}

function preprocessApplianceSalaryTable_(isFirstHalf) {
  var sheet = getSheetByName(SERVICE_CONFIG["家電"].salaryTableName);
  if (isFirstHalf) {
    clearValuesFromJToO_(sheet, 253);
    clearValuesFromJToO_(sheet, 249);
    return;
  }
  copyValuesFromJToO_(sheet, 254, 253);
  copyValuesFromJToO_(sheet, 249, 250);
}

function clearValuesFromJToO_(sheet, row) {
  sheet.getRange(row, 10, 1, 6).clearContent();
}

function copyValuesFromJToO_(sheet, sourceRow, targetRow) {
  sheet.getRange(targetRow, 10, 1, 6).setValues(sheet.getRange(sourceRow, 10, 1, 6).getValues());
}

function clearAllOrderSheets_() {
  SERVICE_TYPES.forEach(function(serviceType) {
    var serviceConfig = getServiceConfig_(serviceType);
    var orderSheet = getSheetByNameOrNull_(serviceConfig.orderSheetName);
    if (!orderSheet) {
      Logger.log("略過清除，找不到訂單表：" + serviceConfig.orderSheetName);
      return;
    }
    clearOrderSheetFromRow_(orderSheet, OTHER_CONTRACT_PDF_CONFIG.ORDER_START_ROW);
  });
}

function clearAllOrderSheetsFromFirstBlankRow_() {
  SERVICE_TYPES.forEach(function(serviceType) {
    var serviceConfig = getServiceConfig_(serviceType);
    var orderSheet = getSheetByNameOrNull_(serviceConfig.orderSheetName);
    if (!orderSheet) {
      Logger.log("略過下半月分段清除，找不到訂單表：" + serviceConfig.orderSheetName);
      return;
    }

    var clearStartRow = findFirstEmptyRowFromTopByColumn_(orderSheet, 2, OTHER_CONTRACT_PDF_CONFIG.ORDER_START_ROW);
    clearOrderSheetFromRow_(orderSheet, clearStartRow);
  });
}

function clearOrderSheetFromRow_(sheet, startRow) {
  var config = OTHER_CONTRACT_PDF_CONFIG;
  var width = config.ORDER_END_COL - config.ORDER_START_COL + 1;
  var clearRows = Math.max(sheet.getMaxRows() - startRow + 1, 1);
  sheet.getRange(startRow, config.ORDER_START_COL, clearRows, width).clearContent().clearFormat();
}

function syncAllOrdersFromIncomeSheets_(isFirstHalf) {
  var updatedServices = [];
  var skippedServices = [];

  SERVICE_TYPES.forEach(function(serviceType) {
    var result = syncOrderFromIncomeSheetByService_(serviceType, isFirstHalf);
    if (result.updated) {
      updatedServices.push(serviceType);
    } else {
      skippedServices.push(serviceType + "（" + result.reason + "）");
    }
  });

  return { updatedServices: updatedServices, skippedServices: skippedServices };
}

function syncOrderFromIncomeSheetByService_(serviceType, isFirstHalf) {
  var config = OTHER_CONTRACT_PDF_CONFIG;
  var serviceConfig = getServiceConfig_(serviceType);

  var orderSheet = getSheetByNameOrNull_(serviceConfig.orderSheetName);
  if (!orderSheet) return { updated: false, reason: "無訂單表" };

  var incomeSheet = getSheetByNameOrNull_(serviceConfig.incomeSheetName);
  if (!incomeSheet) return { updated: false, reason: "無營收明細" };

  var width = config.ORDER_END_COL - config.ORDER_START_COL + 1;

  var sourceStartRow = isFirstHalf
    ? config.ORDER_START_ROW
    : findFirstEmptyRowFromTopByColumn_(incomeSheet, 2, config.ORDER_START_ROW) + 1;

  var incomeData = getRowsAndFormatsFromIncomeSheet_(incomeSheet, width, sourceStartRow);
  if (!incomeData.rows.length) return { updated: false, reason: "無資料" };

  var pasteRow = findFirstEmptyRowFromTopByColumn_(orderSheet, 2, config.ORDER_START_ROW);
  ensureRows_(orderSheet, pasteRow + incomeData.rows.length - 1);

  var targetRange = orderSheet.getRange(pasteRow, config.ORDER_START_COL, incomeData.rows.length, width);
  targetRange.setValues(incomeData.rows);
  targetRange.setBackgrounds(incomeData.backgrounds);

  return { updated: true, reason: "" };
}

function getRowsAndFormatsFromIncomeSheet_(incomeSheet, width, startRow) {
  startRow = Number(startRow || 2);
  var lastRow = incomeSheet.getLastRow();
  if (lastRow < startRow) return { rows: [], backgrounds: [] };

  var range = incomeSheet.getRange(startRow, 1, lastRow - startRow + 1, width);
  var values = range.getValues();
  var backgrounds = range.getBackgrounds();
  var rows = [];
  var rowBackgrounds = [];

  values.forEach(function(row, index) {
    var hasData = row.some(function(cell) {
      return String(cell || "").trim() !== "";
    });
    if (hasData) {
      rows.push(row);
      rowBackgrounds.push(backgrounds[index]);
    }
  });

  return { rows: rows, backgrounds: rowBackgrounds };
}

function findFirstEmptyRowFromTopByColumn_(sheet, checkCol, startRow) {
  var maxRows = sheet.getMaxRows();
  var rowCount = Math.max(maxRows - startRow + 1, 1);
  var values = sheet.getRange(startRow, checkCol, rowCount, 1).getDisplayValues();

  for (var i = 0; i < values.length; i++) {
    if (!String(values[i][0] || "").trim()) return startRow + i;
  }
  return maxRows + 1;
}

function runAllSettlementCore_() {
  clearPdfOutputSheetB2ToI_();
  var result = SERVICE_TYPES.map(function(serviceType) {
    return serviceType + "：" + settleServiceToPdfOutput_(serviceType) + " 人";
  });
  return { message: "✅ 全部結算完成\n" + result.join("\n"), result: result };
}

function clearPdfOutputSheetB2ToI_() {
  var sheet = getSheetByName(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_SHEET_NAME);
  var lastRow = sheet.getLastRow();
  if (lastRow < OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW) return;

  var rowCount = lastRow - OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW + 1;
  sheet.getRange(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW, 2, rowCount, 8).clearContent().clearFormat();
}

function settleServiceToPdfOutput_(serviceType) {
  var serviceConfig = getServiceConfig_(serviceType);
  var sheet = getSheetByName(serviceConfig.salaryTableName);
  var names = collectNonZeroNamesFromJToO_(sheet, serviceConfig.settlementRow);
  if (!names.length) {
    Logger.log(serviceType + " 結算未抓到任何人");
    return 0;
  }
  return writePdfOutputRows_(serviceType, names);
}

function collectNonZeroNamesFromJToO_(sheet, targetRow) {
  var headerValues = sheet.getRange(1, 10, 1, 6).getDisplayValues()[0];
  var rowValues = sheet.getRange(targetRow, 10, 1, 6).getDisplayValues()[0];
  var names = [];

  for (var i = 0; i < rowValues.length; i++) {
    var name = String(headerValues[i] || "").trim();
    var raw = String(rowValues[i] || "").trim();
    if (!name) continue;
    if (isZeroLike_(raw)) continue;
    names.push(name);
  }
  return Array.from(new Set(names));
}

function isZeroLike_(value) {
  var raw = String(value || "").trim();
  if (!raw) return true;
  if (raw === "-" || raw === "－") return true;
  var num = Number(raw.replace(/,/g, ""));
  return !isNaN(num) && num === 0;
}

function writePdfOutputRows_(serviceType, names) {
  var sheet = getSheetByName(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_SHEET_NAME);
  var startRow = findFirstEmptyPdfOutputRow_();
  var rows = names.map(function(name) {
    return [name, "", "", "", "", "", "Y", serviceType];
  });
  if (!rows.length) return 0;
  ensureRows_(sheet, startRow + rows.length - 1);
  sheet.getRange(startRow, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_NAME_COL, rows.length, 8).setValues(rows);
  return rows.length;
}

function findFirstEmptyPdfOutputRow_() {
  var sheet = getSheetByName(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_SHEET_NAME);
  var lastRow = Math.max(sheet.getLastRow(), OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW);
  var rowCount = lastRow - OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW + 1;
  var values = sheet.getRange(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_NAME_COL, rowCount, 1).getDisplayValues();

  for (var i = 0; i < values.length; i++) {
    if (!String(values[i][0] || "").trim()) return i + OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW;
  }
  return lastRow + 1;
}

function generateAllSalaryPDFsCore_() {
  var pdfOutputSheet = getSheetByName(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_SHEET_NAME);
  var lastRow = pdfOutputSheet.getLastRow();

  if (lastRow < OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW) {
    return { message: "⚠️ PDF產出沒有資料", result: [] };
  }

  var services = pdfOutputSheet.getRange(
    OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW,
    OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_SERVICE_COL,
    lastRow - OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW + 1,
    1
  ).getValues()
    .flat()
    .filter(Boolean)
    .map(function(value) { return String(value).split(/[、,，\sXx]+/); })
    .flat()
    .map(function(value) { return String(value || "").trim(); })
    .filter(Boolean)
    .filter(function(value, index, array) { return array.indexOf(value) === index; });

  var result = [];
  services.forEach(function(serviceType) {
    showToast("📄 開始產出 " + serviceType + " 薪資單");
    var count = generateSalaryPDFs_v2025_ByService_(serviceType);
    result.push(serviceType + "：" + count + " 份");
    Utilities.sleep(1200);
  });

  return { message: "✅ 所有類型 PDF 已完成產出\n" + result.join("\n"), result: result };
}

function generateSalaryPDFs_v2025_ByService_(serviceType) {
  var serviceConfig = getServiceConfig_(serviceType);
  var ss = CentralContext.getSpreadsheet();
  var info = getPeriodInfo();
  var periodCode = info.periodCode;
  var timeOnly = Utilities.formatDate(new Date(), OTHER_CONTRACT_PDF_CONFIG.TIMEZONE, "HH:mm");

  var pdfOutputSheet = getSheetByName(OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_SHEET_NAME);
  var salarySheet = getSheetByName(serviceConfig.salarySheetName);
  var dataSheet = getSheetByName(serviceConfig.salaryTableName);
  var rootFolderId = getConfiguredRootFolderId_();

  var root = DriveApp.getFolderById(rootFolderId);
  var firstPeriodFolder = getSafeFolderByName(root, periodCode, true);
  var folder = getSafeFolderByName(firstPeriodFolder, periodCode, true);

  var lastRow = pdfOutputSheet.getLastRow();
  if (lastRow < OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW) {
    throw new Error("⚠️ PDF產出清單沒有任何資料（B2 起），請填入人員名單再執行。");
  }

  var pdfOutputData = pdfOutputSheet.getRange(
    OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW,
    OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_NAME_COL,
    lastRow - OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW + 1,
    8
  ).getValues();

  var targets = pdfOutputData.map(function(row, index) {
    return {
      name: row[0],
      rowNumber: index + OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_START_ROW,
      existingLink: row[3],
      isTarget: row[6] === "Y",
      service: row[7]
    };
  }).filter(function(item) {
    return item.name && item.isTarget && String(item.service || "").trim() === serviceType;
  });

  if (!targets.length) {
    Logger.log("無符合產出條件的 " + serviceType + " 專員");
    return 0;
  }

  var data = dataSheet.getDataRange().getValues();
  var successCount = 0;

  targets.forEach(function(target) {
    var ok = generateSingleSalaryPdf_(ss, folder, pdfOutputSheet, salarySheet, data, serviceType, serviceConfig, periodCode, timeOnly, target);
    if (ok) successCount++;
  });

  return successCount;
}

function generateSingleSalaryPdf_(ss, folder, pdfOutputSheet, salarySheet, data, serviceType, serviceConfig, periodCode, timeOnly, target) {
  var name = String(target.name || "").trim();
  var rowNumber = target.rowNumber;
  showToast("⏳ 正在產出 " + name + " 的 PDF", serviceType);

  salarySheet.getRange("AD2").setValue(name);
  SpreadsheetApp.flush();

  var details = buildDetailRowsByService_(serviceType, data, name);
  if (!details.length) {
    showToast("⚠️ " + name + " 沒有服務資料，已略過", serviceType);
    pdfOutputSheet.getRange(rowNumber, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_LINK_COL)
      .setValue("⚠️ 無資料，未產出")
      .setBackground("#fff3cd");
    return false;
  }

  details = details.map(function(item, index) {
    item[0] = index + 1;
    return item;
  });

  var detailTitleRow = serviceConfig.detailTitleRow;
  var detailStartRow = serviceConfig.detailStartRow;
  var clearRowCount = salarySheet.getMaxRows() - detailTitleRow + 1;

  salarySheet.getRange(detailTitleRow, OTHER_CONTRACT_PDF_CONFIG.EXPORT_START_COL, clearRowCount, serviceConfig.detailColCount).clearContent();
  salarySheet.getRange(detailTitleRow, OTHER_CONTRACT_PDF_CONFIG.EXPORT_START_COL, 1, serviceConfig.detailColCount).setValues([serviceConfig.titleRow]);
  salarySheet.getRange(detailStartRow, OTHER_CONTRACT_PDF_CONFIG.EXPORT_START_COL, details.length, serviceConfig.detailColCount).setValues(details);

  if (serviceConfig.noteCell && serviceConfig.noteRow) {
    adjustPdfNoteRowHeight_(salarySheet, serviceConfig.noteCell, serviceConfig.noteRow);
  }

  var lastExportRow = findLastExportRow_(salarySheet);
  var rangeA1 = OTHER_CONTRACT_PDF_CONFIG.EXPORT_START_COL_A1 + "1:" + OTHER_CONTRACT_PDF_CONFIG.EXPORT_END_COL_A1 + lastExportRow;

  var spreadsheetUrl = ss.getUrl().replace(/edit$/, "");
  var exportUrl = spreadsheetUrl +
    "export?exportFormat=pdf&format=pdf&gid=" + salarySheet.getSheetId() +
    "&range=" + rangeA1 +
    "&size=A4&portrait=true&fitw=true&sheetnames=false" +
    "&printtitle=false&pagenum=false&gridlines=false&fzr=false&singlePage=true";

  try {
    var response = UrlFetchApp.fetch(exportUrl, {
      headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
      muteHttpExceptions: true
    });

    var blob = response.getBlob();
    if (!blob.getContentType().includes("pdf") || blob.getBytes().length < OTHER_CONTRACT_PDF_CONFIG.PDF_MIN_BYTES) {
      var preview = response.getContentText().substring(0, 200);
      throw new Error("匯出內容非 PDF 或為空白。預覽：" + preview);
    }

    var fileTitle = periodCode + " 檸檬家事｜" + serviceType + "承攬服務費_" + name + ".pdf";
    blob.setName(fileTitle);

    var linkCell = pdfOutputSheet.getRange(rowNumber, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_LINK_COL);
    var existingFileUrl = String(linkCell.getValue() || "").trim();
    var saveResult = savePreservingFileLinkEnhanced(folder, fileTitle, blob, existingFileUrl);

    pdfOutputSheet.getRange(rowNumber, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_TIME_COL).setValue(periodCode + " " + timeOnly);
    if (!existingFileUrl) linkCell.setValue(saveResult.url);
    linkCell.setBackground(null);
    pdfOutputSheet.getRange(rowNumber, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_FLAG_COL).clearContent();

    showToast("✅ " + name + " PDF 儲存完成", serviceType);
    Utilities.sleep(1200);
    return true;
  } catch (error) {
    var errorPreview = error.message || "未知錯誤";
    pdfOutputSheet.getRange(rowNumber, OTHER_CONTRACT_PDF_CONFIG.PDF_OUTPUT_LINK_COL)
      .setValue("❌ PDF 匯出失敗：" + errorPreview)
      .setBackground("#f8d7da");
    showToast("❌ " + name + " PDF 匯出失敗", serviceType);
    return false;
  }
}

function buildDetailRowsByService_(serviceType, data, targetName) {
  var rows = [];
  var index = 1;

  data.forEach(function(r) {
    var dateText = formatDateText_(r[1], r[2]);
    var customer = stringValue_(r[3]);

    if (serviceType === "水洗") {
      var washingStaffText = stringValue_(r[6]);
      var washingStaffList = splitStaffList_(washingStaffText, /[、,，\s]+/);
      if (washingStaffList.indexOf(targetName) === -1) return;

      var rawItem = stringValue_(r[4]).replace(/^\s*3\s*水洗[\:：]\s*/, "").trim();
      var label = rawItem.indexOf("：") >= 0 ? rawItem.split("：")[1] : rawItem;
      var quantity = numericDisplay_(r[8]);
      var serviceItem = stringValue_(r[5]);
      rows.push([index++, dateText + "｜" + label, customer, quantity, serviceItem]);
      return;
    }

    if (serviceType === "收納") {
      var storageStaffText = stringValue_(r[6]);
      var storageStaffList = splitStaffList_(storageStaffText, /[、,，\sXx]+/);
      if (storageStaffList.indexOf(targetName) === -1) return;

      var storageRawItem = stringValue_(r[4]);
      var storageLabel = storageRawItem.indexOf("：") >= 0 ? storageRawItem.split("：")[1] : storageRawItem;
      var hours = numericDisplay_(r[7]);
      rows.push([index++, dateText + "｜" + storageLabel, customer, hours, storageRawItem]);
      return;
    }

    if (serviceType === "家電" || serviceType === "地毯" || serviceType === "座椅") {
      var staff = stringValue_(r[6]);
      if (staff !== targetName) return;

      var serviceItem2 = stringValue_(r[4]);
      var qty = numericDisplay_(r[5]) || stringValue_(r[5]);
      var staff = stringValue_(r[6]);
      rows.push([index++, dateText + "｜" + serviceItem2, customer, qty, staff]);
    }
  });

  return rows;
}

function getPeriodInfo() {
  var props = PropertiesService.getScriptProperties();
  var overridePeriod = String(props.getProperty(OTHER_CONTRACT_PDF_CONFIG.PERIOD_OVERRIDE_KEY) || "").trim();
  var periodCode = overridePeriod || detectCurrentPeriod_();
  var year = periodCode.slice(0, 4);
  var month = periodCode.slice(4, 6);
  var isFirstHalf = periodCode.slice(7) === "1";
  var halfText = isFirstHalf ? "上半月" : "下半月";

  return {
    periodCode: periodCode,
    display: year + "年" + month + "月 " + halfText,
    isFirstHalf: isFirstHalf
  };
}

function detectCurrentPeriod_() {
  var name = CentralContext.getSpreadsheet().getName();
  var match = name.match(/^(\d{6}-[12])/);
  if (match) return match[1];

  var today = new Date();
  var year = today.getFullYear();
  var month = String(today.getMonth() + 1).padStart(2, "0");
  var half = today.getDate() <= 15 ? "1" : "2";
  return year + month + "-" + half;
}

function getSheetByName(name) {
  var sheet = CentralContext.getSpreadsheet().getSheetByName(name);
  if (!sheet) throw new Error("❌ 找不到工作表：「" + name + "」");
  return sheet;
}

function getSheetByNameOrNull_(name) {
  return CentralContext.getSpreadsheet().getSheetByName(name);
}

function getServiceConfig_(serviceType) {
  var serviceConfig = SERVICE_CONFIG[serviceType];
  if (!serviceConfig) throw new Error("❌ 不支援的服務類型：" + serviceType);
  return serviceConfig;
}

function ensureRows_(sheet, requiredLastRow) {
  var maxRows = sheet.getMaxRows();
  if (requiredLastRow > maxRows) sheet.insertRowsAfter(maxRows, requiredLastRow - maxRows);
}

function getSafeFolderByName(parent, name, createIfMissing) {
  var folders = parent.getFoldersByName(name);
  if (folders.hasNext()) return folders.next();
  if (createIfMissing) return parent.createFolder(name);
  throw new Error("❌ 找不到資料夾：「" + name + "」");
}

function showToast(msg, title) {
  CentralContext.getSpreadsheet().toast(msg, title || "執行中", 5);
}

function formatDateText_(value, weekday) {
  var weekdayText = stringValue_(weekday);
  if (!value) return " (" + weekdayText + ")";

  var date = new Date(value);
  if (isNaN(date.getTime())) return stringValue_(value) + " (" + weekdayText + ")";

  return Utilities.formatDate(date, OTHER_CONTRACT_PDF_CONFIG.TIMEZONE, "yyyy/MM/dd") + " (" + weekdayText + ")";
}

function stringValue_(value) {
  return String(value || "").trim();
}

function numericDisplay_(value) {
  var num = Number(value || 0);
  return num ? num.toLocaleString() : "";
}

function splitStaffList_(text, regex) {
  return String(text || "")
    .split(regex || /[、,，\s]+/)
    .map(function(item) { return item.trim(); })
    .filter(Boolean);
}

function findLastExportRow_(salarySheet) {
  var values = salarySheet.getRange("AB1:AH").getDisplayValues();
  for (var i = values.length - 1; i >= 0; i--) {
    var hasValue = values[i].some(function(cell) {
      return String(cell || "").trim() !== "";
    });
    if (hasValue) return i + 1;
  }
  return 1;
}

function adjustPdfNoteRowHeight_(sheet, noteCellA1, rowNumber) {
  var noteRange = sheet.getRange(noteCellA1);
  var noteValue = String(noteRange.getDisplayValue() || "").trim();

  if (noteValue) {
    noteRange.setWrap(true);
    sheet.setRowHeight(rowNumber, OTHER_CONTRACT_PDF_CONFIG.NOTE_MIN_HEIGHT);
    SpreadsheetApp.flush();
    sheet.autoResizeRows(rowNumber, 1);
    var currentHeight = sheet.getRowHeight(rowNumber);
    if (currentHeight < OTHER_CONTRACT_PDF_CONFIG.NOTE_MIN_HEIGHT) {
      sheet.setRowHeight(rowNumber, OTHER_CONTRACT_PDF_CONFIG.NOTE_MIN_HEIGHT);
    }
  } else {
    noteRange.setWrap(false);
    sheet.setRowHeight(rowNumber, OTHER_CONTRACT_PDF_CONFIG.NOTE_MIN_HEIGHT);
  }
}

function extractDriveFileId_(url) {
  if (!url) return "";
  var text = String(url).trim();
  var match =
    text.match(/\/d\/([a-zA-Z0-9_-]+)/) ||
    text.match(/[?&]id=([a-zA-Z0-9_-]+)/) ||
    text.match(/^([a-zA-Z0-9_-]{20,})$/);
  return match ? match[1] : "";
}

function savePreservingFileLinkEnhanced(pdfFolder, fileName, pdfBlob, existingFileUrl) {
  try {
    var existingFileId = extractDriveFileId_(existingFileUrl);

    if (existingFileId) {
      var existingFile = DriveApp.getFileById(existingFileId);
      existingFile.setName(fileName);
      Drive.Files.update({ title: fileName }, existingFileId, pdfBlob);
      try {
        existingFile = DriveApp.getFileById(existingFileId);
        existingFile.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
      } catch (permError1) {}
      return {
        success: true,
        fileId: existingFileId,
        url: existingFile.getUrl(),
        action: "updated_existing_same_link"
      };
    }

    var existingFiles = pdfFolder.getFilesByName(fileName);
    if (existingFiles.hasNext()) {
      var sameNameFile = existingFiles.next();
      var sameNameFileId = sameNameFile.getId();
      Drive.Files.update({ title: fileName }, sameNameFileId, pdfBlob);
      try {
        sameNameFile = DriveApp.getFileById(sameNameFileId);
        sameNameFile.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
      } catch (permError2) {}
      return {
        success: true,
        fileId: sameNameFileId,
        url: sameNameFile.getUrl(),
        action: "updated_same_name_existing"
      };
    }

    var file = pdfFolder.createFile(pdfBlob);
    file.setName(fileName);
    try {
      file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    } catch (permError3) {}

    return {
      success: true,
      fileId: file.getId(),
      url: file.getUrl(),
      action: "created_new"
    };
  } catch (error) {
    throw new Error("PDF儲存失敗: " + error.message);
  }
}


  return {
    adjustPdfNoteRowHeight_: adjustPdfNoteRowHeight_,
    api_detectCurrentPeriod: api_detectCurrentPeriod,
    api_getOtherContractSettings: api_getOtherContractSettings,
    api_runOtherContractJob: api_runOtherContractJob,
    api_saveOtherContractSettings: api_saveOtherContractSettings,
    buildDetailRowsByService_: buildDetailRowsByService_,
    clearAllOrderSheetsFromFirstBlankRow_: clearAllOrderSheetsFromFirstBlankRow_,
    clearAllOrderSheets_: clearAllOrderSheets_,
    clearOrderSheetFromRow_: clearOrderSheetFromRow_,
    clearPdfOutputSheetB2ToI_: clearPdfOutputSheetB2ToI_,
    clearValuesFromJToO_: clearValuesFromJToO_,
    collectNonZeroNamesFromJToO_: collectNonZeroNamesFromJToO_,
    copyValuesFromJToO_: copyValuesFromJToO_,
    detectCurrentPeriod_: detectCurrentPeriod_,
    ensureRows_: ensureRows_,
    extractDriveFileId_: extractDriveFileId_,
    findFirstEmptyPdfOutputRow_: findFirstEmptyPdfOutputRow_,
    findFirstEmptyRowFromTopByColumn_: findFirstEmptyRowFromTopByColumn_,
    findLastExportRow_: findLastExportRow_,
    formatDateText_: formatDateText_,
    generateAllSalaryPDFsCore_: generateAllSalaryPDFsCore_,
    generateAllSalaryPDFs_v2025: generateAllSalaryPDFs_v2025,
    generateSalaryPDFs_v2025_ByService_: generateSalaryPDFs_v2025_ByService_,
    generateSingleSalaryPdf_: generateSingleSalaryPdf_,
    getConfiguredRegionName_: getConfiguredRegionName_,
    getConfiguredRootFolderId_: getConfiguredRootFolderId_,
    getPeriodInfo: getPeriodInfo,
    getRowsAndFormatsFromIncomeSheet_: getRowsAndFormatsFromIncomeSheet_,
    getSafeFolderByName: getSafeFolderByName,
    getServiceConfig_: getServiceConfig_,
    getSheetByName: getSheetByName,
    getSheetByNameOrNull_: getSheetByNameOrNull_,
    isZeroLike_: isZeroLike_,
    numericDisplay_: numericDisplay_,
    onOpen: onOpen,
    preprocessApplianceSalaryTable_: preprocessApplianceSalaryTable_,
    preprocessOtherContractSheets_: preprocessOtherContractSheets_,
    preprocessSalaryTablesByPeriod_: preprocessSalaryTablesByPeriod_,
    preprocessWashingSalaryTable_: preprocessWashingSalaryTable_,
    runAllPreprocess: runAllPreprocess,
    runAllPreprocessCore_: runAllPreprocessCore_,
    runAllSettlement: runAllSettlement,
    runAllSettlementCore_: runAllSettlementCore_,
    savePreservingFileLinkEnhanced: savePreservingFileLinkEnhanced,
    settleServiceToPdfOutput_: settleServiceToPdfOutput_,
    showSidebar: showSidebar,
    showToast: showToast,
    splitStaffList_: splitStaffList_,
    stringValue_: stringValue_,
    syncAllOrdersFromIncomeSheets_: syncAllOrdersFromIncomeSheets_,
    syncOrderFromIncomeSheetByService_: syncOrderFromIncomeSheetByService_,
    withPeriodOverride_: withPeriodOverride_,
    writePdfOutputRows_: writePdfOutputRows_,
    writeSettingsToHiddenSheet_: writeSettingsToHiddenSheet_
  };
})();

function other_adjustPdfNoteRowHeight_() { return OtherApp.adjustPdfNoteRowHeight_.apply(null, arguments); }
function other_api_detectCurrentPeriod() { return OtherApp.api_detectCurrentPeriod.apply(null, arguments); }
function other_api_getOtherContractSettings() { return OtherApp.api_getOtherContractSettings.apply(null, arguments); }
function other_api_runOtherContractJob() { return OtherApp.api_runOtherContractJob.apply(null, arguments); }
function other_api_saveOtherContractSettings() { return OtherApp.api_saveOtherContractSettings.apply(null, arguments); }
function other_buildDetailRowsByService_() { return OtherApp.buildDetailRowsByService_.apply(null, arguments); }
function other_clearAllOrderSheetsFromFirstBlankRow_() { return OtherApp.clearAllOrderSheetsFromFirstBlankRow_.apply(null, arguments); }
function other_clearAllOrderSheets_() { return OtherApp.clearAllOrderSheets_.apply(null, arguments); }
function other_clearOrderSheetFromRow_() { return OtherApp.clearOrderSheetFromRow_.apply(null, arguments); }
function other_clearPdfOutputSheetB2ToI_() { return OtherApp.clearPdfOutputSheetB2ToI_.apply(null, arguments); }
function other_clearValuesFromJToO_() { return OtherApp.clearValuesFromJToO_.apply(null, arguments); }
function other_collectNonZeroNamesFromJToO_() { return OtherApp.collectNonZeroNamesFromJToO_.apply(null, arguments); }
function other_copyValuesFromJToO_() { return OtherApp.copyValuesFromJToO_.apply(null, arguments); }
function other_detectCurrentPeriod_() { return OtherApp.detectCurrentPeriod_.apply(null, arguments); }
function other_ensureRows_() { return OtherApp.ensureRows_.apply(null, arguments); }
function other_extractDriveFileId_() { return OtherApp.extractDriveFileId_.apply(null, arguments); }
function other_findFirstEmptyPdfOutputRow_() { return OtherApp.findFirstEmptyPdfOutputRow_.apply(null, arguments); }
function other_findFirstEmptyRowFromTopByColumn_() { return OtherApp.findFirstEmptyRowFromTopByColumn_.apply(null, arguments); }
function other_findLastExportRow_() { return OtherApp.findLastExportRow_.apply(null, arguments); }
function other_formatDateText_() { return OtherApp.formatDateText_.apply(null, arguments); }
function other_generateAllSalaryPDFsCore_() { return OtherApp.generateAllSalaryPDFsCore_.apply(null, arguments); }
function other_generateAllSalaryPDFs_v2025() { return OtherApp.generateAllSalaryPDFs_v2025.apply(null, arguments); }
function other_generateSalaryPDFs_v2025_ByService_() { return OtherApp.generateSalaryPDFs_v2025_ByService_.apply(null, arguments); }
function other_generateSingleSalaryPdf_() { return OtherApp.generateSingleSalaryPdf_.apply(null, arguments); }
function other_getConfiguredRegionName_() { return OtherApp.getConfiguredRegionName_.apply(null, arguments); }
function other_getConfiguredRootFolderId_() { return OtherApp.getConfiguredRootFolderId_.apply(null, arguments); }
function other_getPeriodInfo() { return OtherApp.getPeriodInfo.apply(null, arguments); }
function other_getRowsAndFormatsFromIncomeSheet_() { return OtherApp.getRowsAndFormatsFromIncomeSheet_.apply(null, arguments); }
function other_getSafeFolderByName() { return OtherApp.getSafeFolderByName.apply(null, arguments); }
function other_getServiceConfig_() { return OtherApp.getServiceConfig_.apply(null, arguments); }
function other_getSheetByName() { return OtherApp.getSheetByName.apply(null, arguments); }
function other_getSheetByNameOrNull_() { return OtherApp.getSheetByNameOrNull_.apply(null, arguments); }
function other_isZeroLike_() { return OtherApp.isZeroLike_.apply(null, arguments); }
function other_numericDisplay_() { return OtherApp.numericDisplay_.apply(null, arguments); }
function other_preprocessApplianceSalaryTable_() { return OtherApp.preprocessApplianceSalaryTable_.apply(null, arguments); }
function other_preprocessOtherContractSheets_() { return OtherApp.preprocessOtherContractSheets_.apply(null, arguments); }
function other_preprocessSalaryTablesByPeriod_() { return OtherApp.preprocessSalaryTablesByPeriod_.apply(null, arguments); }
function other_preprocessWashingSalaryTable_() { return OtherApp.preprocessWashingSalaryTable_.apply(null, arguments); }
function other_runAllPreprocess() { return OtherApp.runAllPreprocess.apply(null, arguments); }
function other_runAllPreprocessCore_() { return OtherApp.runAllPreprocessCore_.apply(null, arguments); }
function other_runAllSettlement() { return OtherApp.runAllSettlement.apply(null, arguments); }
function other_runAllSettlementCore_() { return OtherApp.runAllSettlementCore_.apply(null, arguments); }
function other_savePreservingFileLinkEnhanced() { return OtherApp.savePreservingFileLinkEnhanced.apply(null, arguments); }
function other_settleServiceToPdfOutput_() { return OtherApp.settleServiceToPdfOutput_.apply(null, arguments); }
function other_showToast() { return OtherApp.showToast.apply(null, arguments); }
function other_splitStaffList_() { return OtherApp.splitStaffList_.apply(null, arguments); }
function other_stringValue_() { return OtherApp.stringValue_.apply(null, arguments); }
function other_syncAllOrdersFromIncomeSheets_() { return OtherApp.syncAllOrdersFromIncomeSheets_.apply(null, arguments); }
function other_syncOrderFromIncomeSheetByService_() { return OtherApp.syncOrderFromIncomeSheetByService_.apply(null, arguments); }
function other_withPeriodOverride_() { return OtherApp.withPeriodOverride_.apply(null, arguments); }
function other_writePdfOutputRows_() { return OtherApp.writePdfOutputRows_.apply(null, arguments); }
function other_writeSettingsToHiddenSheet_() { return OtherApp.writeSettingsToHiddenSheet_.apply(null, arguments); }
