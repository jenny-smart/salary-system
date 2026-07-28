/**
 * ════════════════════════════════════════════════════════════
 * 主控試算表 GAS — 清潔承攬 PDF 產出系統
 * 檔案：LemonSalarySystem_PDFGenerator.gs
 *
 * 功能：
 *   - 面板（Sidebar）顯示各地區設定、期別選擇、進度回報
 *   - 依各地區 config 開啟清潔承攬試算表
 *   - 讀取 PDF產出 / 專案PDF產出工作表（H=Y 的姓名）
 *   - 逐人寫入薪資單 AD2，連動計算後 export PDF
 *   - PDF 存至 Drive：{地區根目錄}/{期別}/{期別} 子資料夾
 *   - 進度即時回報到 Sidebar（正在產：地區 / 姓名）
 *
 * 設定工作表：_PDF設定（A欄=地區名稱, B欄=清潔承攬試算表ID, C欄=根目錄ID）
 * ════════════════════════════════════════════════════════════
 */

// ────────────────────────────────────────────────────────────
// 常數
// ────────────────────────────────────────────────────────────

var CONFIG_SHEET_NAME = "_PDF設定";   // 主控試算表中的設定工作表
var TIMEZONE          = "Asia/Taipei";

// PDF工作設定
var PDF_JOBS = {
  CLEANING: {
    listSheet:   "PDF產出",
    salarySheet: "薪資單",
    fileTitle:   "清潔承攬服務費",
    exportRange: "AB1:AH",   // 匯出欄範圍（列號動態決定）
  },
  PROJECT: {
    listSheet:   "專案PDF產出",
    salarySheet: "專案薪資單",
    fileTitle:   "清潔專案承攬服務費",
    exportRange: "AB1:AH",
  }
};

// ────────────────────────────────────────────────────────────
// 選單 & Sidebar 入口
// ────────────────────────────────────────────────────────────

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("🍋 PDF 產出")
    .addItem("開啟 PDF 產出面板", "openPdfPanel")
    .addSeparator()
    .addItem("初始化設定工作表", "initConfigSheet")
    .addToUi();
}

function openPdfPanel() {
  var html = HtmlService.createHtmlOutputFromFile("PdfPanel")
    .setTitle("🍋 清潔承攬 PDF 產出")
    .setWidth(360);
  SpreadsheetApp.getUi().showSidebar(html);
}

// ────────────────────────────────────────────────────────────
// 設定工作表初始化
// ────────────────────────────────────────────────────────────

function initConfigSheet() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(CONFIG_SHEET_NAME);
  }
  // 寫入標題列
  sheet.getRange("A1:C1").setValues([["地區名稱", "清潔承攬試算表ID", "根目錄ID"]]);
  sheet.getRange("A1:C1").setFontWeight("bold").setBackground("#d0e4f7");
  SpreadsheetApp.getUi().alert("設定工作表已建立：" + CONFIG_SHEET_NAME + "\n請填入各地區資料後再執行 PDF 產出。");
}

// ────────────────────────────────────────────────────────────
// Sidebar 呼叫的 API 函數
// ────────────────────────────────────────────────────────────

/**
 * 取得所有地區設定（供 Sidebar 顯示選單）
 */
function getRegionConfigs() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG_SHEET_NAME);
  if (!sheet) return [];

  var data = sheet.getDataRange().getValues();
  var configs = [];
  for (var i = 1; i < data.length; i++) {  // 跳過標題列
    var name   = String(data[i][0] || "").trim();
    var fileId = String(data[i][1] || "").trim();
    var rootId = String(data[i][2] || "").trim();
    if (name && fileId) {
      configs.push({ name: name, fileId: fileId, rootId: rootId });
    }
  }
  return configs;
}

/**
 * 儲存地區設定（Sidebar 編輯後回寫）
 */
function saveRegionConfigs(configs) {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(CONFIG_SHEET_NAME);
    sheet.getRange("A1:C1").setValues([["地區名稱", "清潔承攬試算表ID", "根目錄ID"]]);
    sheet.getRange("A1:C1").setFontWeight("bold").setBackground("#d0e4f7");
  }

  // 清空資料列（保留標題）
  var lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, 3).clearContent();
  }

  // 寫入新設定
  if (configs.length > 0) {
    var rows = configs.map(function(c) {
      return [c.name, c.fileId, c.rootId];
    });
    sheet.getRange(2, 1, rows.length, 3).setValues(rows);
  }
  return { success: true };
}

/**
 * 執行 PDF 產出
 * @param {string} regionName - 地區名稱（空字串=全部）
 * @param {string} period     - 期別，如 "202604-2"
 * @param {string} jobType    - "CLEANING" 或 "PROJECT"
 */
function runPdfGeneration(regionName, period, jobType) {
  var configs = getRegionConfigs();
  if (!configs.length) {
    return { error: "設定工作表無地區資料，請先填寫 " + CONFIG_SHEET_NAME };
  }

  // 篩選地區
  var targets = regionName
    ? configs.filter(function(c) { return c.name === regionName; })
    : configs;

  if (!targets.length) {
    return { error: "找不到地區：" + regionName };
  }

  var job     = PDF_JOBS[jobType] || PDF_JOBS.CLEANING;
  var results = [];
  var errors  = [];

  for (var ri = 0; ri < targets.length; ri++) {
    var cfg = targets[ri];
    try {
      _setProgress("🏃 地區：" + cfg.name + " 開始...");
      var result = _generateRegionPdfs(cfg, period, job);
      results.push({ region: cfg.name, count: result.count, skipped: result.skipped });
    } catch (e) {
      errors.push({ region: cfg.name, error: e.message });
      _setProgress("❌ " + cfg.name + " 失敗：" + e.message);
    }
  }

  _setProgress("✅ 全部完成");
  return { results: results, errors: errors };
}

// ────────────────────────────────────────────────────────────
// 核心：產出單一地區的 PDF
// ────────────────────────────────────────────────────────────

function _generateRegionPdfs(cfg, period, job) {
  var ss         = SpreadsheetApp.openById(cfg.fileId);
  var listSheet  = ss.getSheetByName(job.listSheet);
  var salarySheet = ss.getSheetByName(job.salarySheet);

  if (!listSheet) throw new Error("找不到工作表：" + job.listSheet);
  if (!salarySheet) throw new Error("找不到工作表：" + job.salarySheet);

  // 讀取 PDF 清單（B欄=姓名, H欄=Y）
  var lastRow = listSheet.getLastRow();
  if (lastRow < 2) {
    return { count: 0, skipped: 0 };
  }

  var listData = listSheet.getRange(2, 1, lastRow - 1, 8).getValues();
  var targets  = listData
    .map(function(row, i) {
      return { name: String(row[1] || "").trim(), row: i + 2, flag: String(row[7] || "").trim() };
    })
    .filter(function(item) { return item.name && item.flag === "Y"; });

  if (!targets.length) {
    _setProgress("  " + cfg.name + "：無 H=Y 的待產出人員");
    return { count: 0, skipped: 0 };
  }

  // 取得 Drive 目標資料夾
  var folder = _getOrCreatePdfFolder(cfg.rootId, period);

  var count   = 0;
  var skipped = 0;

  for (var i = 0; i < targets.length; i++) {
    var target = targets[i];
    _setProgress("📄 " + cfg.name + " ／ " + target.name + "（" + (i + 1) + "/" + targets.length + "）");

    try {
      var fileId = _generateOnePdf(ss, salarySheet, listSheet, target, period, job, folder);

      // 成功：寫入 E欄連結、D欄時間、清除 H欄
      var timeStr = Utilities.formatDate(new Date(), TIMEZONE, "yyyy/MM/dd HH:mm");
      listSheet.getRange(target.row, 4).setValue(timeStr);           // D欄 完成時間
      listSheet.getRange(target.row, 5).setValue(
        "https://drive.google.com/file/d/" + fileId + "/view"
      );                                                              // E欄 連結
      listSheet.getRange(target.row, 8).setValue("");                // H欄 清除Y

      count++;
    } catch (e) {
      _setProgress("  ⚠️ " + target.name + " 失敗：" + e.message);
      // 失敗保留 H=Y 以便重跑
      skipped++;
    }

    Utilities.sleep(800);  // 避免觸發 API 速率限制
  }

  return { count: count, skipped: skipped };
}

// ────────────────────────────────────────────────────────────
// 產出單人 PDF
// ────────────────────────────────────────────────────────────

function _generateOnePdf(ss, salarySheet, listSheet, target, period, job, folder) {
  // 1. 寫入姓名到 AD2（薪資單公式連動）
  salarySheet.getRange("AD2").setValue(target.name);
  SpreadsheetApp.flush();
  Utilities.sleep(1200);  // 等公式計算

  // 2. 找實際最後一列（避免匯出空白頁）
  var lastRow = _findLastExportRow(salarySheet);
  var exportRange = job.exportRange + lastRow;  // 如 "AB1:AH35"

  // 3. 組合 export URL
  var baseUrl = ss.getUrl().replace(/\/edit.*$/, "");
  var exportUrl = baseUrl + "/export" +
    "?exportFormat=pdf&format=pdf" +
    "&gid=" + salarySheet.getSheetId() +
    "&range=" + exportRange +
    "&size=A4&portrait=true&fitw=true" +
    "&sheetnames=false&printtitle=false&pagenum=false" +
    "&gridlines=false&fzr=false" +
    "&top_margin=0.5&bottom_margin=0.5&left_margin=0.5&right_margin=0.5";

  // 4. 取得 PDF Blob
  var token    = ScriptApp.getOAuthToken();
  var response = UrlFetchApp.fetch(exportUrl, {
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true,
  });

  if (response.getResponseCode() !== 200) {
    throw new Error("PDF匯出失敗，HTTP " + response.getResponseCode());
  }

  var blob = response.getBlob();
  if (blob.getBytes().length < 1000) {
    throw new Error("PDF 檔案過小（" + blob.getBytes().length + " bytes），可能為空白");
  }

  // 5. 命名 & 存檔
  var fileName = period + "_" + job.fileTitle + "_" + target.name + ".pdf";
  blob.setName(fileName);

  // 若已有連結，優先更新原檔
  var existingLink = String(listSheet.getRange(target.row, 5).getValue() || "").trim();
  var existingFileId = _extractFileId(existingLink);
  if (existingFileId) {
    try {
      var existingFile = DriveApp.getFileById(existingFileId);
      existingFile.setContent(blob.getDataAsString());  // 更新原檔
      return existingFileId;
    } catch (e) {
      // 原檔不存在，新建
    }
  }

  var file = folder.createFile(blob);
  return file.getId();
}

// ────────────────────────────────────────────────────────────
// 工具函數
// ────────────────────────────────────────────────────────────

/**
 * 取得或建立 PDF 存放資料夾：根目錄/{期別}/{期別}
 */
function _getOrCreatePdfFolder(rootId, period) {
  var root = DriveApp.getFolderById(rootId);

  // 第一層：{期別}
  var periodFolders = root.getFoldersByName(period);
  var periodFolder  = periodFolders.hasNext()
    ? periodFolders.next()
    : root.createFolder(period);

  // 第二層：{期別}（同名子資料夾）
  var subFolders = periodFolder.getFoldersByName(period);
  var subFolder  = subFolders.hasNext()
    ? subFolders.next()
    : periodFolder.createFolder(period);

  return subFolder;
}

/**
 * 找薪資單匯出的實際最後一列（AB 欄有資料的最後列）
 */
function _findLastExportRow(sheet) {
  var col28 = sheet.getRange("AB1:AB").getValues();
  var last  = 1;
  for (var i = col28.length - 1; i >= 0; i--) {
    if (col28[i][0] !== "") { last = i + 1; break; }
  }
  return Math.max(last, 20);  // 最少 20 列
}

/**
 * 從 Drive 連結提取 fileId
 */
function _extractFileId(url) {
  if (!url) return null;
  var m = url.match(/\/d\/([a-zA-Z0-9_-]+)/);
  return m ? m[1] : null;
}

/**
 * 設定進度訊息（供 Sidebar 輪詢）
 */
var _progressMessage = "";
function _setProgress(msg) {
  _progressMessage = msg;
  Logger.log(msg);
}

/**
 * Sidebar 輪詢進度用
 */
function getProgress() {
  return _progressMessage;
}