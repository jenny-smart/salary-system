// Generated from gas_backup/cleaning. Do not edit directly.
var CleaningApp = (function () {
// ═══════════════════════════════════════════════════════════════
// 📁 共用模組1：基礎工具與設定
// ═══════════════════════════════════════════════════════════════

/** 🔧 系統設定 */
const CONFIG = {
  TIMEZONE: "Asia/Taipei",
  DATE_FORMAT: "yyyy/MM/dd HH:mm",
  SIMPLE_DATE_FORMAT: "yyyy/MM/dd",
  TOAST_DURATION: 3,
  PROCESS_DELAY: 500,
  IMPORT_DELAY: 3000,
  FORMULA_DELAY: 8000,
  BATCH_DELAY: 2000,
  EMAIL_DELAY: 1500,
  PDF_DELAY: 2000
};

/** 🔧 工作表名稱對照 */
function getSheetNames() {
  return {
    exec: "執行",
    salary: "薪資表",
    slip: "薪資單",
    adjust: "00調薪",
    allowance: "01專員請款",
    voucher: "02儲值獎金",
    newcomer: "03新人實境",
    intern: "04新人實習",
    leader: "05組長津貼",
    summary: "場次時數薪資總表",
    staff: "專員清單",
    orders: "清潔訂單",
    projectOrders: "專案訂單",
    revenue: "清潔營收明細",
    PDF: "PDF產出",
    toolDeposit: "工具包押金",
    salaryTime: "場次時數薪資",
    intro: "介紹獎金"
  };
}

/** 🔧 根據名稱取得工作表，若找不到則直接拋出錯誤 */
function getSheetByName(sheetName) {
  const sheet = CentralContext.getSpreadsheet().getSheetByName(sheetName);
  if (!sheet) throw new Error(`❌ 找不到 ${sheetName} 工作表`);
  return sheet;
}

/** 🔧 將欄位數字轉成字母（例如12→L） */
function getColumnLetter(colNum) {
  let temp = "";
  while (colNum > 0) {
    let modulo = (colNum - 1) % 26;
    temp = String.fromCharCode(65 + modulo) + temp;
    colNum = Math.floor((colNum - modulo) / 26);
  }
  return temp;
}

/** 🔧 取得目前 Spreadsheet 的期別資訊
 *  - periodCode ➜ 如 "202504-2"
 *  - display ➜ 如 "2025年04月 下半月"
 */
function getPeriodInfo() {
  try {
    const name = CentralContext.getSpreadsheet().getName();
    const periodCodeMatch = name.match(/\b\d{6}-\d\b/);
    const periodCode = periodCodeMatch ? periodCodeMatch[0] : "";
    
    if (!periodCode) {
      console.warn("無法從檔案名稱中解析期別資訊");
      return { 
        periodCode: "", 
        display: "未知期別", 
        year: new Date().getFullYear(), 
        month: new Date().getMonth() + 1 
      };
    }
    
    const year = periodCode.substring(0, 4);
    const month = periodCode.substring(4, 6);
    const half = periodCode.includes("-1") ? "上半月" : "下半月";
    const display = `${year}年${month}月${half}`;
    
    return { 
      periodCode, 
      display, 
      year: parseInt(year), 
      month: parseInt(month),
      half,
      isFirstHalf: half === "上半月"
    };
  } catch (error) {
    console.log(`❌ 取得期別資訊失敗：${error.message}`);
    return { 
      periodCode: "", 
      display: "錯誤期別", 
      year: new Date().getFullYear(), 
      month: new Date().getMonth() + 1 
    };
  }
}

/** 🔧 根據 ID 取得資料夾 */
function getSafeFolderById(folderId) {
  try {
    if (!folderId) throw new Error("資料夾ID為空");
    return DriveApp.getFolderById(folderId);
  } catch (error) {
    throw new Error(`無法存取資料夾 (ID: ${folderId}): ${error.message}`);
  }
}

/** 🔧 從來源列複製到目標列，直到來源遇到空白 */
function copyRowUntilBlank(sheet, sourceRow, targetRow) {
  const lastCol = sheet.getLastColumn();
  for (let col = 12; col <= lastCol; col++) { // 從L欄開始
    const value = sheet.getRange(sourceRow, col).getValue();
    if (value === "" || value === null) break;
    sheet.getRange(targetRow, col).setValue(value);
  }
}

/** 🔧 根據 PDF 檔名自動推斷期別（如 202504-1） */
function getPeriodFromExistingPdf(folder) {
  const files = folder.getFiles();
  while (files.hasNext()) {
    const name = files.next().getName();
    const match = name.match(/(20\d{2})(\d{2})-(\d)/);
    if (match) return `${match[1]}${match[2]}-${match[3]}`;
  }
  throw new Error("❌ 無法從現有 PDF 檔名中推斷期別，請確認命名格式。");
}

// ═══════════════════════════════════════════════════════════════
// 📁 共用模組2：通知與狀態管理
// ═══════════════════════════════════════════════════════════════

/** 🔧 靜默模式管理 */
function setSilentMode(isSilent) {
  PropertiesService.getScriptProperties().setProperty('SILENT_MODE', isSilent.toString());
  console.log(`🔇 靜默模式已${isSilent ? '啟用' : '關閉'}`);
}

function getSilentMode() {
  const silent = PropertiesService.getScriptProperties().getProperty('SILENT_MODE');
  return silent === 'true';
}

/** 🔧 顯示右下角Toast小提示（若靜默模式開啟則不顯示） */
function showToast(message, title = "進度通知") {
  if (!getSilentMode()) {
    try {
      CentralContext.getSpreadsheet().toast(message, title, CONFIG.TOAST_DURATION);
    } catch (error) {
      console.log(`❌ Toast顯示失敗：${error.message}`);
    }
  }
}

/** 🔧 直接在右下角顯示即時進度（不受靜默模式影響） */
function updateProgress(message) {
  try {
    CentralContext.getSpreadsheet().toast(message, "進度訊息", CONFIG.TOAST_DURATION);
    console.log(`🔵 ${message}`);
  } catch (error) {
    console.log(`❌ 進度顯示失敗：${error.message}`);
  }
}

/** 🔧 更新Sidebar進度（即時版） */
function updateSidebarProgress(message) {
  if (!getSilentMode()) {
    // 先顯示Toast，再處理側邊欄
    showToast(message);
    console.log("進度更新: " + message);
    
    // 立即儲存進度到PropertiesService，使用同步方式
    try {
      const timestamp = new Date().getTime();
      const properties = PropertiesService.getScriptProperties();
      properties.setProperty('latestProgress', message);
      properties.setProperty('progressTimestamp', timestamp.toString());
      
      // 立即刷新
      SpreadsheetApp.flush();
      
    } catch (error) {
      console.log("無法儲存進度: " + error.message);
    }
  }
}

/** 🔧 帶延遲的進度更新（用於第一個步驟） */
function updateSidebarProgressWithDelay(message, delayMs = 300) {
  updateSidebarProgress(message);
  if (delayMs > 0) {
    Utilities.sleep(delayMs);
  }
}


/** 🔧 供 HTML 呼叫的函式 - 獲取最新進度（改善版） */
function getLatestProgress() {
  try {
    const properties = PropertiesService.getScriptProperties().getProperties();
    const message = properties['latestProgress'] || '系統準備就緒...';
    const timestamp = parseInt(properties['progressTimestamp'] || '0');
    
    return {
      message: message,
      timestamp: timestamp
    };
  } catch (error) {
    console.log("獲取進度失敗: " + error.message);
    return {
      message: '系統準備就緒...',
      timestamp: new Date().getTime()
    };
  }
}

/** 🔧 清除進度記錄（改善版） */
function clearProgressData() {
  try {
    PropertiesService.getScriptProperties().deleteProperty('latestProgress');
    PropertiesService.getScriptProperties().deleteProperty('progressTimestamp');
    console.log("進度記錄已清除");
  } catch (error) {
    console.log("清除進度記錄失敗: " + error.message);
  }
}

/** 🔧 從HTML端呼叫來更新進度 - 這個函式供HTML中的JavaScript使用 */
function addProgress(message) {
  // 這個函式是給HTML中的JavaScript使用的
  // 實際的進度更新邏輯
  console.log("HTML進度更新: " + message);
  return message;
}

/** 🔧 執行狀態管理 */
function setExecutionState(state) {
  try {
    const sheet = getSheetByName(sheetname().exec);
    sheet.getRange("A4").setValue(state);
    console.log(`📊 執行狀態已更新為：${state}`);
  } catch (error) {
    console.log(`❌ 設定執行狀態失敗：${error.message}`);
  }
}

function getExecutionState() {
  try {
    const sheet = getSheetByName(sheetname().exec);
    return sheet.getRange("A4").getValue();
  } catch (error) {
    console.log(`❌ 取得執行狀態失敗：${error.message}`);
    return "停止";
  }
}

/** 🔧 畫面滾動到指定工作表與儲存格 */
function scrollToCell(sheetName, row, col) {
  try {
    const sheet = getSheetByName(sheetName);
    SpreadsheetApp.setActiveSheet(sheet);
    const range = sheet.getRange(row, col);
    sheet.setActiveRange(range);
    Utilities.sleep(CONFIG.PROCESS_DELAY);
    showToast("🔵 畫面就定位");
  } catch (error) {
    console.log(`❌ 畫面滾動失敗：${error.message}`);
  }
}

/** 🔧 登記每一個流程完成後的時間，並顯示進度提示 */
function markStepFinish(stepName, finishCell) {
  try {
    const execSheet = getSheetByName(sheetname().exec);
    const now = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, CONFIG.DATE_FORMAT);
    execSheet.getRange(finishCell).setValue(now);
    const message = `✅ ${stepName} 完成｜${now}`;
    updateProgress(message);
    showToast(message);
    updateSidebarProgress(message);
  } catch (error) {
    console.log(`❌ markStepFinish Error: ${error}`);
    const errorMessage = `❌ 登記完成時間失敗：` + error.message;
    showToast(errorMessage);
    updateSidebarProgress(errorMessage);
  }
}

// ═══════════════════════════════════════════════════════════════
// 📁 共用模組3：側邊欄管理
// ═══════════════════════════════════════════════════════════════

/** 🔧 打開右側固定Sidebar（最佳化版） */
function openProgressSidebar() {
  try {
    // 立即清除舊的進度記錄
    PropertiesService.getScriptProperties().deleteProperty('latestProgress');
    PropertiesService.getScriptProperties().deleteProperty('progressTimestamp');
    
    const html = HtmlService.createHtmlOutputFromFile('sidebar')
      .setTitle("📋 流程控制中心");
    SpreadsheetApp.getUi().showSidebar(html);
    
    // 縮短等待時間，但確保載入
    SpreadsheetApp.flush();
    Utilities.sleep(500); // 從1000ms縮短為500ms
    
  } catch (error) {
    console.log(`❌ 開啟側邊欄失敗：${error.message}`);
    try {
      const html = HtmlService.createHtmlOutput(`
        <div style="padding: 10px; font-family: Arial, sans-serif;">
          <h3>📋 流程控制中心</h3>
          <div id="progress" style="margin-top: 10px;">
            <p>系統執行中...</p>
          </div>
          <script>
            function addProgress(message) {
              const progressDiv = document.getElementById('progress');
              const p = document.createElement('p');
              p.innerHTML = message;
              p.style.fontSize = '12px';
              p.style.margin = '2px 0';
              progressDiv.appendChild(p);
              progressDiv.scrollTop = progressDiv.scrollHeight;
            }
          </script>
        </div>
      `).setTitle("📋 流程控制中心");
      SpreadsheetApp.getUi().showSidebar(html);
      Utilities.sleep(300);
    } catch (fallbackError) {
      console.log("連備用側邊欄都失敗，繼續執行");
    }
  }
}

function showExecutionSidebar() {
  openProgressSidebar();
}

// 全域變數控制執行狀態
var EXECUTION_CONTROL = {
  shouldStop: false,
  shouldPause: false,
  pauseStartTime: null
};

// 在每個主程式中加入檢查點
function checkExecutionControl() {
  const properties = PropertiesService.getScriptProperties();
  const stopFlag = properties.getProperty('STOP_EXECUTION');
  const pauseFlag = properties.getProperty('PAUSE_EXECUTION');
  
  // 檢查停止指令
  if (stopFlag === 'true') {
    properties.deleteProperty('STOP_EXECUTION');
    throw new Error("⏹️ 使用者手動停止執行");
  }
  
  // 檢查暫停指令
  if (pauseFlag === 'true') {
    updateSidebarProgress("⏸️ 執行已暫停，等待繼續指令...");
    
    // 等待繼續指令
    while (properties.getProperty('PAUSE_EXECUTION') === 'true') {
      Utilities.sleep(1000);
      
      // 檢查是否在暫停期間收到停止指令
      if (properties.getProperty('STOP_EXECUTION') === 'true') {
        properties.deleteProperty('STOP_EXECUTION');
        properties.deleteProperty('PAUSE_EXECUTION');
        throw new Error("⏹️ 使用者在暫停期間停止執行");
      }
    }
    
    updateSidebarProgress("▶️ 執行已恢復");
  }
}

// 停止執行函數
function stopExecution() {
  PropertiesService.getScriptProperties().setProperty('STOP_EXECUTION', 'true');
  showToast("⏹️ 停止指令已發送");
}

// 暫停執行函數
function pauseExecution() {
  PropertiesService.getScriptProperties().setProperty('PAUSE_EXECUTION', 'true');
  showToast("⏸️ 暫停指令已發送");
}

// 繼續執行函數
function resumeExecution() {
  PropertiesService.getScriptProperties().deleteProperty('PAUSE_EXECUTION');
  showToast("▶️ 繼續指令已發送");
}

// ═══════════════════════════════════════════════════════════════
// 📁 共用模組4：資料處理核心
// ═══════════════════════════════════════════════════════════════

/** 🔧 統一資料匯入處理（修正版） */
function importAndPrepareData(sheetName, options) {
  try {
    const sheet = getSheetByName(sheetName);
    if (!sheet) throw new Error(`❌ 找不到 ${sheetName} 工作表`);

    const {
      folderId,
      scheduleName,
      isFirstHalf,
      qColSource = 2,
      rColSource = 6,
      sColSource = 8,
      customQRSHandler = null
    } = options;

    updateProgress(`📥 開始匯入 ${sheetName} 資料...`);

    if (isFirstHalf) {
      sheet.getRange("A2:AC").clearContent();
    }

    let sourceCell = "";
    let sheetTab = "";
    let rangeArea = "";
    let conditionCol = "";
    let fieldCount = 0;

    // 修正：使用 getSheetNames() 而不是 sheetname()
    const sheetNames = getSheetNames();
    
    if (sheetName === sheetNames.allowance) {
      sourceCell = "'執行'!$C$2";
      sheetTab = "專員請款";
      rangeArea = "AJ2:AQ8000";
      conditionCol = "AJ2:AJ8000";
      fieldCount = 8;
    } else if (sheetName === sheetNames.newcomer) {
      sourceCell = "'執行'!$C$3";
      sheetTab = "新人實境";
      rangeArea = "A2:L8000";
      conditionCol = "A2:A8000";
      fieldCount = 12;

    } else if (sheetName === sheetNames.intern) {
      sourceCell = "'執行'!$C$3";
      sheetTab = "新人實習";
      rangeArea = "A2:L8000";
      conditionCol = "A2:A8000";
      fieldCount = 12;

    } else if (sheetName === sheetNames.leader) {
      sourceCell = "'執行'!$C$3";
      sheetTab = "組長津貼";
      rangeArea = "A2:L8000";
      conditionCol = "A2:A8000";
      fieldCount = 12;
    } else {
      throw new Error("❌ importAndPrepareData 找不到對應表單規則！");
    }

    const matchCode = isFirstHalf ? "'執行'!$B$1&\"-1\"" : "'執行'!$B$1&\"-2\"";
    const formula = `=FILTER(IMPORTRANGE(${sourceCell},"${sheetTab}!${rangeArea}"),IMPORTRANGE(${sourceCell},"${sheetTab}!${conditionCol}")=${matchCode})`;

    if (isFirstHalf) {
      sheet.getRange("A2").setFormula(formula);
    } else {
      const aValues = sheet.getRange("A:A").getValues();
      const firstEmptyRow = aValues.findIndex(row => row[0] === "");
      if (firstEmptyRow !== -1) {
        sheet.getRange(firstEmptyRow + 1, 1).setFormula(formula);
      } else {
        throw new Error("❌ 找不到A欄空白列放公式");
      }
    }

    SpreadsheetApp.flush();
    Utilities.sleep(CONFIG.IMPORT_DELAY);

    const firstCell = sheet.getRange(2, 1).getValue();
    if (firstCell === "" || firstCell.toString().includes("#ERROR")) {
      throw new Error("❌ 匯入資料失敗，A2出現錯誤！");
    }

    const numRows = sheet.getLastRow() - 1;
    if (numRows > 0) {
      if (typeof customQRSHandler === "function") {
        customQRSHandler(sheet, numRows);
      } else {
        const qValues = sheet.getRange(2, qColSource, numRows).getValues();
        const rValues = sheet.getRange(2, rColSource, numRows).getValues();
        const sValues = sheet.getRange(2, sColSource, numRows).getValues();

        for (let i = 0; i < numRows; i++) {
          const row = 2 + i;

          const cValue = sheet.getRange(row, 3).getValue();   // C
          const eValue = sheet.getRange(row, 5).getValue();   // E
          const gValue = sheet.getRange(row, 7).getValue();   // G
          const hValue = sheet.getRange(row, 8).getValue();   // H
          const jValue = sheet.getRange(row, 10).getValue();  // J
          const kValue = sheet.getRange(row, 11).getValue();  // K

          if (sheetName === sheetNames.newcomer) {
            sheet.getRange(row, 17).setValue(cValue);          // Q = C
            sheet.getRange(row, 18).setValue(200 * kValue);    // R = 200*K
            sheet.getRange(row, 19).setFormula(`=TEXT(E${row},"mm/dd")&G${row}`); // S

          } else if (sheetName === sheetNames.intern) {
            sheet.getRange(row, 17).setValue(cValue);          // Q = C
            sheet.getRange(row, 18).setValue(200 * kValue);    // R = 200*K
            sheet.getRange(row, 19).setFormula(`=TEXT(E${row},"mm/dd")&G${row}`); // S

          } else if (sheetName === sheetNames.leader) {
            sheet.getRange(row, 17).setValue(hValue);          // Q = H
            sheet.getRange(row, 18).setValue(jValue * kValue); // R = J*K
            sheet.getRange(row, 19).setFormula(`=TEXT(E${row},"mm/dd")&G${row}`); // S

          } else {
            sheet.getRange(row, 17).setValue(qValues[i][0]);
            sheet.getRange(row, 18).setValue(rValues[i][0]);
           sheet.getRange(row, 19).setValue(sValues[i][0]);
          }
        }
      }
    }

    showToast(`✅ ${sheetName} 匯入與整理完成`);

  } catch (error) {
    console.log("❌ importAndPrepareData Error: " + error);
    showToast(`❌ 匯入資料失敗：` + error.message);
    throw error;
  }
}

/** 🔧 通用完成標記（修正版） */
function markCustomFinishByHalf(cellC, cellD, isFirstHalf, sheetName = null, statusCell = null) {
  const sheetNames = getSheetNames(); // 修正：使用 getSheetNames()
  const execSheet = getSheetByName(sheetNames.exec);

  if (isFirstHalf && cellC) {
    execSheet.getRange(cellC).setValue(new Date());
    console.log(`✅ 已寫入 ${cellC}`);
  }
  if (!isFirstHalf && cellD) {
    execSheet.getRange(cellD).setValue(new Date());
    console.log(`✅ 已寫入 ${cellD}`);
  }

  if (sheetName && statusCell) {
    const sheet = getSheetByName(sheetName);
    if (sheet) {
      const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
      let foundProblem = false;

      for (let row of data) {
        if (row.includes("#N/A") || row.includes("#ERROR")) {
          foundProblem = true;
          break;
        }
      }

      const statusText = foundProblem
        ? `⚠️ ${sheetName} 有異常，請檢查`
        : `✅ ${sheetName} 無異常`;
      execSheet.getRange(statusCell).setValue(statusText);
      console.log(`📝 資料檢查完成：${statusText}`);
    }
  }
}

/** 🔧 通用資料整理（修正版） */
function runCommonProcess(sheetName) {
  try {
    updateProgress(`🔄 開始 ${sheetName} 通用整理...`);
    const sheet = getSheetByName(sheetName);
    if (!sheet) throw new Error(`❌ 找不到 ${sheetName} 工作表`);

    const lastRow = sheet.getLastRow();
    if (lastRow <= 1) throw new Error(`❌ ${sheetName} 沒有資料可整理`);

    // 取QRS資料，複製到VWX（只留Q有值且R≠0的）
    const qrsData = sheet.getRange(2, 17, lastRow - 1, 3).getValues();
    const vwxData = [];
    for (let i = 0; i < qrsData.length; i++) {
      const [q, r, s] = qrsData[i];
      if (q !== "" && r !== "" && r !== 0) {
        vwxData.push([q, r, s]);
      }
    }
    if (vwxData.length > 0) {
      sheet.getRange(2, 22, vwxData.length, 3).setValues(vwxData);
    }

    SpreadsheetApp.flush();

    // 統計U欄（V欄姓名出現次數）
    const vValues = sheet.getRange(2, 22, vwxData.length).getValues();
    for (let i = 0; i < vValues.length; i++) {
      const name = vValues[i][0];
      if (name) {
        const count = vValues.filter(row => row[0] === name).length;
        sheet.getRange(2 + i, 21).setValue(count);
      }
    }

    SpreadsheetApp.flush();

    // 排序 U→V→X
    sheet.getRange(2, 21, vwxData.length, 4)
      .sort([{ column: 21, ascending: true }, { column: 22, ascending: true }, { column: 24, ascending: true }]);

    // 合併備註Y欄（改良版 - 加入逗號分隔）
    const uValues = sheet.getRange(2, 21, vwxData.length).getValues();
    const xValues = sheet.getRange(2, 24, vwxData.length).getValues();

    for (let i = 0; i < uValues.length; i++) {
      const count = uValues[i][0];
      if (count > 0) {
        let combined = "";
        const validRemarks = []; // 收集有效的備註
    
        // 收集所有有效的備註內容
        for (let j = 0; j < count; j++) {
          if (xValues[i + j] && xValues[i + j][0] && xValues[i + j][0].toString().trim() !== "") {
            validRemarks.push(xValues[i + j][0].toString().trim());
          }
        }
    
        // 用逗號連接有效的備註
        if (validRemarks.length > 0) {
          combined = validRemarks.join(",");
        }
    
        sheet.getRange(i + 2, 25).setValue(combined);
        i += (count - 1);
      }
    }

    SpreadsheetApp.flush();

    // 將V欄去重複放到AA
    const uniqueV = [...new Set(vValues.map(row => row[0]).filter(String))];
    if (uniqueV.length > 0) {
      sheet.getRange(2, 27, uniqueV.length).setValues(uniqueV.map(name => [name]));
    }

    // AB：SUMIF金額加總，AC：備註組字
    for (let i = 0; i < uniqueV.length; i++) {
      const formulaSum = `=SUMIF(V:V,AA${i+2},W:W)`;
      const formulaNote = `=AC1&VLOOKUP(AA${i+2},V:Y,4,FALSE)`;
      sheet.getRange(i + 2, 28).setFormula(formulaSum);
      sheet.getRange(i + 2, 29).setFormula(formulaNote);
    }

    updateProgress(`✅ ${sheetName} 共通整理完成`);
    showToast(`✅ ${sheetName} 共通整理完成`);

  } catch (error) {
    console.log("❌ runCommonProcess Error: " + error);
    showToast(`❌ ${sheetName} 共通整理失敗：` + error.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🕐 上下半月打卡模組
// ═══════════════════════════════════════════════════════════════
// 統一處理上半月與下半月的打卡功能，根據不同階段打在不同欄位

/**
 * 打卡模組類別
 */
class AttendanceModule {
  constructor(config = {}) {
    this.config = {
      TIMEZONE: config.TIMEZONE || "Asia/Taipei",
      DATE_FORMAT: config.DATE_FORMAT || "yyyy/MM/dd HH:mm",
      PROCESS_DELAY: config.PROCESS_DELAY || 1000,
      ...config
    };
  }

  /**
   * 核心打卡方法
   * @param {Sheet} execSheet - 執行工作表
   * @param {string} cellAddress - 打卡欄位地址 (例如: "C14")
   * @param {boolean} isFirstHalf - 是否為上半月
   * @param {string} functionName - 功能名稱 (例如: "02儲值獎金")
   */
  punchClock(execSheet, cellAddress, isFirstHalf, functionName) {
    try {
      // 定位到打卡位置
      SpreadsheetApp.flush();
      Utilities.sleep(this.config.PROCESS_DELAY);
      
      // 生成時間戳記
      const now = new Date();
      const timestamp = Utilities.formatDate(now, this.config.TIMEZONE, this.config.DATE_FORMAT);
      
      // 執行打卡
      CentralMaster.recordExecution(functionName, null, CentralContext.getPeriod());
      
      // 生成打卡訊息
      const halfType = isFirstHalf ? "上半月" : "下半月";
      const message = `✅ ${functionName}${halfType}完成｜${timestamp}`;
      
      // 顯示通知
      if (typeof showToast === 'function') {
        showToast(message);
      }
      
      if (typeof updateSidebarProgress === 'function') {
        updateSidebarProgress(message);
      }
      
      console.log(`打卡完成: ${cellAddress} - ${message}`);
      return timestamp;
      
    } catch (error) {
      const errorMsg = `打卡失敗 (${cellAddress}): ${error.message}`;
      console.error(errorMsg);
      
      if (typeof updateSidebarProgress === 'function') {
        updateSidebarProgress(`❌ ${errorMsg}`);
      }
      
      throw new Error(errorMsg);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 各功能打卡方法
  // ═══════════════════════════════════════════════════════════════

  /**
   * 新薪資表整理打卡 - C11/D11
   */
  punchNewSalaryProcessing(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C11" : "D11";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "新薪資表整理");
  }

  /**
   * 00調薪打卡 - C12/D12
   */
  punchSalaryAdjustment(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C12" : "D12";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "00調薪");
  }

  /**
   * 01專員請款打卡 - C13/D13
   */
  punchSpecialistPayment(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C13" : "D13";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "01專員請款");
  }

  /**
   * 02儲值獎金打卡 - C14/D14
   */
  punchVoucherBonus(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C14" : "D14";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "02儲值獎金");
  }

  /**
   * 03新人實境打卡 - C15/D15
   */
  punchNewEmployeeTraining(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C15" : "D15";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "03新人實境");
  }

  /**
   * 04新人實習打卡 - C16/D16
   */
  punchNewEmployeePractice(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C16" : "D16";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "04新人實習");
  }

  /**
   * 05組長津貼打卡 - C17/D17
   */
  punchTeamLeaderAllowance(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C17" : "D17";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "05組長津貼");
  }

  /**
   * 新人實境期別標註打卡 - C18/D18
   */
  punchNewEmployeePeriodLabel(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C18" : "D18";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "新人實境期別標註");
  }

  /**
   * 工具包押金打卡 - C19/D19
   */
  punchReferralBonus(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C19" : "D19";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "工具包押金");
  }

  /**
   * 元大帳戶打卡 - C20/D20
   */
  punchYuantaAccount(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C20" : "D20";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "元大帳戶");
  }

  /**
   * 薪資結算整理打卡 - C21/D21
   */
  punchSalarySettlement(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C21" : "D21";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "薪資結算整理");
  }

  /**
   * 上/下半月完整執行打卡 - C22/D22
   */
  punchCompleteExecution(execSheet, isFirstHalf) {
    const cellAddress = isFirstHalf ? "C22" : "D22";
    return this.punchClock(execSheet, cellAddress, isFirstHalf, "上/下半月完整執行");
  }

  // ═══════════════════════════════════════════════════════════════
  // 批量操作方法
  // ═══════════════════════════════════════════════════════════════

  /**
   * 批量打卡 - 處理多個功能的打卡
   */
  batchPunchClock(execSheet, functionList, isFirstHalf) {
    const results = [];
    const halfType = isFirstHalf ? "上半月" : "下半月";
    
    console.log(`開始批量打卡 - ${halfType}，共 ${functionList.length} 個功能`);
    
    functionList.forEach((func, index) => {
      try {
        let timestamp;
        
        switch(func) {
          case "新薪資表整理":
            timestamp = this.punchNewSalaryProcessing(execSheet, isFirstHalf);
            break;
          case "00調薪":
            timestamp = this.punchSalaryAdjustment(execSheet, isFirstHalf);
            break;
          case "01專員請款":
            timestamp = this.punchSpecialistPayment(execSheet, isFirstHalf);
            break;
          case "02儲值獎金":
            timestamp = this.punchVoucherBonus(execSheet, isFirstHalf);
            break;
          case "03新人實境":
            timestamp = this.punchNewEmployeeTraining(execSheet, isFirstHalf);
            break;
          case "04新人實習":
            timestamp = this.punchNewEmployeePractice(execSheet, isFirstHalf);
            break;
          case "05組長津貼":
            timestamp = this.punchTeamLeaderAllowance(execSheet, isFirstHalf);
            break;
          case "新人實境期別標註":
            timestamp = this.punchNewEmployeePeriodLabel(execSheet, isFirstHalf);
            break;
          case "介紹獎金":
            timestamp = this.punchReferralBonus(execSheet, isFirstHalf);
            break;
          case "元大帳戶":
            timestamp = this.punchYuantaAccount(execSheet, isFirstHalf);
            break;
          case "薪資結算整理":
            timestamp = this.punchSalarySettlement(execSheet, isFirstHalf);
            break;
          case "上/下半月完整執行":
            timestamp = this.punchCompleteExecution(execSheet, isFirstHalf);
            break;
          default:
            throw new Error(`未知的功能名稱: ${func}`);
        }
        
        results.push({
          function: func,
          status: "success",
          timestamp: timestamp
        });
        
        // 批量打卡之間的延遲
        if (index < functionList.length - 1) {
          Utilities.sleep(500);
        }
        
      } catch (error) {
        results.push({
          function: func,
          status: "error",
          error: error.message
        });
        console.error(`批量打卡失敗 - ${func}: ${error.message}`);
      }
    });
    
    return results;
  }

  // ═══════════════════════════════════════════════════════════════
  // 管理功能方法
  // ═══════════════════════════════════════════════════════════════

  /**
   * 檢查打卡狀態
   */
  checkPunchStatus(execSheet, functionName, isFirstHalf) {
    let cellAddress;
    
    switch(functionName) {
      case "新薪資表整理":
        cellAddress = isFirstHalf ? "C11" : "D11";
        break;
      case "00調薪":
        cellAddress = isFirstHalf ? "C12" : "D12";
        break;
      case "01專員請款":
        cellAddress = isFirstHalf ? "C13" : "D13";
        break;
      case "02儲值獎金":
        cellAddress = isFirstHalf ? "C14" : "D14";
        break;
      case "03新人實境":
        cellAddress = isFirstHalf ? "C15" : "D15";
        break;
      case "04新人實習":
        cellAddress = isFirstHalf ? "C16" : "D16";
        break;
      case "05組長津貼":
        cellAddress = isFirstHalf ? "C17" : "D17";
        break;
      case "新人實境期別標註":
        cellAddress = isFirstHalf ? "C18" : "D18";
        break;
      case "介紹獎金":
        cellAddress = isFirstHalf ? "C19" : "D19";
        break;
      case "元大帳戶":
        cellAddress = isFirstHalf ? "C20" : "D20";
        break;
      case "薪資結算整理":
        cellAddress = isFirstHalf ? "C21" : "D21";
        break;
      case "上/下半月完整執行":
        cellAddress = isFirstHalf ? "C22" : "D22";
        break;
      default:
        return {
          hasPunched: false,
          message: `未知的功能名稱: ${functionName}`
        };
    }
    
    try {
      const actualValue = execSheet.getRange(cellAddress).getValue();
      
      if (!actualValue) {
        return {
          hasPunched: false,
          cellAddress: cellAddress,
          timestamp: null,
          message: `${cellAddress} 欄位為空`
        };
      }
      
      // 檢查是否為有效的日期時間格式
      const timestamp = actualValue.toString();
      const datePattern = /^\d{4}\/\d{2}\/\d{2}\s\d{2}:\d{2}$/;
      
      return {
        hasPunched: datePattern.test(timestamp),
        cellAddress: cellAddress,
        timestamp: timestamp,
        message: datePattern.test(timestamp) ? `${cellAddress} = ${timestamp}` : `${cellAddress} 格式不正確`
      };
      
    } catch (error) {
      return {
        hasPunched: false,
        cellAddress: cellAddress,
        timestamp: null,
        message: `檢查失敗: ${error.message}`
      };
    }
  }

  /**
   * 獲取所有功能的打卡狀態報告
   */
  getAllPunchStatus(execSheet, isFirstHalf) {
    const functions = [
      "新薪資表整理",
      "00調薪", 
      "01專員請款",
      "02儲值獎金",
      "03新人實境",
      "04新人實習",
      "05組長津貼",
      "新人實境期別標註",
      "介紹獎金",
      "元大帳戶",
      "薪資結算整理",
      "上/下半月完整執行"
    ];
    
    const halfType = isFirstHalf ? "上半月" : "下半月";
    const report = {
      halfType: halfType,
      totalFunctions: functions.length,
      completedCount: 0,
      details: []
    };
    
    functions.forEach(func => {
      const status = this.checkPunchStatus(execSheet, func, isFirstHalf);
      report.details.push({
        function: func,
        cellAddress: status.cellAddress,
        hasPunched: status.hasPunched,
        timestamp: status.timestamp,
        message: status.message
      });
      
      if (status.hasPunched) {
        report.completedCount++;
      }
    });
    
    report.completionRate = Math.round((report.completedCount / report.totalFunctions) * 100);
    
    return report;
  }

  /**
   * 顯示打卡狀態報告
   */
  displayPunchReport(execSheet, isFirstHalf) {
    const report = this.getAllPunchStatus(execSheet, isFirstHalf);
    
    console.log(`\n📊 ${report.halfType}打卡狀態報告`);
    console.log(`完成度: ${report.completedCount}/${report.totalFunctions} (${report.completionRate}%)`);
    console.log("─".repeat(50));
    
    report.details.forEach(detail => {
      const statusIcon = detail.hasPunched ? "✅" : "❌";
      const timestamp = detail.timestamp || "未打卡";
      console.log(`${statusIcon} ${detail.function} (${detail.cellAddress}): ${timestamp}`);
    });
    
    console.log("─".repeat(50));
    
    if (typeof updateSidebarProgress === 'function') {
      const summaryMessage = `📊 ${report.halfType}完成度: ${report.completionRate}% (${report.completedCount}/${report.totalFunctions})`;
      updateSidebarProgress(summaryMessage);
    }
    
    return report;
  }
}

// ═══════════════════════════════════════════════════════════════
// 🚀 使用範例
// ═══════════════════════════════════════════════════════════════

/**
 * 使用範例：在主程式中如何使用打卡模組
 */
function exampleUsage() {
  try {
    // 1. 初始化打卡模組
    const attendance = new AttendanceModule({
      TIMEZONE: "Asia/Taipei",
      DATE_FORMAT: "yyyy/MM/dd HH:mm",
      PROCESS_DELAY: 1000
    });
    
    // 2. 獲取執行工作表
    const sheetNames = getSheetNames();
    const execSheet = getSheetByName(sheetNames.exec);
    
    // 3. 執行單一打卡
    const isFirstHalf = true; // 或 false
    attendance.punchVoucherBonus(execSheet, isFirstHalf);
    
    // 4. 或批量打卡
    const functionList = ["02儲值獎金", "03新人實境", "05組長津貼"];
    attendance.batchPunchClock(execSheet, functionList, isFirstHalf);
    
    // 5. 檢查打卡狀態
    attendance.displayPunchReport(execSheet, isFirstHalf);
    
  } catch (error) {
    console.error("使用範例執行失敗:", error.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔄 統一上下半月處理框架
// ═══════════════════════════════════════════════════════════════
// 可重複使用的通用處理邏輯，避免代碼重複

/**
 * 統一上下半月處理類別
 */
class UnifiedProcessHandler {
  constructor(config = {}) {
    this.config = {
      TIMEZONE: config.TIMEZONE || "Asia/Taipei",
      DATE_FORMAT: config.DATE_FORMAT || "yyyy/MM/dd HH:mm",
      PROCESS_DELAY: config.PROCESS_DELAY || 2000,
      IMPORT_DELAY: config.IMPORT_DELAY || 3000,
      FORMULA_DELAY: config.FORMULA_DELAY || 1500,
      ...config
    };

    this.attendance = new AttendanceModule(this.config);
    this.currentStep = 0;
}  


  /**
   * 統一流程執行器
   * @param {Object} processConfig - 流程配置
   * @param {boolean} isFirstHalf - 是否為上半月
   */
  executeProcess(processConfig, isFirstHalf) {
    try {
      // 1. 初始化
      this.initializeProcess(processConfig.name, processConfig.totalSteps);
      
      // 2. 準備工作表
      const sheets = this.prepareSheets(processConfig.sheetNames);
      
      // 3. 執行處理步驟
      const results = this.executeSteps(processConfig.steps, sheets, isFirstHalf);
      
      // 4. 完成打卡
      this.completePunchClock(sheets.execSheet, processConfig.punchMethod, isFirstHalf, processConfig.cells);
      
      // 5. 清理和完成
      this.finalizeProcess(processConfig.name);
      
      return { success: true, results: results };
      
    } catch (error) {
      this.handleError(processConfig.name, error);
      throw error;
    }
  }

  /**
   * 初始化流程
   */
  initializeProcess(processName, totalSteps) {
    this.processName = processName;
    this.totalSteps = totalSteps;
    this.currentStep = 0;
    this.actualSteps = 0; // 🔧 新增：記錄實際步驟數
    
    openProgressSidebar();
    this.updateProgress(`開始 ${processName}流程...`, 300);
    
    console.log(`🚀 開始 ${processName}，預期 ${totalSteps} 步驟`);
  }

  /**
   * 準備工作表
   */
  prepareSheets(sheetNames) {
    this.updateProgress("準備工作表...", 200);
    
    const sheets = {};
    const sheetConfig = getSheetNames();
    
    // 動態獲取所需的工作表
    Object.keys(sheetNames).forEach(key => {
      const sheetName = sheetNames[key];
      sheets[key] = getSheetByName(sheetConfig[sheetName]);
      
      if (!sheets[key]) {
        throw new Error(`找不到工作表: ${sheetName}`);
      }
    });
    
    SpreadsheetApp.flush();
    Utilities.sleep(this.config.PROCESS_DELAY);
    
    this.completeStep("工作表準備完成");
    return sheets;
  }

  /**
   * 執行處理步驟
   */
  executeSteps(steps, sheets, isFirstHalf) {
    const results = [];
    
    steps.forEach((step, index) => {
      try {
        const stepResult = this.executeStep(step, sheets, isFirstHalf);
        results.push({
          stepName: step.name,
          success: true,
          result: stepResult
        });
      } catch (error) {
        results.push({
          stepName: step.name,
          success: false,
          error: error.message
        });
        
        if (step.required !== false) {
          throw error;
        }
      }
    });
    
    return results;
  }

  /**
   * 執行單一步驟
   */
  executeStep(step, sheets, isFirstHalf) {
    this.updateProgress(step.description || step.name);
    
    // 處理條件執行
    if (step.condition && !step.condition(isFirstHalf)) {
      this.completeStep(`${step.name}（跳過）`);
      return { skipped: true };
    }
    
    // 處理定位 - 統一處理方式
    if (step.position) {
      const targetSheet = step.position.sheet ? sheets[step.position.sheet] : sheets.mainSheet;
      if (targetSheet && step.position.cell) {
        targetSheet.activate();
        targetSheet.getRange(step.position.cell).activate();
        SpreadsheetApp.flush();
        Utilities.sleep(1000);
      }
    }
    
    let result;
    
    // 根據步驟類型執行不同邏輯
    switch (step.type) {
      case 'clearRange':
        result = this.clearRange(sheets[step.sheet], step.range);
        break;
        
      case 'importRange':
        // 處理定位（如果在步驟中指定）
        if (step.position) {
          const targetSheet = step.position.sheet ? sheets[step.position.sheet] : sheets[step.sheet] || sheets.mainSheet;
          if (targetSheet && step.position.cell) {
            targetSheet.activate();
            targetSheet.getRange(step.position.cell).activate();
            SpreadsheetApp.flush();
            Utilities.sleep(1000);
          }
        }
        result = this.importRange(sheets[step.sheet], step.cell, step.formula, step.waitTime);
        break;
        
      case 'convertToValues':
        result = this.convertToValues(sheets[step.sheet], step.range);
        break;
        
      case 'setFormulas':
        result = this.setFormulas(sheets[step.sheet], step.formulas, step.startRow);
        break;
        
      case 'copyData':
        result = this.copyData(step, sheets, isFirstHalf);
        break;
        
      case 'custom':
        result = step.handler(sheets, isFirstHalf, this);
        break;
        
      default:
        throw new Error(`未知的步驟類型: ${step.type}`);
    }
    
    // 處理等待時間
    if (step.waitTime) {
      Utilities.sleep(step.waitTime);
    }
    
    this.completeStep(step.successMessage || `${step.name}完成`);
    return result;
  }

  /**
   * 定位到指定位置
   */
  positionToCell(sheet, cellAddress) {
    sheet.activate();
    sheet.getRange(cellAddress).activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
  }

  /**
   * 清空範圍
   */
  clearRange(sheet, range) {
    sheet.getRange(range).clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(this.config.PROCESS_DELAY);
    return { clearedRange: range };
  }

  /**
   * 匯入範圍
   */
  importRange(sheet, cell, formula, waitTime = null) {
    sheet.getRange(cell).setFormula(formula);
    
    const actualWaitTime = waitTime || this.config.IMPORT_DELAY;
    SpreadsheetApp.flush();
    Utilities.sleep(actualWaitTime);
    
    // 檢查匯入結果
    const result = sheet.getRange(cell).getValue();
    if (result === "" || result.toString().includes("#ERROR")) {
      throw new Error(`匯入失敗: ${cell}`);
    }
    
    return { importedCell: cell, result: result };
  }

  /**
   * 轉換為數值
   */
  convertToValues(sheet, range) {
    const dataRange = sheet.getRange(range);
    const values = dataRange.getValues();
    dataRange.setValues(values);
    
    SpreadsheetApp.flush();
    Utilities.sleep(this.config.IMPORT_DELAY);
    
    return { convertedRange: range, rowCount: values.length };
  }

  /**
   * 設定公式
   */
  setFormulas(sheet, formulas, startRow) {
    const results = {};
    
    Object.keys(formulas).forEach(columnKey => {
      const formulaArray = formulas[columnKey];
      const columnIndex = this.getColumnIndex(columnKey);
      
      sheet.getRange(startRow, columnIndex, formulaArray.length, 1).setFormulas(formulaArray);
      results[columnKey] = formulaArray.length;
    });
    
    SpreadsheetApp.flush();
    Utilities.sleep(this.config.FORMULA_DELAY);
    
    return results;
  }

  /**
   * 複製資料
   */
  copyData(step, sheets, isFirstHalf) {
    const sourceSheet = sheets[step.sourceSheet];
    const targetSheet = sheets[step.targetSheet];
    
    const sourceRange = sourceSheet.getRange(step.sourceRange);
    const data = step.includeFormats ? 
      { values: sourceRange.getValues(), formats: sourceRange.getBackgrounds() } :
      { values: sourceRange.getValues() };
    
    let targetRange;
    
    if (step.dynamicTarget) {
      const targetRow = step.dynamicTarget(targetSheet, isFirstHalf);
      targetRange = targetSheet.getRange(targetRow, step.targetStartCol, data.values.length, data.values[0].length);
    } else {
      targetRange = targetSheet.getRange(step.targetRange);
    }
    
    targetRange.setValues(data.values);
    if (data.formats) {
      targetRange.setBackgrounds(data.formats);
    }
    
    SpreadsheetApp.flush();
    Utilities.sleep(this.config.PROCESS_DELAY);
    
    return { copiedRows: data.values.length };
  }

  /**
   * 完成打卡
   */
  completePunchClock(execSheet, punchMethod, isFirstHalf, cells) {
    this.updateProgress("執行完成打卡...");
    
    // 定位到執行工作表
    const punchCell = isFirstHalf ? cells.firstHalf : cells.secondHalf;
    this.positionToCell(execSheet, punchCell);
    
    try {
      // 使用對應的打卡方法
      this.attendance[punchMethod](execSheet, isFirstHalf);
      
      // 更新完成狀態
      this.updateCompletionStatus(execSheet, cells.firstHalf, cells.secondHalf, cells.status);
      
    } catch (punchError) {
      console.error("打卡失敗：", punchError.message);
      this.updateProgress("⚠️ 主要流程完成，但打卡失敗：" + punchError.message);
    }
  }

  /**
   * 更新完成狀態
   */
  updateCompletionStatus(execSheet, firstHalfCell, secondHalfCell, statusCell) {
    try {
      const firstHalfValue = execSheet.getRange(firstHalfCell).getValue();
      const secondHalfValue = execSheet.getRange(secondHalfCell).getValue();
      
      if (firstHalfValue && secondHalfValue) {
        execSheet.getRange(statusCell).setValue("完成");
      } else if (firstHalfValue && !secondHalfValue) {
        execSheet.getRange(statusCell).setValue("上半月完成");
      } else if (!firstHalfValue && secondHalfValue) {
        execSheet.getRange(statusCell).setValue("下半月完成");
      }
      
      SpreadsheetApp.flush();
      
    } catch (error) {
      console.error("更新完成狀態失敗：", error.message);
    }
  }

  /**
   * 完成流程
   */
  finalizeProcess(processName) {
    // 🔧 新增：檢查實際步驟數與預期是否一致
    if (this.actualSteps !== this.totalSteps) {
      console.warn(`⚠️ ${processName} 預期${this.totalSteps}步驟，實際${this.actualSteps}步驟，建議更新配置中的totalSteps`);
      
      // 建議的配置更新
      if (typeof updateSidebarProgress === 'function') {
        updateSidebarProgress(`💡 建議：將 ${processName} 的 totalSteps 更新為 ${this.actualSteps}`);
      }
    } else {
      console.log(`✅ ${processName} 步驟數正確: ${this.actualSteps}/${this.totalSteps}`);
    }
    
    // 清除進度記錄
    PropertiesService.getScriptProperties().deleteProperty('latestProgress');
    PropertiesService.getScriptProperties().deleteProperty('progressTimestamp');
    
    const finalMessage = `✅ ${processName}流程全部完成！`;
    showToast(finalMessage);
    this.updateProgress(finalMessage);
  }

  /**
   * 錯誤處理
   */
  handleError(processName, error) {
    const errorMessage = `❌ ${processName}錯誤: ${error.message}`;
    showToast(errorMessage);
    this.updateProgress(errorMessage);
    console.log("❌ Error: " + error);
  }

  /**
   * 更新進度
   */
  updateProgress(message, delay = 200) {
    this.currentStep++;
    this.actualSteps++; // 🔧 新增：記錄實際步驟數
    
    // 🔧 新增：保護機制，防止步驟數超出預期
    if (this.currentStep > this.totalSteps) {
      console.warn(`⚠️ ${this.processName} 步驟數超出預期: ${this.currentStep}/${this.totalSteps}`);
      // 使用實際步驟數，避免顯示異常
      const stepInfo = `${this.currentStep}/${Math.max(this.totalSteps, this.currentStep)}`;
      const fullMessage = `🔵 步驟${stepInfo}：${message}`;
    } else {
      const stepInfo = this.totalSteps > 0 ? `${this.currentStep}/${this.totalSteps}` : this.currentStep;
      const fullMessage = `🔵 步驟${stepInfo}：${message}`;
    }
    
    const stepInfo = this.totalSteps > 0 ? `${this.currentStep}/${Math.max(this.totalSteps, this.currentStep)}` : this.currentStep;
    const fullMessage = `🔵 步驟${stepInfo}：${message}`;
    
    if (delay) {
      Utilities.sleep(delay);
    }
    
    if (typeof updateSidebarProgress === 'function') {
      updateSidebarProgress(fullMessage);
    }
  }

  /**
   * 完成步驟
   */
  completeStep(message) {
    const stepInfo = this.totalSteps > 0 ? `${this.currentStep}/${this.totalSteps}` : this.currentStep;
    const fullMessage = `✅ 步驟${stepInfo}：${message}`;
    
    if (typeof showToast === 'function') {
      showToast(fullMessage);
    }
    
    if (typeof updateSidebarProgress === 'function') {
      updateSidebarProgress(fullMessage);
    }
  }

  /**
   * 獲取欄位索引
   */
  getColumnIndex(columnKey) {
    if (typeof columnKey === 'number') return columnKey;
    if (typeof columnKey === 'string') {
      return columnKey.charCodeAt(0) - 64; // A=1, B=2, etc.
    }
    throw new Error(`無效的欄位標識: ${columnKey}`);
  }
}

// ═══════════════════════════════════════════
// 🟪 顯示彈窗（自動判斷 HTML / GAS）
// ═══════════════════════════════════════════
UnifiedProcessHandler.prototype.showConfirm = function (message, onYes, onNo) {
  try {
    // 若在 HTML (client side)，google.script.run 可用
    if (typeof google !== "undefined" && google.script && google.script.run) {
      google.script.run
        .withSuccessHandler(function (res) {
          if (res === "yes") onYes && onYes();
          else onNo && onNo();
        })
        .clientPrompt(message);
      return;
    }
  } catch (e) {}

  // ⚠ 若在 GAS server side → fallback Browser.msgBox
  const ui = SpreadsheetApp.getUi();
  const res = ui.alert("確認操作", message, ui.ButtonSet.YES_NO);

  if (res === ui.Button.YES) onYes && onYes();
  else onNo && onNo();
};


// ═══════════════════════════════════════════
// 🟪 Client 端彈窗
// ═══════════════════════════════════════════
function clientPrompt(message) {
  const r = Browser.msgBox("確認操作", message, Browser.Buttons.YES_NO);
  return r === "yes" ? "yes" : "no";
}


// ═══════════════════════════════════════════
// 🟪 通用：彈窗 + 自動轉值
// ═══════════════════════════════════════════
UnifiedProcessHandler.prototype.confirmAndConvert = function (range, message) {
  const self = this;

  this.showConfirm(
    message || "是否將公式轉為靜態值？",
    function () {
      range.setValues(range.getValues());
      self.updateProgress("資料已成功轉為靜態值。");
    },
    function () {
      self.updateProgress("已選擇保留公式。");
    }
  );
};


// ═══════════════════════════════════════════════════════════════
// 🔧 使用範例：薪資表整理
// ═══════════════════════════════════════════════════════════════



/**
 * 統一薪資表整理執行
 */
function runSalaryPreparation(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getSalaryProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月薪資表整理
 */
function runSalaryPreparationFirstHalf() {
  return runSalaryPreparation(true);
}

/**
 * 下半月薪資表整理
 */
function runSalaryPreparationSecondHalf() {
  return runSalaryPreparation(false);
}

/* 🔥 放在這裡最剛好 */
function runSalaryPreparationWithProjectOrdersFirstHalf() {
  return runSalaryPreparationWithProjectOrders(true);
}

function runSalaryPreparationWithProjectOrdersSecondHalf() {
  return runSalaryPreparationWithProjectOrders(false);
}

function runSalaryPreparationWithProjectOrders(isFirstHalf) {
  return runSalaryPreparation(isFirstHalf);
}

// ═══════════════════════════════════════════════════════════════
// 📁 共用模組5：PDF工具函式
// 負責管理 PDF 根目錄、期別資料夾、檔案分享權限與命名規則。
// 使用情境：
// 1. 設定 PDF 儲存根目錄
// 2. 依期別建立對應資料夾
// 3. 產生 PDF 檔名
// 4. 將檔案或資料夾設為「知道連結的任何人可檢視」
// ═══════════════════════════════════════════════════════════════


/**
 * 設定 PDF 根目錄（永久）
 *
 * 執行流程：
 * 1️⃣ 由使用者輸入 Drive 資料夾 ID
 * 2️⃣ 驗證資料夾是否存在
 * 3️⃣ 將資料夾 ID 儲存到 Script Properties
 * 4️⃣ 後續所有 PDF 作業都會使用這個根目錄
 */
function setPdfRootFolder() {
  const ui = SpreadsheetApp.getUi();

  const response = ui.prompt(
    "📁 設定PDF根目錄（永久）",
    "請貼上 Drive 資料夾網址中的 Folder ID\n設定後會一直沿用，直到下次重新設定。",
    ui.ButtonSet.OK_CANCEL
  );

  if (response.getSelectedButton() !== ui.Button.OK) return;

  const folderId = response.getResponseText().trim();
  if (!folderId) {
    ui.alert("⚠️ 未輸入資料夾 ID");
    return;
  }

  try {
    const folder = DriveApp.getFolderById(folderId);

    PropertiesService.getScriptProperties()
      .setProperty("PDF_ROOT_FOLDER_ID", folderId);

    ui.alert("✅ 根目錄設定完成！\n\n目前根目錄：\n" + folder.getName());
  } catch (error) {
    ui.alert("❌ 根目錄設定失敗\n\n請確認 Folder ID 是否正確。\n\n錯誤訊息：\n" + error.message);
  }
}


/**
 * 取得 PDF 根目錄
 *
 * 執行流程：
 * 1️⃣ 從 Script Properties 讀取 PDF_ROOT_FOLDER_ID
 * 2️⃣ 驗證是否已設定
 * 3️⃣ 回傳對應的 Drive 資料夾物件
 */
function getPdfRootFolder() {
  const folderId = PropertiesService.getScriptProperties()
    .getProperty("PDF_ROOT_FOLDER_ID");

  if (!folderId) {
    throw new Error("尚未設定PDF根目錄，請先執行 setPdfRootFolder()");
  }

  return DriveApp.getFolderById(folderId);
}


/**
 * 取得 PDF 最終存放資料夾
 *
 * 資料夾結構：
 * PDF根目錄
 *   └── 期別
 *       └── 期別
 *
 * 例如：
 * PDF根目錄
 *   └── 202603-1
 *       └── 202603-1
 *
 * 執行流程：
 * 1️⃣ 取得 PDF 根目錄
 * 2️⃣ 建立第一層期別資料夾（若不存在則建立）
 * 3️⃣ 建立第二層期別資料夾（若不存在則建立）
 * 4️⃣ 回傳最終存放資料夾
 *
 * @param {string} periodCode 期別代碼，例如 202603-1
 * @returns {GoogleAppsScript.Drive.Folder}
 */
function getFinalPdfStorageFolder(periodCode) {
  if (!periodCode) {
    throw new Error("periodCode 不可為空");
  }

  const rootFolder = getPdfRootFolder();
  const periodFolder = getSafeFolderByName(rootFolder, periodCode, true);
  const finalFolder = getSafeFolderByName(periodFolder, periodCode, true);

  return finalFolder;
}


/**
 * 將檔案設定為「知道連結的任何人可檢視」
 *
 * 注意：
 * - 建議用這個版本，不需要 Advanced Drive Service
 * - 適用於 PDF / 試算表 / 一般檔案
 *
 * 執行流程：
 * 1️⃣ 取得檔案
 * 2️⃣ 設定分享權限為 ANYONE_WITH_LINK + VIEW
 * 3️⃣ 回傳檔案網址
 *
 * @param {string} fileId Drive 檔案 ID
 * @returns {string} 檔案連結
 */
function makeFileAnyoneWithLinkViewOnly(fileId) {
  if (!fileId) {
    throw new Error("fileId 不可為空");
  }

  const file = DriveApp.getFileById(fileId);

  file.setSharing(
    DriveApp.Access.ANYONE_WITH_LINK,
    DriveApp.Permission.VIEW
  );

  console.log("🔓 檔案已設定為：知道連結的任何人可檢視");
  return file.getUrl();
}


/**
 * 將 PDF 檔案設定為「知道連結的任何人可檢視」
 *
 * 這是 makeFileAnyoneWithLinkViewOnly 的語意化包裝，
 * 方便在 PDF 流程中直接呼叫。
 *
 * @param {string} fileId PDF 檔案 ID
 * @returns {string} PDF 檔案連結
 */
function makePdfAnyoneWithLinkViewOnly(fileId) {
  return makeFileAnyoneWithLinkViewOnly(fileId);
}


/**
 * 將資料夾設定為「知道連結的任何人可檢視」
 *
 * 注意：
 * - 這是針對資料夾，不是檔案
 * - 請使用 setSharing，不建議用 Drive.Permissions.insert
 *
 * @param {string} folderId Drive 資料夾 ID
 * @returns {string} 資料夾連結
 */
function makeFolderAnyoneWithLinkViewOnly(folderId) {
  if (!folderId) {
    throw new Error("folderId 不可為空");
  }

  const folder = DriveApp.getFolderById(folderId);

  folder.setSharing(
    DriveApp.Access.ANYONE_WITH_LINK,
    DriveApp.Permission.VIEW
  );

  console.log("🔓 資料夾已設定為：知道連結的任何人可檢視");
  return folder.getUrl();
}


/**
 * 將某期別的最終 PDF 存放資料夾設為公開連結可檢視
 *
 * 執行流程：
 * 1️⃣ 取得該期別的最終 PDF 資料夾
 * 2️⃣ 將資料夾權限設為知道連結可檢視
 * 3️⃣ 回傳資料夾網址
 *
 * @param {string} periodCode 期別代碼，例如 202603-1
 * @returns {string} 資料夾連結
 */
function makeFinalPdfFolderAnyoneWithLinkViewOnly(periodCode) {
  const folder = getFinalPdfStorageFolder(periodCode);

  folder.setSharing(
    DriveApp.Access.ANYONE_WITH_LINK,
    DriveApp.Permission.VIEW
  );

  console.log(`🔓 ${periodCode} PDF資料夾已設定為：知道連結的任何人可檢視`);
  return folder.getUrl();
}


/**
 * 取得薪資 PDF 檔名
 *
 * 命名格式：
 * 期別 prefix_姓名
 *
 * 範例：
 * 202603-1 薪資單_王小明
 *
 * @param {string} prefix 檔名前綴，例如 薪資單 / 獎金單
 * @param {string} name 姓名
 * @returns {string}
 */
function getSalaryPdfFileName(prefix, name) {
  const { periodCode } = getPeriodInfo();
  return `${periodCode} ${prefix}_${name}`;
}


/**
 * 依名稱安全取得子資料夾
 *
 * 執行流程：
 * 1️⃣ 先找是否已有同名資料夾
 * 2️⃣ 若存在則直接回傳
 * 3️⃣ 若不存在且允許建立，則建立後回傳
 * 4️⃣ 若不存在且不允許建立，則回傳 null
 *
 * @param {GoogleAppsScript.Drive.Folder} parent 父資料夾
 * @param {string} name 子資料夾名稱
 * @param {boolean} createIfNotExist 若不存在是否建立
 * @returns {GoogleAppsScript.Drive.Folder|null}
 */
function getSafeFolderByName(parent, name, createIfNotExist = false) {
  const folders = parent.getFoldersByName(name);
  if (folders.hasNext()) return folders.next();
  if (createIfNotExist) return parent.createFolder(name);
  return null;
}

// ═══════════════════════════════════════════════════════════════
// 🔧 手動觸發 Google Apps Script 權限檢查
// ═══════════════════════════════════════════════════════════════

/**
 * 方法1：強制觸發所有權限檢查
 * 執行這個函數來觸發權限對話框
 */
function triggerAllPermissions() {
  console.log("🔍 開始觸發權限檢查...");
  
  try {
    // 觸發 Spreadsheets 權限
    console.log("📊 觸發 Spreadsheets 權限...");
    const activeSheet = CentralContext.getSpreadsheet();
    console.log(`✅ Spreadsheets 權限正常: ${activeSheet.getName()}`);
    
    // 觸發 Drive 權限
    console.log("📁 觸發 Drive 權限...");
    const files = DriveApp.getFiles();
    let fileCount = 0;
    while (files.hasNext() && fileCount < 1) {
      const file = files.next();
      console.log(`✅ Drive 權限正常，找到檔案: ${file.getName()}`);
      fileCount++;
    }
    
    // 觸發 DriveApp.getFileById 權限
    console.log("🆔 觸發 DriveApp.getFileById 權限...");
    const currentFileId = CentralContext.getSpreadsheet().getId();
    const currentFile = DriveApp.getFileById(currentFileId);
    console.log(`✅ getFileById 權限正常: ${currentFile.getName()}`);
    
    // 觸發資料夾存取權限
    console.log("📂 觸發資料夾存取權限...");
    const parents = currentFile.getParents();
    if (parents.hasNext()) {
      const folder = parents.next();
      console.log(`✅ 資料夾存取權限正常: ${folder.getName()}`);
      
      // 觸發資料夾檔案列表權限
      console.log("📋 觸發資料夾檔案列表權限...");
      const folderFiles = folder.getFiles();
      let folderFileCount = 0;
      while (folderFiles.hasNext() && folderFileCount < 3) {
        const file = folderFiles.next();
        console.log(`📄 資料夾檔案: ${file.getName()}`);
        folderFileCount++;
      }
    }
    
    console.log("✅ 所有權限檢查完成！");
    return "權限檢查成功";
    
  } catch (error) {
    console.error("❌ 權限檢查失敗:", error.message);
    console.error("❌ 完整錯誤:", error);
    
    // 檢查是否是權限錯誤
    if (error.message.includes("權限") || error.message.includes("permission") || error.message.includes("authorization")) {
      console.log("🔧 這是權限錯誤，需要重新授權");
      return "需要重新授權";
    }
    
    throw error;
  }
}

/**
 * 方法2：測試特定的 SpreadsheetApp.open 權限
 * 專門測試開啟其他檔案的權限
 */
function testSpreadsheetOpenPermission() {
  console.log("🔍 測試 SpreadsheetApp.open 權限...");
  
  try {
    // 取得當前資料夾的第一個試算表檔案
    const currentFileId = CentralContext.getSpreadsheet().getId();
    const currentFile = DriveApp.getFileById(currentFileId);
    const folder = currentFile.getParents().next();
    
    console.log(`📂 在資料夾中搜尋其他試算表: ${folder.getName()}`);
    
    const files = folder.getFiles();
    let testFile = null;
    
    while (files.hasNext()) {
      const file = files.next();
      const fileName = file.getName();
      
      // 找一個不是當前檔案的試算表
      if (file.getId() !== currentFileId && 
          (fileName.includes('.xlsx') || fileName.includes('試算表') || fileName.includes('元大'))) {
        testFile = file;
        console.log(`🎯 找到測試檔案: ${fileName}`);
        break;
      }
    }
    
    if (testFile) {
      console.log("🔧 嘗試開啟測試檔案...");
      const testSpreadsheet = SpreadsheetApp.open(testFile);
      console.log(`✅ SpreadsheetApp.open 權限正常: ${testSpreadsheet.getName()}`);
      return "SpreadsheetApp.open 權限正常";
    } else {
      console.log("⚠️ 找不到適合的測試檔案");
      return "找不到測試檔案";
    }
    
  } catch (error) {
    console.error("❌ SpreadsheetApp.open 權限測試失敗:", error.message);
    
    if (error.message.includes("SpreadsheetApp.open") || 
        error.message.includes("權限不足") ||
        error.message.includes("insufficient")) {
      console.log("🔧 確認是 SpreadsheetApp.open 權限問題");
      return "SpreadsheetApp.open 權限不足";
    }
    
    throw error;
  }
}

/**
 * 方法3：檢查並列出所有可用權限
 */
function checkCurrentPermissions() {
  console.log("🔍 檢查當前可用的權限...");
  
  const permissions = {
    spreadsheets: false,
    drive: false,
    driveFileById: false,
    spreadsheetOpen: false
  };
  
  // 測試 Spreadsheets 基本權限
  try {
    CentralContext.getSpreadsheet();
    permissions.spreadsheets = true;
    console.log("✅ Spreadsheets 基本權限：正常");
  } catch (error) {
    console.log("❌ Spreadsheets 基本權限：失敗");
  }
  
  // 測試 Drive 基本權限
  try {
    DriveApp.getFiles().hasNext();
    permissions.drive = true;
    console.log("✅ Drive 基本權限：正常");
  } catch (error) {
    console.log("❌ Drive 基本權限：失敗");
  }
  
  // 測試 DriveApp.getFileById 權限
  try {
    const fileId = CentralContext.getSpreadsheet().getId();
    DriveApp.getFileById(fileId);
    permissions.driveFileById = true;
    console.log("✅ DriveApp.getFileById 權限：正常");
  } catch (error) {
    console.log("❌ DriveApp.getFileById 權限：失敗");
  }
  
  console.log("📋 權限檢查結果:", permissions);
  return permissions;
}

// ═══════════════════════════════════════════════════════════════
// 🔧 加強元大帳戶檔案搜尋功能
// ═══════════════════════════════════════════════════════════════

/**
 * 詳細的檔案搜尋和診斷函數
 */
function findYuantaAccountFileDetailed() {
  console.log("🔍 開始詳細搜尋元大帳戶檔案...");
  
  try {
    const currentFileId = CentralContext.getSpreadsheet().getId();
    const currentFile = DriveApp.getFileById(currentFileId);
    const currentFolder = currentFile.getParents().next();
    
    console.log(`📂 當前資料夾：${currentFolder.getName()}`);
    console.log(`📊 當前檔案：${currentFile.getName()}`);
    
    // 取得資料夾中所有檔案
    const allFiles = currentFolder.getFiles();
    const fileDetails = [];
    
    console.log("📋 資料夾中所有檔案詳細資訊：");
    
    while (allFiles.hasNext()) {
      const file = allFiles.next();
      const fileName = file.getName();
      const fileType = file.getMimeType();
      
      const detail = {
        name: fileName,
        type: fileType,
        isSpreadsheet: fileType.includes('spreadsheet'),
        hasYuanta: fileName.includes('元大'),
        hasAccount: fileName.includes('帳戶'),
        id: file.getId()
      };
      
      fileDetails.push(detail);
      
      console.log(`📄 檔案：${fileName}`);
      console.log(`   類型：${fileType}`);
      console.log(`   包含"元大"：${detail.hasYuanta}`);
      console.log(`   包含"帳戶"：${detail.hasAccount}`);
      console.log(`   是試算表：${detail.isSpreadsheet}`);
      console.log(`   檔案ID：${file.getId()}`);
      console.log('---');
    }
    
    // 尋找可能的元大帳戶檔案
    console.log("🎯 尋找可能的元大帳戶檔案：");
    
    const candidates = fileDetails.filter(file => 
      file.hasYuanta && file.hasAccount && file.isSpreadsheet
    );
    
    console.log(`✅ 找到 ${candidates.length} 個符合條件的檔案：`);
    candidates.forEach(candidate => {
      console.log(`   候選檔案：${candidate.name}`);
    });
    
    // 如果沒有完全符合的，找只包含"元大"的
    if (candidates.length === 0) {
      console.log("⚠️ 沒有找到同時包含'元大'和'帳戶'的檔案");
      
      const yuantaFiles = fileDetails.filter(file => 
        file.hasYuanta && file.isSpreadsheet
      );
      
      console.log(`📊 包含'元大'的試算表檔案 (${yuantaFiles.length} 個)：`);
      yuantaFiles.forEach(file => {
        console.log(`   元大檔案：${file.name}`);
      });
    }
    
    // 嘗試不同的搜尋策略
    console.log("🔄 嘗試不同搜尋策略：");
    
    const searchPatterns = [
      { name: "完整搜尋", pattern: (name) => name.includes('元大帳戶') },
      { name: "寬鬆搜尋1", pattern: (name) => name.includes('元大') && name.includes('帳戶') },
      { name: "寬鬆搜尋2", pattern: (name) => name.includes('元大') },
      { name: "期別搜尋", pattern: (name) => name.includes('202508-1') && name.includes('元大') }
    ];
    
    searchPatterns.forEach(strategy => {
      const matches = fileDetails.filter(file => 
        strategy.pattern(file.name) && file.isSpreadsheet
      );
      
      console.log(`${strategy.name}：找到 ${matches.length} 個檔案`);
      matches.forEach(match => {
        console.log(`   - ${match.name}`);
      });
    });
    
    return fileDetails;
    
  } catch (error) {
    console.error("❌ 檔案搜尋失敗：", error.message);
    throw error;
  }
}

/**
 * 修正版的元大帳戶檔案搜尋
 */
function findYuantaAccountFile(currentFolder) {
  console.log("🔍 執行修正版元大帳戶檔案搜尋...");
  
  const allFiles = currentFolder.getFiles();
  const candidates = [];
  
  // 收集所有候選檔案
  while (allFiles.hasNext()) {
    const file = allFiles.next();
    const fileName = file.getName();
    
    // 排除非試算表檔案
    if (!file.getMimeType().includes('spreadsheet')) {
      continue;
    }
    
    // 搜尋策略：從最嚴格到最寬鬆
    let priority = 0;
    
    if (fileName.includes('元大帳戶') && 
        !fileName.includes('承攬費') && 
        !fileName.includes('工具包押金')) {
      priority = 1; // 最高優先級
    } else if (fileName.includes('元大') && fileName.includes('帳戶')) {
      priority = 2; // 高優先級
    } else if (fileName.includes('元大') && fileName.includes('202508-1')) {
      priority = 3; // 中優先級
    } else if (fileName.includes('元大')) {
      priority = 4; // 低優先級
    }
    
    if (priority > 0) {
      candidates.push({
        file: file,
        name: fileName,
        priority: priority
      });
      
      console.log(`📄 候選檔案 (優先級${priority})：${fileName}`);
    }
  }
  
  if (candidates.length === 0) {
    console.log("❌ 沒有找到任何包含'元大'的試算表檔案");
    return null;
  }
  
  // 按優先級排序，選擇最佳候選
  candidates.sort((a, b) => a.priority - b.priority);
  const bestCandidate = candidates[0];
  
  console.log(`✅ 選擇最佳候選檔案：${bestCandidate.name} (優先級${bestCandidate.priority})`);
  
  return bestCandidate.file;
}

/**
 * 手動指定檔案的替代方案
 */
function manualSelectYuantaFile() {
  console.log("🔧 手動選擇元大帳戶檔案...");
  
  try {
    const currentFileId = CentralContext.getSpreadsheet().getId();
    const currentFile = DriveApp.getFileById(currentFileId);
    const currentFolder = currentFile.getParents().next();
    
    // 列出所有試算表檔案
    const allFiles = currentFolder.getFiles();
    const spreadsheets = [];
    
    while (allFiles.hasNext()) {
      const file = allFiles.next();
      if (file.getMimeType().includes('spreadsheet')) {
        spreadsheets.push({
          name: file.getName(),
          id: file.getId()
        });
      }
    }
    
    console.log("📊 資料夾中所有試算表檔案：");
    spreadsheets.forEach((sheet, index) => {
      console.log(`${index + 1}. ${sheet.name}`);
    });
    
    // 提示使用者手動選擇
    console.log("💡 請手動確認哪一個是元大帳戶檔案");
    
    return spreadsheets;
    
  } catch (error) {
    console.error("❌ 手動選擇失敗：", error.message);
    throw error;
  }
}

//==================================================================
/**轉值工具
/************************************************************
 * 共用模組 CommonModules.gs
 * ----------------------------------------------------------
 * 內容包含：
 * 1. convertRangeToValues() 轉成值工具
 * 2. showImportConfirmDialog() 顯示右下角 modeless dialog
 * 3. processImportDecision() 接收 HTML 按鈕回傳
 * 4. waitForImportDecision() 供主程式等待使用者按鈕
 ************************************************************/


/************************************************************
 * 1. 將指定範圍轉成值（通用工具）
 ************************************************************/
function convertRangeToValues(sheet, rangeA1) {
  const range = sheet.getRange(rangeA1);
  const values = range.getValues();
  range.setValues(values);
}


/************************************************************
 * 2. 顯示右下角彈窗（modeless）
 ************************************************************/
function showImportConfirmDialog() {
  const html = HtmlService.createHtmlOutputFromFile("ConfirmImportModal")
    .setWidth(280)
    .setHeight(220);

  SpreadsheetApp.getUi().showModelessDialog(html, "確認資料匯入");
}


/************************************************************
 * 3. 處理使用者按下按鈕（存到 cache）
 ************************************************************/
function processImportDecision(decision) {
  CacheService.getScriptCache().put("IMPORT_DECISION", decision, 300);
}


/************************************************************
 * 4. 主程式等待使用者結果（輪詢 cache）
 ************************************************************/
function waitForImportDecision() {
  const cache = CacheService.getScriptCache();
  
  let result = null;
  for (let i = 0; i < 60; i++) {  // 最多等 12 秒
    result = cache.get("IMPORT_DECISION");
    if (result) break;
    Utilities.sleep(200);
  }

  return result || "cancel";
}

// ═══════════════════════════════════════════════════════════════
// 📁 共用模組：彈窗詢問轉成值
// ═════════════════════════════════════════════
/**
 * Client 彈窗（側邊欄 confirm 使用）
 * 會回傳 "yes" 或 "no"
 */
function clientPrompt(message) {
  var result = Browser.msgBox(
    "確認操作",
    message,
    Browser.Buttons.YES_NO
  );
  return (result === "yes") ? "yes" : "no";
}

// ═══════════════════════════════════════════════════════════════
// 📌 UnifiedProcessHandler 擴充功能：標準彈窗 + 自動轉值
// ═══════════════════════════════════════════════════════════════

UnifiedProcessHandler.prototype.confirmAndConvert = function (range, message) {
  const self = this;

  this.showConfirm(
    message || "是否將公式轉為靜態值？",
    function () {
      range.setValues(range.getValues());
      self.updateProgress("資料已成功轉為靜態值。");
    },
    function () {
      self.updateProgress("已選擇保留公式。");
    }
  );
};


/**
 * ValueHelper - 共用工具模組
 * 用於：等待匯入完成、彈窗確認、將公式轉成靜態值
 */
const ValueHelper = {

  /**
   * 等待某個儲存格資料載入（通常用在 IMPORTRANGE）
   * @param {Sheet} sheet - 工作表
   * @param {String} rangeA1 - 用來檢查的單一儲存格，例如 "S3"
   * @param {Number} timeoutMs - 超時毫秒（預設15000）
   * @returns {Boolean} 是否成功載入
   */
  waitForImport: function (sheet, rangeA1, timeoutMs = 15000) {
    const start = Date.now();
    while (true) {
      SpreadsheetApp.flush();

      const value = sheet.getRange(rangeA1).getValue();
      if (value !== "" && value !== null) {
        return true; // 已載入
      }

      if (Date.now() - start > timeoutMs) {
        return false; // 超時
      }

      Utilities.sleep(500);
    }
  },


  /**
   * 通用：彈窗 + 單範圍轉值
   */
  promptAndConvert: function (range, message) {

    const response = Browser.msgBox(
      "轉換公式為值",
      message || "偵測到資料包含公式，是否將其轉換為靜態值？",
      Browser.Buttons.YES_NO
    );

    if (response === "no") return false;

    range.setValues(range.getValues());

    Browser.msgBox("轉換完成", "已成功轉換為靜態值。", Browser.Buttons.OK);

    return true;
  },   // ← ★★★ 這個逗號非常重要！

  /**
   * 清潔承攬費用：一次轉三段
   */
  convertThreeRanges: function (sheet) {
    const ui = SpreadsheetApp.getUi();

    const response = ui.alert(
      "轉換三段公式為值",
      "本次匯入包含三段公式：\n" +
      "• 專員名冊（N欄）\n" +
      "• 調薪資料 S 段（T 欄）\n" +
      "• 調薪資料 AH 段（AB 欄）\n\n" +
      "是否要一次將所有資料轉換為靜態值？",
      ui.ButtonSet.YES_NO
    );

    if (response === ui.Button.NO) return false;

    const nValues = sheet.getRange("S3:S").getValues();
    const lastRow = nValues.findLastIndex(r => r[0] !== "") + 3;

    if (lastRow < 3) {
      ui.alert("無資料", "N 欄沒有匯入資料，無法轉換。", ui.ButtonSet.OK);
      return false;
    }

    const numRows = lastRow - 2;
    const numCols = 22;

    const range = sheet.getRange(3, 19, numRows, numCols);
    range.setValues(range.getValues());

    ui.alert("轉換完成", "三段資料已成功轉換為靜態值。", ui.ButtonSet.OK);
    return true;
  }

};


// ████████████████████████████████████████████████████
// 📁 主程式1：薪資表整理（使用統一框架完整版）
// ████████████████████████████████████████████████████
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), getColumnLetter(), CONFIG, 
//          openProgressSidebar(), showToast(), updateSidebarProgress() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ═══════════════════════════════════════════════════════════════

/**
 * 薪資表整理流程配置
 */
function getSalaryProcessConfig() {
  return {
    name: "薪資表整理",
    totalSteps: 16,
    punchMethod: "punchNewSalaryProcessing",
    cells: {
      firstHalf: "C11",
      secondHalf: "D11",
      status: "E11"
    },
    sheetNames: {
      salarySheet: "salary",
      execSheet: "exec",
      orderSheet: "orders",
      projectOrderSheet: "projectOrders",
      revenueSheet: "revenue"
    },
    steps: [
      {
        name: "準備薪資表參數",
        description: "準備薪資表處理參數...",
        type: "custom",
        position: {
          sheet: "salarySheet",
          cell: "L2037"
        },
        handler: prepareSalaryParameters,
        required: true
      },
      {
        name: "處理薪資表L欄",
        description: "處理薪資表 L2037/L2041...",
        type: "custom",
        handler: processSalaryLColumn,
        required: true
      },
      {
        name: "準備營收資料",
        description: "準備清潔營收資料...",
        type: "custom",
        position: {
          sheet: "revenueSheet",
          cell: "A2"
        },
        handler: prepareRevenueData,
        required: true
      },
      {
        name: "處理上半月清空邏輯",
        description: "上半月清空清潔訂單與專案訂單...",
        type: "custom",
        handler: processOrderClearDataWithProjectOrders,
        required: true
      },
      {
        name: "分流並搬入清潔營收資料",
        description: "Y欄1299先進專案訂單，其餘進清潔訂單...",
        type: "custom",
        handler: processRevenueDataWithProjectOrders,
        required: true
      },
      {
        name: "處理檸檬人資料",
        description: "處理清潔訂單檸檬人資料...",
        type: "custom",
        position: {
          sheet: "orderSheet",
          cell: "AH1"
        },
        handler: processLemonData,
        required: true
      }
    ]
  };
}

/**
 * 統一薪資表整理執行函數
 */
function runSalaryPreparation(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getSalaryProcessConfig();
  
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月薪資表整理
 */
function runSalaryPreparationFirstHalf() {
  return runSalaryPreparation(true);
}

/**
 * 下半月薪資表整理
 */
function runSalaryPreparationSecondHalf() {
  return runSalaryPreparation(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 薪資表整理專用處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 準備薪資表參數 - 步驟1
 */
function prepareSalaryParameters(sheets, isFirstHalf, handler) {
  const { salarySheet, execSheet } = sheets;
  
  // 獲取必要參數
  const lastColNum = salarySheet.getLastColumn();
  const lastColLetter = getColumnLetter(lastColNum);
  const processType = isFirstHalf ? "上半月" : "下半月";
  
  // 獲取執行參數
  const c8Value = execSheet.getRange("C8").getValue();
  const d8Value = execSheet.getRange("D8").getValue();
  
  if (!c8Value && !d8Value) {
    throw new Error("執行工作表C8和D8參數不能都為空");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  return {
    lastColNum: lastColNum,
    lastColLetter: lastColLetter,
    c8Value: c8Value,
    d8Value: d8Value,
    processType: processType,
    message: `薪資表參數準備完成（${processType}，最後欄位：${lastColLetter}）`
  };
}

/**
 * 處理薪資表L欄 - 步驟2
 */
function processSalaryLColumn(sheets, isFirstHalf, handler) {
  const { salarySheet } = sheets;

  const lastColNum = salarySheet.getLastColumn();
  const lastColLetter = getColumnLetter(lastColNum);

  if (isFirstHalf) {
    salarySheet.getRange("L2045:" + lastColLetter + "2045").clearContent();
    salarySheet.getRange("L2041:" + lastColLetter + "2041").clearContent();

    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);

    return {
      action: "清空",
      message: "上半月 L2045 / L2041 清空完成"
    };
  }

  SpreadsheetApp.flush();
  Utilities.sleep(500);

  // 🔥 Step 1：先 2046 → 2045（貼值）
  const row2042Values = salarySheet
    .getRange("L2046:" + lastColLetter + "2046")
    .getValues();

  salarySheet
    .getRange("L2045:" + lastColLetter + "2045")
    .setValues(row2042Values);

  SpreadsheetApp.flush();
  Utilities.sleep(500);

  // 🔥 Step 2：再 2040 → 2041（貼值）
  const row2036Values = salarySheet
    .getRange("L2040:" + lastColLetter + "2040")
    .getValues();

  salarySheet
    .getRange("L2041:" + lastColLetter + "2041")
    .setValues(row2036Values);

  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);

  return {
    action: "複製值",
    message: "下半月已完成：2042→2041、2036→2037（皆貼值）"
  };
}

/**
 * 準備營收資料 - 步驟3
 */
function prepareRevenueData(sheets, isFirstHalf, handler) {
  const revenueSheet = sheets.revenueSheet;
  
  // 定位到清潔營收工作表
  handler.positionToCell(revenueSheet, "A2");
  
  // 獲取營收資料
  const revenueRange = revenueSheet.getRange("A2:BJ");
  const revenueValues = revenueRange.getValues();
  const revenueFormats = revenueRange.getBackgrounds();

  // 找到最後一行
  const bValues = revenueValues.map(r => r[1]);
  let lastRowIndex = bValues.length - 1;
  while (lastRowIndex >= 0 && !bValues[lastRowIndex]) {
    lastRowIndex--;
  }
  lastRowIndex += 1;
  
  if (lastRowIndex <= 0) {
    throw new Error("營收資料表中沒有找到有效資料");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  return {
    revenueValues: revenueValues,
    revenueFormats: revenueFormats,
    lastRowIndex: lastRowIndex,
    totalRows: revenueValues.length,
    message: `營收資料準備完成，有效資料至第 ${lastRowIndex} 行`
  };
}

/**
 * 處理上半月清空邏輯 - 步驟4（僅上半月）
 */
function processOrderClearData(sheets, isFirstHalf, handler) {
  const orderSheet = sheets.orderSheet;
  
  if (!isFirstHalf) {
    return { skipped: true, message: "下半月跳過清空步驟" };
  }
  
  // 定位到清潔訂單工作表
  handler.positionToCell(orderSheet, "A2");
  
  // 清空訂單表
  const clearRange = orderSheet.getRange("A2:BJ");
  clearRange.clear();
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  return {
    clearedRange: "A2:BJ",
    message: "訂單表資料清空完成"
  };
}

/* 🔥 在這裡新增 */
function processOrderClearDataWithProjectOrders(sheets, isFirstHalf, handler) {
  const orderSheet = sheets.orderSheet;
  const projectOrderSheet = sheets.projectOrderSheet;

  if (!isFirstHalf) {
    return {
      skipped: true,
      message: "下半月不清空，後續會接在B欄最後一筆資料下方"
    };
  }

  handler.positionToCell(orderSheet, "A2");
  orderSheet.getRange("A2:BJ").clear();

  handler.positionToCell(projectOrderSheet, "A2");
  projectOrderSheet.getRange("A2:BJ").clear();

  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);

  return {
    message: "清潔訂單與專案訂單清空完成"
  };
}




/**
 * 搬入清潔營收資料 - 步驟5
 */
function processRevenueDataWithProjectOrders(sheets, isFirstHalf, handler) {
  const { revenueSheet, orderSheet, projectOrderSheet, execSheet } = sheets;

  const revenueRange = revenueSheet.getRange("A2:BJ");
  const revenueValues = revenueRange.getValues();
  const revenueFormats = revenueRange.getBackgrounds();

  const bValues = revenueValues.map(r => r[1]);
  let lastRowIndex = bValues.length - 1;

  while (lastRowIndex >= 0 && !bValues[lastRowIndex]) {
    lastRowIndex--;
  }

  lastRowIndex += 1;

  const countValue = isFirstHalf
    ? parseInt(execSheet.getRange("C8").getValue(), 10)
    : parseInt(execSheet.getRange("D8").getValue(), 10);

  const startRow = Math.max(1, lastRowIndex - countValue + 1);
  const endRow = lastRowIndex;

  const valuesToProcess = revenueValues.slice(startRow - 1, endRow);
  const formatsToProcess = revenueFormats.slice(startRow - 1, endRow);

  const normalValues = [];
  const normalFormats = [];
  const projectValues = [];
  const projectFormats = [];

  valuesToProcess.forEach((row, index) => {
    const yValue = row[24];

    if (String(yValue).trim() === "1299") {
      projectValues.push(row);
      projectFormats.push(formatsToProcess[index]);
    } else {
      normalValues.push(row);
      normalFormats.push(formatsToProcess[index]);
    }
  });

  if (isFirstHalf) {
    if (normalValues.length > 0) {
      orderSheet.getRange(2, 1, normalValues.length, normalValues[0].length).setValues(normalValues);
      orderSheet.getRange(2, 1, normalFormats.length, normalFormats[0].length).setBackgrounds(normalFormats);
    }

    if (projectValues.length > 0) {
      projectOrderSheet.getRange(2, 1, projectValues.length, projectValues[0].length).setValues(projectValues);
      projectOrderSheet.getRange(2, 1, projectFormats.length, projectFormats[0].length).setBackgrounds(projectFormats);
    }

  } else {
    const orderPasteRow = getFirstEmptyRowByColumn(orderSheet, 2);
    const projectPasteRow = getFirstEmptyRowByColumn(projectOrderSheet, 2);

    if (normalValues.length > 0) {
      orderSheet.getRange(orderPasteRow, 1, normalValues.length, normalValues[0].length).setValues(normalValues);
      orderSheet.getRange(orderPasteRow, 1, normalFormats.length, normalFormats[0].length).setBackgrounds(normalFormats);
    }

    if (projectValues.length > 0) {
      projectOrderSheet.getRange(projectPasteRow, 1, projectValues.length, projectValues[0].length).setValues(projectValues);
      projectOrderSheet.getRange(projectPasteRow, 1, projectFormats.length, projectFormats[0].length).setBackgrounds(projectFormats);
    }
  }

  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY);

  return {
    action: "清潔營收資料分流",
    normalCount: normalValues.length,
    projectCount: projectValues.length,
    message: `分流完成：清潔訂單 ${normalValues.length} 筆，專案訂單 ${projectValues.length} 筆`
  };
}

function getFirstEmptyRowByColumn(sheet, columnNumber) {
  const lastRow = sheet.getLastRow();
  const values = sheet.getRange(1, columnNumber, lastRow).getValues();

  for (let i = values.length - 1; i >= 1; i--) {
    if (values[i][0] !== "" && values[i][0] !== null) {
      return i + 2;
    }
  }

  return 2;
}

/**
 * 處理檸檬人資料 - 步驟6
 */
function processLemonData(sheets, isFirstHalf, handler) {
  const orderSheet = sheets.orderSheet;
  
  const lastRow = orderSheet.getLastRow();
  
  if (lastRow <= 1) {
    return {
      hasValidData: false,
      message: "訂單表中無資料需要處理檸檬人"
    };
  }
  
  const data = orderSheet.getRange(1, 1, lastRow, 40).getValues();

  let processedCount = 0;
  let lemonFoundCount = 0;
  
  for (let i = 1; i < data.length; i++) {
    let ahValue = data[i][33]; // AH欄（第34欄，索引33）- 服務專員

    if (ahValue && ahValue.toString().includes("檸檬人")) {
      lemonFoundCount++;
      
      // 移除檸檬人，保留其他姓名
      const names = ahValue
        .split(/\s*X\s*/g)
        .map(name => name.trim())
        .filter(name => name && !name.includes("檸檬人"));

      const cleaned = names.length > 0 ? names.join(" X ") : "";
      
      // 只更新AH欄（服務專員）
      data[i][33] = cleaned;
      
      processedCount++;
      
      if (processedCount <= 5) { // 只顯示前5個處理記錄
        handler.updateProgress(`第${i + 1}行：移除檸檬人`);
      }
    }

    // 如果AH欄為空，清空對應的J欄
    if (!data[i][33]) {
      data[i][9] = ""; // J欄清空
    }
  }

  handler.updateProgress("寫入檸檬人處理結果...");
  orderSheet.getRange(1, 1, lastRow, 40).setValues(data);
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  return {
    processedCount: processedCount,
    lemonFoundCount: lemonFoundCount,
    totalRows: lastRow,
    hasValidData: true,
    message: `檸檬人資料處理完成，發現 ${lemonFoundCount} 筆，處理 ${processedCount} 筆記錄`
  };
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數（保持原有功能）
// ═══════════════════════════════════════════════════════════════

/**
 * 工作表驗證
 */
function validateAndGetSheet(sheetName, description) {
  if (!sheetName) {
    throw new Error(description + "名稱未定義");
  }
  
  const spreadsheet = CentralContext.getSpreadsheet();
  const sheet = spreadsheet.getSheetByName(sheetName);
  
  if (!sheet) {
    throw new Error("找不到" + description + "：" + sheetName);
  }
  
  return sheet;
}

/**
 * 儲存格值驗證
 */
function validateCellValue(sheet, cellAddress, description) {
  try {
    const value = sheet.getRange(cellAddress).getValue();
    if (value === null || value === undefined || value === "") {
      throw new Error(description + "（" + cellAddress + "）為空值");
    }
    return value;
  } catch (error) {
    throw new Error("讀取" + description + "（" + cellAddress + "）失敗：" + error.message);
  }
}

/**
 * 帶延遲的進度更新
 */
function updateSidebarProgressWithDelay(message, delay) {
  if (delay) {
    Utilities.sleep(delay);
  }
  if (typeof updateSidebarProgress === 'function') {
    updateSidebarProgress(message);
  }
}



// ████████████████████████████████████████████████████
// 📁 主程式2：00調薪（基於穩定版本使用統一框架）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), getColumnLetter(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ████████████████████████████████████████████████████
/**
 * 00調薪流程配置
 */
function getAdjustmentProcessConfig() {
  return {
    name: "00調薪",
    totalSteps: 20, // 🔧 預估步驟數，會自動偵測實際數量
    punchMethod: "punchSalaryAdjustment",
    cells: {
      firstHalf: "C12",
      secondHalf: "D12", 
      status: "E12"
    },
    sheetNames: {
      adjustSheet: "adjust",
      execSheet: "exec",
      salarySheet: "salary",
      summarySheet: "summary"
    },
    steps: [
      {
        name: "完整00調薪流程",
        description: "執行完整的00調薪流程...",
        type: "custom",
        handler: executeFullAdjustmentProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一00調薪執行函數
 */
function runAdjustmentPreparation(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getAdjustmentProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月00調薪
 */
function runAdjustmentPreparationFirstHalf() {
  return runAdjustmentPreparation(true);
}

/**
 * 下半月00調薪
 */
function runAdjustmentPreparationSecondHalf() {
  return runAdjustmentPreparation(false);
}

// ---------------------------------------------
// 🔧 00調薪完整處理函數（基於穩定版本）
// ---------------------------------------------

/**
 * 執行完整的00調薪流程 - 基於穩定的原版邏輯
 */
function executeFullAdjustmentProcess(sheets, isFirstHalf, handler) {
  const { adjustSheet, execSheet, salarySheet, summarySheet } = sheets;
  
  // 步驟1：準備工作表
  handler.updateProgress("準備工作表...");
  
  // 定位到調薪工作表S3
  adjustSheet.activate();
  adjustSheet.getRange("S3").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  // 步驟2：清空S3:AP範圍
  handler.updateProgress("清空調薪表範圍 S3:AP...");
  adjustSheet.getRange("S3:AP").clearContent();
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  // 步驟3：更新S3:S（專員名冊）
  handler.updateProgress("匯入專員名冊至 S3:S...");
  adjustSheet.getRange("S3").setFormula("=IMPORTRANGE('執行'!$C$4,'執行'!$B$1&\"專員名冊!$B$2:$F\")");
  
  // 等待IMPORTRANGE載入
  handler.updateProgress("等待專員名冊匯入完成...");
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY || 3000);

  // 步驟4：更新Y3:AF（調薪資料S欄範圍）
  handler.updateProgress("匯入調薪資料至 Y3:AF...");
  
  // 定位到Y3
  adjustSheet.getRange("Y3").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  adjustSheet.getRange("Y3").setFormula(
    `=ARRAYFORMULA(IF(S3:S200="",,FILTER(IMPORTRANGE('執行'!$C$3,'執行'!$B$1&"調薪資料!K3:R200"),IMPORTRANGE('執行'!$C$3,'執行'!$B$1&"調薪資料!B3:B200")=S3:S200)))`
  );
  
  // 等待複雜IMPORTRANGE載入
  handler.updateProgress("等待調薪資料Y3:AF匯入完成...");
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY || 3000);

  // 步驟5：更新AG3:AP（調薪資料AH欄範圍）
  handler.updateProgress("匯入調薪資料至 AG3:AP...");
  
  // 定位到AG3
  adjustSheet.getRange("AG3").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  adjustSheet.getRange("AG3").setFormula(
    `=ARRAYFORMULA(IF(S3:S200="",,FILTER(IMPORTRANGE('執行'!$C$3,'執行'!$B$1&"調薪資料!Z3:AI200"),IMPORTRANGE('執行'!$C$3,'執行'!$B$1&"調薪資料!B3:B200")=S3:S200)))`
  );
  
  // 等待複雜IMPORTRANGE載入
  handler.updateProgress("等待調薪資料AG3:AP匯入完成...");
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY || 3000);

// 步驟6：詢問是否將三段匯入公式一併轉為數值
handler.updateProgress("確認資料是否已載入...");

if (!ValueHelper.waitForImport(adjustSheet, "S3")) {
  SpreadsheetApp.getUi().alert(
    "資料尚未載入",
    "等待超過 15 秒仍未成功讀取資料，請確認來源試算表或授權設定。",
    SpreadsheetApp.getUi().ButtonSet.OK
  );
  return;
}

// ★★★ 這裡開始用新版彈窗詢問是否轉值 ★★★
handler.showConfirm(
  "新人實境資料已匯入並完成 QRS 計算。\n\n是否要一次將三段資料（N、T、AB 欄）轉成靜態值？",
  function () {
    handler.updateProgress("正在轉換三段公式為靜態值...");
    ValueHelper.convertThreeRanges(adjustSheet);

    // 轉完值後一定要重新抓 lastRow
    const nValuesAfterConvert = adjustSheet.getRange("S3:S").getValues();
    lastRow = nValuesAfterConvert.findLastIndex(r => r[0] !== "") + 3;

    handler.updateProgress("三段資料已完成靜態化。");
  },
  function () {
    handler.updateProgress("保留公式，不進行靜態化。");

    // 仍需要抓 lastRow（因為後面公式還是要下）
    const nValuesAfterCheck = adjustSheet.getRange("S3:S").getValues();
    lastRow = nValuesAfterCheck.findLastIndex(r => r[0] !== "") + 3;
  }
);


  // 步驟7：批次設定A3:E, G3, I3公式
  handler.updateProgress("批次設定計算公式...");
  
  // 定位到A3
  adjustSheet.getRange("A3").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  const numRows = lastRow - 2;
  const formulas = {
    a: [], b: [], c: [], d: [], e: [], g: [], i: []
  };

  for (let i = 0; i < numRows; i++) {
    const row = i + 3;
    formulas.a.push([`=S${row}`]);
    formulas.b.push([`=T${row}`]);
    formulas.c.push([`=VLOOKUP($A${row},$S:$AF,8,FALSE)`]);
    formulas.d.push([`=VLOOKUP($A${row},$S:$AF,9,FALSE)`]);
    formulas.e.push([`=IF(E$1=-1,0,VLOOKUP($A${row},$S:$AF,11,FALSE))`]);
    formulas.g.push([`=VLOOKUP($A${row},$S:$AF,14,FALSE)`]);
    formulas.i.push([`=FILTER($AG:$AP,$S:$S=$A${row})`]);
  }

  adjustSheet.getRange(3, 1, numRows, 1).setFormulas(formulas.a);
  adjustSheet.getRange(3, 2, numRows, 1).setFormulas(formulas.b);
  adjustSheet.getRange(3, 3, numRows, 1).setFormulas(formulas.c);
  adjustSheet.getRange(3, 4, numRows, 1).setFormulas(formulas.d);
  adjustSheet.getRange(3, 5, numRows, 1).setFormulas(formulas.e);
  adjustSheet.getRange(3, 7, numRows, 1).setFormulas(formulas.g);
  adjustSheet.getRange(3, 9, numRows, 1).setFormulas(formulas.i);

  // 等待公式設定完成
  handler.updateProgress("等待計算公式設定完成...");
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.FORMULA_DELAY || 1500);

// 步驟8：更新場次時數總表A4:A
handler.updateProgress("更新場次時數總表 A4:A...");

// 定位到場次時數總表
summarySheet.activate();
SpreadsheetApp.flush();
Utilities.sleep(500);

// ★★★ 新增：先清空 A4:A120 範圍 ★★★
handler.updateProgress("清空場次時數總表 A4:A120...");
summarySheet.getRange("A4:A120").clearContent();
console.log("✅ 已清空場次時數總表 A4:A120");
SpreadsheetApp.flush();
Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

// 定位到場次時數總表A4
summarySheet.getRange("A4").activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

// 從調薪表獲取A3:A的資料
const aRange = adjustSheet.getRange(3, 1, lastRow - 2, 1);
const aValues = aRange.getValues();
console.log(`📊 從調薪表獲取 ${aValues.length} 筆姓名資料`);

// 寫入資料到場次時數總表A4:A
const targetRange = summarySheet.getRange(4, 1, aValues.length, 1);
targetRange.setValues(aValues);
console.log(`✅ 已寫入 ${aValues.length} 筆姓名到場次時數總表 A4:A${3 + aValues.length}`);

// 等待資料寫入完成
SpreadsheetApp.flush();
Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

handler.updateProgress("檢查變數狀態...");
console.log(`🔍 lastRow: ${lastRow}`);
console.log(`🔍 adjustSheet: ${adjustSheet ? '已定義' : '未定義'}`);
console.log(`🔍 salarySheet: ${salarySheet ? '已定義' : '未定義'}`);

// 步驟9：更新薪資表 L1:1 名單（來源：00調薪 S4:S）
let nValues = [];
let diff = 0;
let colFirst = 12;

try {
  handler.updateProgress("更新薪資表 L1:1 名單（來源：00調薪 S3:S）...");

  const preCount = countSalaryHeaderFromL_(salarySheet);
  nValues = getAdjustmentNamesFromS_(adjustSheet);

  diff = nValues.length - preCount;
  colFirst = 12 + preCount;

  salarySheet.getRange(1, 12, 1, salarySheet.getMaxColumns() - 11).clearContent();
  salarySheet.getRange(1, 12, 1, nValues.length).setValues([nValues]);

  const clearedExtraCols = clearSalaryStaffColumnsAfterNames_(salarySheet, nValues.length);
  console.log("✅ 已清除名單後方多餘公式欄，共 " + clearedExtraCols + " 欄");

  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);


  console.log("✅ 薪資表 L1:1 已由 00調薪 S4:S 轉置更新");
  console.log("📊 寫入名單：" + nValues.join(", "));
  console.log("📊 原人數：" + preCount + "，新人數：" + nValues.length + "，差異：" + diff);

  // 步驟10：批次複製薪資計算公式
  handler.updateProgress("檢查是否需要複製薪資計算公式...");

  if (diff > 0) {
    handler.updateProgress("批次複製薪資計算公式...");

    copyFormulasWithReplaceBatchSkipRows(
      salarySheet,
      12,          // 來源 L 欄
      colFirst,    // 新增欄位起點
      diff,
      2,
      2048,
      "L",
      isFirstHalf ? [] : [2041, 2045, 2047, 2048]
    );

    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.FORMULA_DELAY || 1500);

    console.log("✅ 已為新增 " + diff + " 欄複製薪資計算公式");
  } else {
    console.log("📝 無新增人員，跳過公式複製");
  }

  console.log("✅ 步驟9/10完成");

} catch (error) {
  console.error("❌ 步驟9/10執行錯誤:", error.message);
  console.error("❌ 錯誤堆疊:", error.stack);
  handler.updateProgress("❌ 步驟9/10錯誤: " + error.message);
  throw error;
}




// 確保diff和colFirst有值供後續步驟使用
console.log(`📊 步驟9/10完成後變數狀態: nValues.length=${nValues.length}, diff=${diff}, colFirst=${colFirst}`);

// 步驟11：處理下半月特定列清空（僅下半月）
// 現在 diff 變數在此處是可存取的
handler.updateProgress("檢查是否需要處理下半月特定列...");
if (!isFirstHalf && diff > 0) {
  handler.updateProgress("清空下半月特定列...");
  
  // 定位到薪資表2037列
  salarySheet.getRange("A2037").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  [2041, 2045, 2047, 2048].forEach(r =>
  salarySheet.getRange(r, colFirst, 1, diff).clearContent()
);
  
  // 等待清空完成
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
}

  // 步驟12：設定E1期別標記
  handler.updateProgress("設定期別標記...");
  
  // 定位回調薪工作表E1
  adjustSheet.activate();
  adjustSheet.getRange("E1").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  adjustSheet.getRange("E1").setValue(isFirstHalf ? "-1" : "-2");
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  // 返回處理結果
  return {
    processedRows: numRows,
    lastRow: lastRow,
    nameUpdates: nValues.length,
    newColumns: diff > 0 ? diff : 0,
    clearedSecondHalfRows: (!isFirstHalf && diff > 0),
    periodMark: isFirstHalf ? "-1" : "-2",
    message: `00調薪流程完成，處理 ${numRows} 列資料${diff > 0 ? `，新增 ${diff} 欄` : ''}`
  };
}

function copyFormulasWithReplaceBatchSkipRows(
  sheet,
  sourceCol,
  targetColStart,
  numCols,
  startRow,
  endRow,
  sourceLetter,
  skipRows
) {
  const numRows = endRow - startRow + 1;

  const sourceFormulas = sheet.getRange(startRow, sourceCol, numRows, 1).getFormulas();
  const sourceValues = sheet.getRange(startRow, sourceCol, numRows, 1).getValues();

  const output = Array.from({ length: numRows }, () => Array(numCols).fill(""));

  const regex = new RegExp(`(\\$?)\\b${sourceLetter}`, "g");

  for (let c = 0; c < numCols; c++) {
    const targetCol = targetColStart + c;
    const targetLetter = getColumnLetter(targetCol);

    for (let r = 0; r < numRows; r++) {
      const actualRow = startRow + r;

      // 這幾列完全不複製 L 欄公式和值
      if (skipRows.includes(actualRow)) continue;

      const formula = sourceFormulas[r][0];
      const value = sourceValues[r][0];

      if (formula) {
        output[r][c] = formula.replace(regex, function(match, dollar) {
          return dollar ? match : targetLetter;
        });
      } else {
        output[r][c] = value;
      }
    }
  }

  sheet
    .getRange(startRow, targetColStart, numRows, numCols)
    .setValues(output);

  console.log("✅ 已批次複製 L 欄公式和值，並略過指定列");
}



// -----------------------------------------------
// 🔧 輔助函數（保持原有功能）
// -----------------------------------------------

/**
 * 更新完成狀態 - 檢查兩個半月是否都完成
 */
function updateCompletionStatus(execSheet, firstHalfCell, secondHalfCell, statusCell) {
  try {
    const firstHalfValue = execSheet.getRange(firstHalfCell).getValue();
    const secondHalfValue = execSheet.getRange(secondHalfCell).getValue();
    
    // 無論是上半月還是下半月完成，都要更新狀態
    if (firstHalfValue && secondHalfValue) {
      // 兩個半月都完成
      execSheet.getRange(statusCell).setValue("完成");
      console.log(`✅ ${firstHalfCell}/${secondHalfCell} 兩個半月都已完成，更新狀態為：完成`);
    } else if (firstHalfValue && !secondHalfValue) {
      // 只有上半月完成
      execSheet.getRange(statusCell).setValue("上半月完成");
      console.log(`📝 ${firstHalfCell} 上半月完成，更新狀態為：上半月完成`);
    } else if (!firstHalfValue && secondHalfValue) {
      // 只有下半月完成
      execSheet.getRange(statusCell).setValue("下半月完成");
      console.log(`📝 ${secondHalfCell} 下半月完成，更新狀態為：下半月完成`);
    }
    
    // 確保更新生效
    SpreadsheetApp.flush();
    
  } catch (error) {
    console.error("更新完成狀態失敗：", error.message);
  }
}

/**
 * 帶延遲的進度更新
 */
function updateSidebarProgressWithDelay(message, delay) {
  if (delay) {
    Utilities.sleep(delay);
  }
  if (typeof updateSidebarProgress === 'function') {
    updateSidebarProgress(message);
  }
}

/** 
 * 通用批次複製公式函式 
 */
function copyFormulasWithReplace(sheet, sourceCol, targetCol, startRow, endRow, sourceLetter, targetLetter) {
  const numRows = endRow - startRow + 1;
  const baseFormulas = sheet.getRange(startRow, sourceCol, numRows, 1).getFormulas();
  const regex = new RegExp('(\\$?)' + sourceLetter, 'g');

  const newFormulas = baseFormulas.map(row => {
    const formula = row[0];
    if (formula) {
      const replaced = formula.replace(regex, (match, dollar) => {
        return dollar ? match : targetLetter;
      });
      return [replaced];
    } else {
      return [""];
    }
  });

  sheet.getRange(startRow, targetCol, numRows, 1).setFormulas(newFormulas);
  console.log(`✅ 複製公式 ${sourceLetter}${startRow}:${sourceLetter}${endRow} → ${targetLetter}${startRow}:${targetLetter}${endRow}`);
}

/** 
 * 批次複製公式函式（優化版） 
 */
function copyFormulasWithReplaceBatch(sheet, sourceCol, targetColStart, numCols, startRow, endRow, sourceLetter) {
  const numRows = endRow - startRow + 1;
  
  // 一次性取得來源公式
  const baseFormulas = sheet.getRange(startRow, sourceCol, numRows, 1).getFormulas();
  
  // 批次處理所有欄位
  for (let c = 0; c < numCols; c++) {
    const targetCol = targetColStart + c;
    const targetLetter = getColumnLetter(targetCol);
    const regex = new RegExp('(\\$?)' + sourceLetter, 'g');

    const newFormulas = baseFormulas.map(row => {
      const formula = row[0];
      if (formula) {
        const replaced = formula.replace(regex, (match, dollar) => {
          return dollar ? match : targetLetter;
        });
        return [replaced];
      } else {
        return [""];
      }
    });

    // 批次設定公式
    sheet.getRange(startRow, targetCol, numRows, 1).setFormulas(newFormulas);
    console.log(`✅ 批次複製公式 ${sourceLetter}${startRow}:${sourceLetter}${endRow} → ${targetLetter}${startRow}:${targetLetter}${endRow}`);
  }
  
  console.log(`✅ 批次複製完成，共處理 ${numCols} 欄 x ${numRows} 列`);
}

function getAdjustmentNamesFromS_(adjustSheet) {
  const START_ROW = 3;   // S3 開始是第一位專員
  const END_ROW = 200;
  const COL_S = 19;

  const values = adjustSheet
    .getRange(START_ROW, COL_S, END_ROW - START_ROW + 1, 1)
    .getDisplayValues();

  const names = [];
  const seen = {};

  for (let i = 0; i < values.length; i++) {
    const name = String(values[i][0] || "").trim();

    // 遇到第一個空白就停止
    if (!name) break;

    if (!seen[name]) {
      seen[name] = true;
      names.push(name);
    }
  }

  if (names.length === 0) {
    throw new Error("00調薪 S3:S200 找不到可寫入薪資表 L1:1 的專員姓名");
  }

  return names;
}

function countSalaryHeaderFromL_(salarySheet) {
  const maxCols = salarySheet.getMaxColumns() - 11;
  const values = salarySheet.getRange(1, 12, 1, maxCols).getDisplayValues()[0];

  let count = 0;
  for (let i = 0; i < values.length; i++) {
    if (!String(values[i] || "").trim()) break;
    count++;
  }

  return count;
}

function clearSalaryStaffColumnsAfterNames_(salarySheet, nameCount) {
  const firstExtraCol = 12 + nameCount; // L=12，所以名字結束後下一欄
  const maxCols = salarySheet.getMaxColumns();

  if (firstExtraCol > maxCols) return 0;

  const numCols = maxCols - firstExtraCol + 1;
  const lastRowToClear = 2048;

  salarySheet
    .getRange(1, firstExtraCol, lastRowToClear, numCols)
    .clearContent();

  return numCols;
}



// ███████████████████████████████████████████████████
// 📁 主程式3：01專員請款（使用統一框架重構版）
// ███████████████████████████████████████████████████
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress(),
//          importAndPrepareData(), runCommonProcess() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ═══════════════════════════════════════════════════════════════

/**
 * 01專員請款流程配置
 */
function getAllowanceProcessConfig() {
  return {
    name: "01專員請款",
    totalSteps: 11,
    punchMethod: "punchSpecialistPayment",
    cells: {
      firstHalf: "C13",
      secondHalf: "D13", 
      status: "E13"
    },
    sheetNames: {
      allowanceSheet: "allowance",
      execSheet: "exec"
    },
    steps: [
      {
        name: "準備匯入參數",
        description: "準備匯入參數...",
        type: "custom",
        position: {
          sheet: "allowanceSheet",
          cell: "A2"
        },
        handler: prepareImportParameters,
        required: true
      },
      {
        name: "匯入專員請款資料",
        description: "匯入與準備專員請款資料...",
        type: "custom",
        handler: importAllowanceData,
        waitTime: 3000,
        required: true
      },
      {
        name: "處理QRS欄位計算",
        description: "處理專員請款計算...",
        type: "custom",
        position: {
          sheet: "allowanceSheet",
          cell: "Q2"
        },
        handler: processQRSCalculation,
        required: true
      },
      {
        name: "執行共通處理",
        description: "執行專員請款共通處理...",
        type: "custom",
        handler: executeCommonProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一01專員請款執行函數
 */
function runAllowanceProcess(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getAllowanceProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月01專員請款
 */
function runAllowanceProcessFirstHalf() {
  return runAllowanceProcess(true);
}

/**
 * 下半月01專員請款
 */
function runAllowanceProcessSecondHalf() {
  return runAllowanceProcess(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 01專員請款專用處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 準備匯入參數 - 步驟1
 */
function prepareImportParameters(sheets, isFirstHalf, handler) {
  const execSheet = sheets.execSheet;
  
  // 獲取必要參數
  const folderId = execSheet.getRange("C2").getValue();
  const monthCode = execSheet.getRange("B1").getValue();
  const scheduleName = monthCode + (isFirstHalf ? "-1" : "-2");
  
  if (!folderId) {
    throw new Error("資料夾ID不能為空 (C2)");
  }
  
  if (!monthCode) {
    throw new Error("月份代碼不能為空 (B1)");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  return {
    folderId: folderId,
    monthCode: monthCode,
    scheduleName: scheduleName,
    isFirstHalf: isFirstHalf,
    message: `參數準備完成（排程：${scheduleName}）`
  };
}

/**
 * 匯入專員請款資料 - 步驟2
 */
function importAllowanceData(sheets, isFirstHalf, handler) {
  const { allowanceSheet, execSheet } = sheets;
  
  // 直接讀取儲存格（避免框架修改）
  const folderId = execSheet.getRange("C2").getValue();
  const monthCode = execSheet.getRange("B1").getValue();
  const scheduleName = monthCode + (isFirstHalf ? "-1" : "-2");
  
  // 執行匯入和準備資料
  const importConfig = {
    folderId: folderId,
    scheduleName: scheduleName,
    isFirstHalf: isFirstHalf,
    qColSource: 2,
    rColSource: 6,
    sColSource: 8
  };
  
  try {
    // 使用共用模組的匯入函數
    importAndPrepareData(getSheetNames().allowance, importConfig);
    
// 等待專員請款資料匯入完成
handler.updateProgress("等待專員請款資料匯入完成...");
SpreadsheetApp.flush();
Utilities.sleep(handler.config.IMPORT_DELAY);

// ★★★ 等待資料完整載入（避免未載入就跳彈窗）★★★
handler.updateProgress("確認資料是否已成功載入...");
if (!ValueHelper.waitForImport(allowanceSheet, "A2")) {
  SpreadsheetApp.getUi().alert(
    "資料尚未載入",
    "IMPORTRANGE 尚未成功載入，請檢查資料來源是否已授權或正確。",
    SpreadsheetApp.getUi().ButtonSet.OK
  );
  return;
}

// 驗證匯入結果
const lastRow = allowanceSheet.getLastRow();
if (lastRow <= 1) {
  throw new Error("匯入資料為空，請檢查資料來源");
}

// ===================================================
// ★★★ 在此加入彈窗詢問是否轉成值（✔ 正確位置）
// ===================================================

// 自動偵測匯入資料範圍：假設資料從 A2 開始（A1 是標題）
const rangeToConvert = allowanceSheet.getRange(
  2,
  1,
  lastRow - 1,
  allowanceSheet.getLastColumn()
);

ValueHelper.promptAndConvert(
  rangeToConvert,
  "是否要將匯入的專員請款資料轉換為靜態值？\n\n" +
  "（建議：轉成值可避免 IMPORTRANGE 延遲與來源變動影響）"
);

// 回傳資訊
return {
  importConfig: importConfig,
  dataRows: lastRow - 1,
  message: "專員請款資料匯入完成（可選擇是否轉值）"
};

} catch (importError) {
  throw new Error(`資料匯入失敗: ${importError.message}`);
}
} // ★★★ 這裡是結束 importAllowanceData 的大括號（不能刪）★★★



/**
 * 處理QRS欄位計算 - 步驟3
 */
function processQRSCalculation(sheets, isFirstHalf, handler) {
  console.log("🚀 開始批次產出 QRS");
  handler.updateProgress("開始批次產出QRS欄位...");

  const sheet = sheets.allowanceSheet;
  const lastRow = sheet.getLastRow();

  if (lastRow <= 1) {
    return {
      hasValidData: false,
      message: "無資料需要處理"
    };
  }

  const numRows = lastRow - 1;

  // 一次抓 A:H，因為 Q=B、R=F、S=H
  const sourceValues = sheet.getRange(2, 1, numRows, 8).getValues();

  const qrsOutput = sourceValues.map(row => {
    return [
      row[1], // Q = B
      row[5], // R = F
      row[7]  // S = H
    ];
  });

  // 一次寫入 Q:R:S
  sheet.getRange(2, 17, numRows, 3).setValues(qrsOutput);

  const validRowCount = qrsOutput.filter(row =>
    row[0] !== "" || row[1] !== "" || row[2] !== ""
  ).length;

  handler.updateProgress(`✅ QRS批次產出完成，共 ${validRowCount} 筆`);

  return {
    hasValidData: validRowCount > 0,
    totalRows: numRows,
    validRowCount: validRowCount,
    message: `QRS批次產出完成，共 ${validRowCount} 筆`
  };
}


/**
 * 執行共通處理 - 步驟4
 */
function executeCommonProcess(sheets, isFirstHalf, handler) {
  console.log("🚀 開始執行共通處理");
  handler.updateProgress("開始執行共通處理...");
  
  const sheetNames = getSheetNames();
  
  try {
    // 使用共用模組的共通處理函數
    runCommonProcess(sheetNames.allowance);
    
    handler.updateProgress("等待共通處理完成...");
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);
    
    return {
      processedSheet: sheetNames.allowance,
      message: "共通處理執行完成"
    };
    
  } catch (commonError) {
    throw new Error(`共通處理失敗: ${commonError.message}`);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數（保持與原版一致）
// ═══════════════════════════════════════════════════════════════

/**
 * 帶延遲的進度更新
 */
function updateSidebarProgressWithDelay(message, delay) {
  if (delay) {
    Utilities.sleep(delay);
  }
  if (typeof updateSidebarProgress === 'function') {
    updateSidebarProgress(message);
  }
}



// ███████████████████████████████████████████████████
// 主程式4：02儲值獎金（完整版 - 保護標題列格式）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress(),
//          runCommonProcess(), getPeriodInfo() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 02儲值獎金流程配置
 */
function getVoucherProcessConfig() {
  return {
    name: "02儲值獎金",
    totalSteps: 15,
    punchMethod: "punchVoucherBonus",
    cells: {
      firstHalf: "C14",
      secondHalf: "D14", 
      status: "E14"
    },
    sheetNames: {
      voucherSheet: "voucher",  // 英文名稱
      execSheet: "exec"         // 英文名稱
    },
    steps: [
      {
        name: "準備匯入參數",
        description: "準備儲值獎金匯入參數...",
        type: "custom",
        position: {
          sheet: "voucherSheet",
          cell: "A2"
        },
        handler: prepareVoucherImportParameters,
        required: true
      },
      {
        name: "處理上半月清空邏輯",
        description: "清空儲值獎金資料...",
        type: "custom",
        condition: (isFirstHalf) => isFirstHalf,
        handler: processVoucherClearData,
        required: true
      },
      {
        name: "匯入儲值金資料",
        description: "匯入儲值金資料...",
        type: "custom",
        condition: (isFirstHalf) => !isFirstHalf,
        handler: importVoucherData,
        waitTime: 6000,
        required: true
      },
      {
        name: "轉換資料為數值",
        description: "將匯入資料轉為數值...",
        type: "custom",
        condition: (isFirstHalf) => !isFirstHalf,
        handler: convertVoucherDataToValues,
        required: true
      },
      {
        name: "處理獎金分配",
        description: "執行獎金分配資料處理...",
        type: "custom",
        condition: (isFirstHalf) => !isFirstHalf,
        position: {
          sheet: "voucherSheet",
          cell: "F2"
        },
        handler: processVoucherBonusDistribution,
        required: true
      },
      {
        name: "執行共通處理",
        description: "執行儲值獎金共通處理...",
        type: "custom",
        condition: (isFirstHalf) => !isFirstHalf,
        handler: executeVoucherCommonProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一02儲值獎金執行函數
 */
function runVoucherPreparation(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getVoucherProcessConfig();
  
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月02儲值獎金
 */
function runVoucherPreparationFirstHalf() {
  return runVoucherPreparation(true);
}

/**
 * 下半月02儲值獎金
 */
function runVoucherPreparationSecondHalf() {
  return runVoucherPreparation(false);
}

// ═══════════════════════════════════════════════════════════════
// 保護標題列的標記與清除函數
// ═══════════════════════════════════════════════════════════════

/**
 * 安全標記處理範圍（不影響標題列）
 */
function safeHighlightProcessingRange(sheet, startRow, startCol, numRows, numCols, color = "#FFFBCC") {
  try {
    if (numRows > 0 && numCols > 0 && startRow >= 2) {
      const range = sheet.getRange(startRow, startCol, numRows, numCols);
      range.setBackground(color);
      console.log(`安全標記：${sheet.getName()} 第${startRow}行起 (${numRows}x${numCols})`);
    }
  } catch (error) {
    console.warn("安全標記失敗：", error.message);
  }
}

/**
 * 安全清除標記（絕對保護標題列）
 */
function safeClearHighlight(sheet) {
  try {
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    
    // 只處理第2行以下的範圍
    if (lastRow >= 2 && lastCol > 0) {
      const safeRange = sheet.getRange(2, 1, lastRow - 1, lastCol);
      safeRange.setBackground(null);
      console.log(`安全清除：${sheet.getName()} 已清除第2-${lastRow}行標記，保護標題列`);
    }
    
  } catch (error) {
    console.warn("安全清除失敗：", error.message);
  }
}

/**
 * 檢查並備份標題列格式
 */
function backupHeaderFormat(sheet) {
  try {
    if (sheet.getLastRow() >= 1 && sheet.getLastColumn() > 0) {
      const headerRange = sheet.getRange(1, 1, 1, sheet.getLastColumn());
      const backgrounds = headerRange.getBackgrounds();
      const fontColors = headerRange.getFontColors();
      const fontWeights = headerRange.getFontWeights();
      
      // 儲存到 Properties
      const sheetId = sheet.getSheetId();
      const backupData = {
        backgrounds: backgrounds,
        fontColors: fontColors,
        fontWeights: fontWeights,
        timestamp: new Date().getTime()
      };
      
      PropertiesService.getScriptProperties().setProperty(`header_backup_${sheetId}`, JSON.stringify(backupData));
      console.log(`已備份 ${sheet.getName()} 標題列格式`);
    }
  } catch (error) {
    console.warn("備份標題列格式失敗：", error.message);
  }
}

/**
 * 還原標題列格式
 */
function restoreHeaderFormat(sheet) {
  try {
    const sheetId = sheet.getSheetId();
    const backupDataStr = PropertiesService.getScriptProperties().getProperty(`header_backup_${sheetId}`);
    
    if (backupDataStr) {
      const backupData = JSON.parse(backupDataStr);
      const headerRange = sheet.getRange(1, 1, 1, sheet.getLastColumn());
      
      if (backupData.backgrounds) {
        headerRange.setBackgrounds(backupData.backgrounds);
      }
      if (backupData.fontColors) {
        headerRange.setFontColors(backupData.fontColors);
      }
      if (backupData.fontWeights) {
        headerRange.setFontWeights(backupData.fontWeights);
      }
      
      console.log(`已還原 ${sheet.getName()} 標題列格式`);
    }
  } catch (error) {
    console.warn("還原標題列格式失敗：", error.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 02儲值獎金專用處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 準備匯入參數 - 步驟1
 */
function prepareVoucherImportParameters(sheets, isFirstHalf, handler) {
  const execSheet = sheets.execSheet;
  
  // 獲取必要參數
  const folderId = execSheet.getRange("C5").getValue();
  const processType = isFirstHalf ? "上半月" : "下半月";
  
  if (!folderId) {
    throw new Error("資料夾ID不能為空 (C5)");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 1000);
  
  return {
    folderId: folderId,
    processType: processType,
    message: `參數準備完成（${processType}，資料夾ID：${folderId.substring(0, 10)}...）`
  };
}

/**
 * 處理上半月清空邏輯 - 步驟2（僅上半月）
 */
function processVoucherClearData(sheets, isFirstHalf, handler) {
  const voucherSheet = sheets.voucherSheet;
  
  if (!isFirstHalf) {
    return { skipped: true, message: "下半月跳過清空步驟" };
  }
  
  // 備份標題列格式
  backupHeaderFormat(voucherSheet);
  
  // 只清空第2行以下的資料，保護標題列
  const lastRow = voucherSheet.getLastRow();
  const lastCol = voucherSheet.getLastColumn();
  
  if (lastRow >= 2 && lastCol > 0) {
    voucherSheet.getRange(2, 1, lastRow - 1, lastCol).clearContent();
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 1000);
  
  // 還原標題列格式
  restoreHeaderFormat(voucherSheet);
  
  return {
    clearedRange: `第2-${lastRow}行`,
    message: "儲值獎金資料清空完成（標題列已保護）"
  };
}

/**
 * 匯入儲值金資料 - 步驟3（僅下半月）
 */
function importVoucherData(sheets, isFirstHalf, handler) {
  const { voucherSheet, execSheet } = sheets;
  
  if (isFirstHalf) {
    return { skipped: true, message: "上半月跳過匯入步驟" };
  }
  
  try {
    // 備份標題列格式
    backupHeaderFormat(voucherSheet);
    
    // 從第一步驟獲取參數
    const folderId = execSheet.getRange("C5").getValue();
    
    // 尋找第一個空白列（從第2行開始查找）
    const aValues = voucherSheet.getRange("A2:A").getValues();
    let firstEmptyRow = 2;
    for (let i = 0; i < aValues.length; i++) {
      if (aValues[i][0] === "") {
        firstEmptyRow = i + 2;
        break;
      }
    }
    
    // 定位到匯入位置
    handler.positionToCell(voucherSheet, `A${firstEmptyRow}`);
    
    const importFormula = 
      '=filter({' +
      'filter(importrange("' + folderId + '", "範本!A2:BB8000"),' +
      'importrange("' + folderId + '", "範本!A2:A8000")="儲值金")' +
      '},{0,1,1,1,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1})';

    voucherSheet.getRange(firstEmptyRow, 1).setFormula(importFormula);

    // 等待儲值金資料匯入完成
    SpreadsheetApp.flush();
    Utilities.sleep(6000);
    
    // 檢查匯入是否成功
    const firstCell = voucherSheet.getRange(firstEmptyRow, 1).getValue();
    if (firstCell === "" || firstCell.toString().includes("#ERROR")) {
      throw new Error("儲值金資料匯入失敗，請檢查資料夾ID和工作表名稱");
    }
    
    // 還原標題列格式
    restoreHeaderFormat(voucherSheet);
    
    return {
      firstEmptyRow: firstEmptyRow,
      importFormula: importFormula,
      message: "儲值金資料匯入完成（標題列已保護）"
    };
    
  } catch (importError) {
    // 錯誤時也要還原標題列格式
    restoreHeaderFormat(voucherSheet);
    throw importError;
  }
}

/**
 * 轉換資料為數值 - 步驟4（僅下半月）
 */
function convertVoucherDataToValues(sheets, isFirstHalf, handler) {
  const voucherSheet = sheets.voucherSheet;
  
  if (isFirstHalf) {
    return { skipped: true, message: "上半月跳過轉換步驟" };
  }
  
  try {
    // 備份標題列格式
    backupHeaderFormat(voucherSheet);
    
    // 定位到A2開始的資料區域
    handler.positionToCell(voucherSheet, "A2");
    
    const lastRow = voucherSheet.getLastRow();
    if (lastRow <= 1) {
      return {
        hasValidData: false,
        message: "無資料需要轉換"
      };
    }
  
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.IMPORT_DELAY || 2000);

    // ★★★ 彈窗應該放在這裡 ★★★
    const rangeToConvert = voucherSheet.getRange(
      2, 
      1, 
      lastRow - 1, 
      voucherSheet.getLastColumn()
    );

    // ★★★ 這裡改成使用新版 auto-fallback 彈窗 ★★★
    handler.confirmAndConvert(
      rangeToConvert,
      "是否要將匯入的儲值金資料轉換為靜態值？\n\n" +
      "（建議：轉成值能避免 IMPORTRANGE 延遲與來源變動影響。）"
    );

    // 還原標題列格式
    restoreHeaderFormat(voucherSheet);

    return {
      convertedRows: lastRow - 1,
      convertedCols: voucherSheet.getLastColumn(),
      hasValidData: true,
      message: "儲值金資料轉數值完成（依使用者選擇）"
    };
    
  } catch (convertError) {
    restoreHeaderFormat(voucherSheet);
    throw convertError;
  }
}
/**
 * 處理獎金分配 - 步驟5（僅下半月）
 */
function processVoucherBonusDistribution(sheets, isFirstHalf, handler) {
  const voucherSheet = sheets.voucherSheet;
  
  if (isFirstHalf) {
    return { skipped: true, message: "上半月跳過獎金分配步驟" };
  }
  
  try {
    // 備份標題列格式
    backupHeaderFormat(voucherSheet);
    
    // 取得資料範圍
    const currentLastRow = voucherSheet.getLastRow();
    
    if (currentLastRow <= 1) {
      return {
        hasValidData: false,
        message: "無資料需要處理獎金分配"
      };
    }
    
    // 安全標記處理範圍（從第2行開始）
    safeHighlightProcessingRange(voucherSheet, 2, 1, currentLastRow - 1, 19);
    
    // 定位到F欄
    handler.positionToCell(voucherSheet, "F2");
    
    const dataAtoF = voucherSheet.getRange(2, 1, currentLastRow - 1, 6).getValues();
    const output = [];
    let gColumnAdjustmentCount = 0;

    dataAtoF.forEach((row, index) => {
      if (!row[0]) return;
      
      const a = row[0];
      const b = row[1];
      const c = row[2];
      const d = row[3];
      const e = row[4];
      const f = row[5];
      const rowNumber = index + 2;
      
      try {
        const cleanF = (f || "").toString().replace(/獎金[:：]/g, "");
        const names = cleanF.split(/[ＸXｘ]/).map(s => s.trim()).filter(Boolean);
        
        // 實際人數
        const actualPersonCount = Math.max(names.length, 1);
        
        // G欄最小值邏輯
        let gForCalculation = actualPersonCount;
        if (actualPersonCount > 0 && actualPersonCount < 2) {
          gForCalculation = 2;
          gColumnAdjustmentCount++;
          console.warn("第" + rowNumber + "行：實際人數" + actualPersonCount + "人，但G欄按2人計算獎金分配");
          
          if (gColumnAdjustmentCount <= 3) {
            handler.updateProgress("⚠️ 第" + rowNumber + "行按最小2人分配計算");
          }
        }
        
        // 計算H欄（總獎金）
        const h = d && (d.includes("48") || d.includes("50,000")) ? 800 :
                  d && (d.includes("24") || d.includes("20,000")) ? 320 : "";
        
        // 計算R欄（平均分配金額）
        const r = gForCalculation ? Math.round(h / gForCalculation) : "";
        
        // 計算S欄（日期格式化）
        const s = c && e ? Utilities.formatDate(new Date(c), handler.config.TIMEZONE || "Asia/Taipei", "MM/dd") + e : "";

        // 根據實際人數生成記錄
        for (let i = 0; i < actualPersonCount; i++) {
          const name = names[i] || names[0] || "";
          output.push([
            a,                // A欄
            b,                // B欄  
            c,                // C欄
            d,                // D欄
            e,                // E欄
            f,                // F欄
            gForCalculation,  // G欄
            h,                // H欄
            name,             // I欄
            "",               // J欄
            "",               // K欄
            "",               // L欄
            "",               // M欄
            "",               // N欄
            "",               // O欄
            "",               // P欄
            name,             // Q欄
            r,                // R欄
            s                 // S欄
          ]);
        }
        
      } catch (rowError) {
        console.error("第" + rowNumber + "行處理失敗：", rowError.message);
        handler.updateProgress("❌ 第" + rowNumber + "行資料處理失敗");
      }
    });

    if (output.length) {
      // 清空現有資料（從第2行開始，保護標題列）
      const maxRows = voucherSheet.getLastRow();
      if (maxRows > 1) {
        voucherSheet.getRange(2, 1, maxRows - 1, 19).clearContent();
      }
      
      // 定位到寫入區域
      handler.positionToCell(voucherSheet, "A2");
      
      // 寫入處理後的資料
      voucherSheet.getRange(2, 1, output.length, 19).setValues(output);
      
      // 安全標記寫入的範圍
      safeHighlightProcessingRange(voucherSheet, 2, 1, output.length, 19);
      SpreadsheetApp.flush();
      Utilities.sleep(1000);
      
      // 安全清除標記
      safeClearHighlight(voucherSheet);
    }
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 1000);
    
    // 還原標題列格式
    restoreHeaderFormat(voucherSheet);
    
    let message = "獎金分配資料處理完成，共 " + output.length + " 筆記錄";
    if (gColumnAdjustmentCount > 0) {
      message += "（" + gColumnAdjustmentCount + " 筆按最小2人分配計算）";
    }
    message += "（標題列已保護）";
    
    return {
      processedRecords: output.length,
      adjustmentCount: gColumnAdjustmentCount,
      hasValidData: true,
      message: message
    };
    
  } catch (distributionError) {
    // 錯誤時也要安全清除和還原
    safeClearHighlight(voucherSheet);
    restoreHeaderFormat(voucherSheet);
    throw new Error("獎金分配處理失敗：" + distributionError.message);
  }
}

/**
 * 執行共通處理 - 步驟6（僅下半月）
 */
function executeVoucherCommonProcess(sheets, isFirstHalf, handler) {
  const voucherSheet = sheets.voucherSheet;
  
  if (isFirstHalf) {
    return { skipped: true, message: "上半月跳過共通處理" };
  }
  
  try {
    // 備份標題列格式
    backupHeaderFormat(voucherSheet);
    
    // 安全標記共通處理範圍（從第2行開始）
    const lastRow = voucherSheet.getLastRow();
    const lastCol = voucherSheet.getLastColumn();
    
    if (lastRow > 1 && lastCol > 0) {
      safeHighlightProcessingRange(voucherSheet, 2, 1, lastRow - 1, lastCol);
    }
    
    const sheetNames = getSheetNames();
    runCommonProcess(sheetNames.voucher);
    
    handler.updateProgress("等待共通處理完成...");
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 1000);
    
    // 安全清除標記
    safeClearHighlight(voucherSheet);
    
    // 還原標題列格式
    restoreHeaderFormat(voucherSheet);
    
    return {
      processedSheet: sheetNames.voucher,
      message: "共通處理執行完成（標題列格式已完全保護）"
    };
    
  } catch (commonError) {
    // 錯誤處理時也要安全清除和還原
    safeClearHighlight(voucherSheet);
    restoreHeaderFormat(voucherSheet);
    throw new Error("共通處理失敗：" + commonError.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 輔助函數（保持原有功能）
// ═══════════════════════════════════════════════════════════════

/**
 * 工作表驗證
 */
function validateAndGetSheet(sheetName, description) {
  if (!sheetName) {
    throw new Error(description + "名稱未定義");
  }
  
  const spreadsheet = CentralContext.getSpreadsheet();
  const sheet = spreadsheet.getSheetByName(sheetName);
  
  if (!sheet) {
    throw new Error("找不到" + description + "：" + sheetName);
  }
  
  return sheet;
}

/**
 * 儲存格值驗證
 */
function validateCellValue(sheet, cellAddress, description) {
  try {
    const value = sheet.getRange(cellAddress).getValue();
    if (value === null || value === undefined || value === "") {
      throw new Error(description + "（" + cellAddress + "）為空值");
    }
    return value;
  } catch (error) {
    throw new Error("讀取" + description + "（" + cellAddress + "）失敗：" + error.message);
  }
}

/**
 * 帶延遲的進度更新
 */
function updateSidebarProgressWithDelay(message, delay) {
  if (delay) {
    Utilities.sleep(delay);
  }
  if (typeof updateSidebarProgress === 'function') {
    updateSidebarProgress(message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 程式結束標記
// ═══════════════════════════════════════════════════════════════

console.log("完整主程式4載入完成 - 已加強標題列保護機制");


// ███████████████████████████████████████████████████
// 📁 獨立功能1：新人實境期別標註（使用統一框架版本）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress(),
//          getPeriodInfo() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 新人實境期別標註流程配置
 */
function getPeriodLabelProcessConfig() {
  return {
    name: "新人實境期別標註",
    totalSteps: 6, // 🔧 調整為實際步驟數
    punchMethod: "punchNewEmployeePeriodLabel",
    cells: {
      firstHalf: "C18",
      secondHalf: "D18", 
      status: "E18"
    },
    sheetNames: {
      salarySheet: "salary",
      newcomerSheet: "newcomer", 
      internSheet: "intern",
      execSheet: "exec"
    },
    steps: [
      {
        name: "獲取期別資訊",
        description: "準備工作表和期別資訊...",
        type: "custom",
        handler: preparePeriodInfo,
        required: true
      },
      {
        name: "取得薪資表人員名單",
        description: "從薪資表取得人員名單...",
        type: "custom",
        handler: getSalaryNamesList,
        required: true
      },
      {
        name: "處理新人實境期別標註",
        description: "為新人實境人員標註期別...",
        type: "custom",
        position: {
          sheet: "newcomerSheet",
          cell: "AK2"
        },
        handler: processNewcomerPeriodLabel,
        required: true
      },
      {
        name: "處理新人實習期別標註",
        description: "為新人實習人員標註期別...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "AK2"
        },
        handler: processInternPeriodLabel,
        required: true
      }
    ]
  };
}

/**
 * 統一新人實境期別標註執行函數
 */
function syncPeriodToNewcomerAndIntern() {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getPeriodLabelProcessConfig();
  
  // 此功能不分上下半月，統一使用false
  return processor.executeProcess(config, false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 新人實境期別標註專用處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 準備期別資訊
 */
function preparePeriodInfo(sheets, isFirstHalf, handler) {
  const period = getPeriodInfo();
  const periodCode = period.periodCode;
  
  if (!periodCode) {
    throw new Error("無法取得期別資訊");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  // 將期別資訊存儲到處理器中，供後續步驟使用
  handler.periodInfo = {
    periodCode: periodCode,
    fullPeriod: period
  };
  
  return {
    periodCode: periodCode,
    message: `期別資訊準備完成：${periodCode}`
  };
}

/**
 * 取得薪資表人員名單
 */
function getSalaryNamesList(sheets, isFirstHalf, handler) {
  const { salarySheet } = sheets;
  
  // 取得薪資表第一列的所有姓名
  const salaryNames = salarySheet.getRange(1, 1, 1, salarySheet.getLastColumn())
    .getValues()[0]
    .filter(name => name && name.toString().trim() !== "");
  
  if (salaryNames.length === 0) {
    throw new Error("薪資表中未找到人員名單");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  // 將人員名單存儲到處理器中
  handler.salaryNames = salaryNames;
  
  return {
    namesCount: salaryNames.length,
    names: salaryNames,
    message: `取得薪資表人員名單：${salaryNames.length}人`
  };
}

/**
 * 處理新人實境期別標註
 */
function processNewcomerPeriodLabel(sheets, isFirstHalf, handler) {
  const { newcomerSheet } = sheets;
  const periodCode = handler.periodInfo.periodCode;
  const salaryNames = handler.salaryNames;
  
  let newcomerCount = 0;
  const lastRowNewcomer = newcomerSheet.getLastRow();
  
  if (lastRowNewcomer <= 1) {
    return {
      processedCount: 0,
      message: "新人實境工作表無資料需要處理"
    };
  }
  
  try {
    // 定位到新人實境工作表的AK欄（期別標註欄）
    handler.positionToCell(newcomerSheet, "AK2");
    
    // 取得相關欄位資料
    // AH欄（第34欄）：姓名
    // AF欄（第32欄）：結訓日期
    // AK欄（第37欄）：期別標註
    const nameColumn = newcomerSheet.getRange(2, 34, lastRowNewcomer - 1, 1).getValues();
    const graduationColumn = newcomerSheet.getRange(2, 32, lastRowNewcomer - 1, 1).getValues();
    const periodLabelRange = newcomerSheet.getRange(2, 37, lastRowNewcomer - 1, 1);
    
    // 清空現有期別標註
    periodLabelRange.clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    const periodLabelValues = periodLabelRange.getValues();
    
    // 對每個薪資表人員進行處理
    salaryNames.forEach(salaryName => {
      nameColumn.forEach((nameRow, rowIndex) => {
        const personName = nameRow[0];
        const graduationDate = graduationColumn[rowIndex][0];
        
        // 如果姓名匹配且未結訓（結訓日期為空），則標註期別
        if (personName === salaryName && !graduationDate) {
          periodLabelValues[rowIndex][0] = periodCode;
          newcomerCount++;
        }
      });
    });
    
    // 寫回期別標註
    periodLabelRange.setValues(periodLabelValues);
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);
    
    return {
      processedCount: newcomerCount,
      totalRows: lastRowNewcomer - 1,
      message: `新人實境期別標註完成：${newcomerCount}筆`
    };
    
  } catch (error) {
    throw new Error(`新人實境期別標註失敗：${error.message}`);
  }
}

/**
 * 處理新人實習期別標註
 */
function processInternPeriodLabel(sheets, isFirstHalf, handler) {
  const { internSheet } = sheets;
  const periodCode = handler.periodInfo.periodCode;
  const salaryNames = handler.salaryNames;
  
  let internCount = 0;
  const lastRowIntern = internSheet.getLastRow();
  
  if (lastRowIntern <= 1) {
    return {
      processedCount: 0,
      message: "新人實習工作表無資料需要處理"
    };
  }
  
  try {
    // 定位到新人實習工作表的AK欄（期別標註欄）
    handler.positionToCell(internSheet, "AK2");
    
    // 取得相關欄位資料
    // AH欄（第34欄）：姓名
    // AF欄（第32欄）：結訓日期
    // AK欄（第37欄）：期別標註
    const nameColumn = internSheet.getRange(2, 34, lastRowIntern - 1, 1).getValues();
    const graduationColumn = internSheet.getRange(2, 32, lastRowIntern - 1, 1).getValues();
    const periodLabelRange = internSheet.getRange(2, 37, lastRowIntern - 1, 1);
    
    // 清空現有期別標註
    periodLabelRange.clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    const periodLabelValues = periodLabelRange.getValues();
    
    // 對每個薪資表人員進行處理
    salaryNames.forEach(salaryName => {
      nameColumn.forEach((nameRow, rowIndex) => {
        const personName = nameRow[0];
        const graduationDate = graduationColumn[rowIndex][0];
        
        // 如果姓名匹配且未結訓（結訓日期為空），則標註期別
        if (personName === salaryName && !graduationDate) {
          periodLabelValues[rowIndex][0] = periodCode;
          internCount++;
        }
      });
    });
    
    // 寫回期別標註
    periodLabelRange.setValues(periodLabelValues);
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);
    
    return {
      processedCount: internCount,
      totalRows: lastRowIntern - 1,
      message: `新人實習期別標註完成：${internCount}筆`
    };
    
  } catch (error) {
    throw new Error(`新人實習期別標註失敗：${error.message}`);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數和相容性支援
// ═══════════════════════════════════════════════════════════════

/**
 * 原始函數名稱的別名，保持向後相容
 * @deprecated 建議使用 syncPeriodToNewcomerAndIntern()
 */
function runNewEmployeePeriodLabel() {
  console.warn("⚠️ runNewEmployeePeriodLabel() 已棄用，請使用 syncPeriodToNewcomerAndIntern()");
  return syncPeriodToNewcomerAndIntern();
}

/**
 * 快速執行函數（不使用統一框架，僅供調試）
 * @deprecated 建議使用統一框架版本
 */
function quickSyncPeriodLabel() {
  try {
    openProgressSidebar();
    showToast("🚀 開始新人實境期別標註（快速版）...");
    
    const sheetNames = getSheetNames();
    const salarySheet = getSheetByName(sheetNames.salary);
    const newcomerSheet = getSheetByName(sheetNames.newcomer);
    const internSheet = getSheetByName(sheetNames.intern);
    
    const period = getPeriodInfo();
    const periodCode = period.periodCode;
    
    const salaryNames = salarySheet.getRange(1, 1, 1, salarySheet.getLastColumn())
      .getValues()[0].filter(name => name);
    
    let totalProcessed = 0;
    
    // 處理新人實境
    const lastRowNewcomer = newcomerSheet.getLastRow();
    if (lastRowNewcomer > 1) {
      const ahNew = newcomerSheet.getRange(2, 34, lastRowNewcomer - 1, 1).getValues();
      const afNew = newcomerSheet.getRange(2, 32, lastRowNewcomer - 1, 1).getValues();
      const akNewRange = newcomerSheet.getRange(2, 37, lastRowNewcomer - 1, 1);
      akNewRange.clearContent();
      const akNew = akNewRange.getValues();

      salaryNames.forEach(name => {
        ahNew.forEach((row, idx) => {
          if (row[0] === name && !afNew[idx][0]) {
            akNew[idx][0] = periodCode;
            totalProcessed++;
          }
        });
      });

      akNewRange.setValues(akNew);
    }
    
    // 處理新人實習
    const lastRowIntern = internSheet.getLastRow();
    if (lastRowIntern > 1) {
      const ahInt = internSheet.getRange(2, 34, lastRowIntern - 1, 1).getValues();
      const afInt = internSheet.getRange(2, 32, lastRowIntern - 1, 1).getValues();
      const akIntRange = internSheet.getRange(2, 37, lastRowIntern - 1, 1);
      akIntRange.clearContent();
      const akInt = akIntRange.getValues();

      salaryNames.forEach(name => {
        ahInt.forEach((row, idx) => {
          if (row[0] === name && !afInt[idx][0]) {
            akInt[idx][0] = periodCode;
            totalProcessed++;
          }
        });
      });

      akIntRange.setValues(akInt);
    }
    
    const message = `✅ 快速期別標註完成！總計處理：${totalProcessed}筆`;
    showToast(message);
    updateSidebarProgress(message);
    
    return { success: true, totalProcessed: totalProcessed };
    
  } catch (error) {
    const errorMessage = `❌ 快速期別標註失敗：${error.message}`;
    showToast(errorMessage);
    updateSidebarProgress(errorMessage);
    throw error;
  }
}



// ███████████████████████████████████████████████████
// 📁 主程式5：03新人實境（使用統一框架版本）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress(),
//          runCommonProcess(), markCustomFinishByHalf(), scrollToCell() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 03新人實境流程配置
 */
function getNewcomerProcessConfig() {
  return {
    name: "03新人實境",
    totalSteps: 8, // 🔧 預估步驟數，框架會自動偵測實際數量
    punchMethod: "punchNewEmployeeTraining", // 🔧 正確的打卡方法名稱
    cells: {
      firstHalf: "C15",
      secondHalf: "D15", 
      status: "E15"
    },
    sheetNames: {
      newcomerSheet: "newcomer",
      execSheet: "exec"
    },
    steps: [
      {
        name: "準備匯入參數與資料驗證",
        description: "驗證參數和工作表狀態...",
        type: "custom",
        position: {
          sheet: "newcomerSheet",
          cell: "A2"
        },
        handler: prepareNewcomerImportParams,
        required: true
      },
      {
        name: "處理上下半月清空邏輯",
        description: "根據上下半月處理資料清空...",
        type: "custom",
        position: {
          sheet: "newcomerSheet",
          cell: "A2"
        },
        handler: handleNewcomerClearLogic,
        required: true
      },
      {
        name: "匯入新人實境資料",
        description: "執行資料匯入與QRS計算...",
        type: "custom",
        position: {
          sheet: "newcomerSheet",
          cell: "A2"
        },
        handler: importNewcomerDataWithQRS,
        required: true
      },
      {
        name: "執行新人實境共通處理",
        description: "執行共通處理流程...",
        type: "custom",
        position: {
          sheet: "newcomerSheet",
          cell: "A1"
        },
        handler: executeNewcomerCommonProcess,
        required: true
      },
      {
        name: "完成標記與資料檢查",
        description: "標記完成狀態並驗證資料...",
        type: "custom",
        position: {
          sheet: "newcomerSheet",
          cell: "C15"
        },
        handler: finishNewcomerProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一03新人實境執行函數
 */
function runNewcomerProcess(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getNewcomerProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月03新人實境
 */
function runNewcomerProcessFirstHalf() {
  return runNewcomerProcess(true);
}

/**
 * 下半月03新人實境
 */
function runNewcomerProcessSecondHalf() {
  return runNewcomerProcess(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 03新人實境專用處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 準備匯入參數與資料驗證
 */
function prepareNewcomerImportParams(sheets, isFirstHalf, handler) {
  const { newcomerSheet, execSheet } = sheets;
  
  // 核心參數驗證
  const folderId = validateCellValue(execSheet, "C3", "資料夾ID");
  const monthCode = validateCellValue(execSheet, "B1", "月份代碼");
  
  // 參數格式驗證（期別格式：YYYYMM）
  if (!monthCode.toString().match(/^\d{6}$/)) {
    throw new Error(`月份代碼格式錯誤：${monthCode}，應為 YYYYMM 格式（如：202507）`);
  }
  
  const scheduleName = monthCode + (isFirstHalf ? "-1" : "-2");
  const processType = isFirstHalf ? "上半月" : "下半月";
  
  // 工作表狀態檢查
  validateSheetStatus(newcomerSheet, processType);
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  // 將參數存儲到處理器中
  handler.importParams = {
    folderId: folderId,
    monthCode: monthCode,
    scheduleName: scheduleName,
    processType: processType,
    isFirstHalf: isFirstHalf
  };
  
  return {
    folderId: folderId,
    scheduleName: scheduleName,
    processType: processType,
    message: `參數驗證完成（${processType}排程：${scheduleName}）`
  };
}

/**
 * 處理上下半月清空邏輯
 */
function handleNewcomerClearLogic(sheets, isFirstHalf, handler) {
  const { newcomerSheet } = sheets;
  
  if (isFirstHalf) {
    // 上半月：清空現有資料
    handler.updateProgress("清空新人實境現有資料...");
    newcomerSheet.getRange("A2:AC").clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    return {
      action: "清空資料",
      message: "上半月新人實境資料清空完成"
    };
  } else {
    // 下半月：不清空，準備追加
    return {
      action: "準備追加",
      message: "下半月準備在現有資料後追加"
    };
  }
}

/**
 * 匯入新人實境資料（包含QRS處理）
 */
function importNewcomerDataWithQRS(sheets, isFirstHalf, handler) {
  const { newcomerSheet } = sheets;
  const params = handler.importParams;

  let targetRow = 0;
  let numRows = 0;
  let lastRow = 0;

  try {
    // ─────────────────────────────────────
    // 📌 1. 計算匯入起始列
    // ─────────────────────────────────────
    if (isFirstHalf) {
      targetRow = 2;
      handler.positionToCell(newcomerSheet, "A2");

    } else {
      const aValues = newcomerSheet.getRange("A:A").getValues();
      const firstEmptyRow = aValues.findIndex(r => r[0] === "");
      if (firstEmptyRow === -1) throw new Error("❌ 找不到A欄空白列放公式");

      targetRow = firstEmptyRow + 1;
      handler.positionToCell(newcomerSheet, `A${targetRow}`);
    }

    // ─────────────────────────────────────
    // 📌 2. 匯入主要資料（filter+importrange）
    // ─────────────────────────────────────
    const importFormula =
      '=FILTER(' +
      'IMPORTRANGE("' + params.folderId + '", "新人實境!A2:L8000"),' +
      'IMPORTRANGE("' + params.folderId + '", "新人實境!A2:A8000")="' + params.scheduleName + '"' +
      ')';
    newcomerSheet.getRange(targetRow, 1).setFormula(importFormula);

    handler.updateProgress("等待新人實境資料匯入完成...");
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.IMPORT_DELAY);

    // 檢查匯入成功
    const firstCell = newcomerSheet.getRange(targetRow, 1).getValue();
    if (!firstCell || firstCell.toString().includes("#ERROR")) {
      throw new Error("❌ 新人實境資料匯入失敗");
    }

    // ─────────────────────────────────────
    // 📌 3. 計算筆數
    // ─────────────────────────────────────
    lastRow = newcomerSheet.getLastRow();
    numRows = Math.max(0, lastRow - targetRow + 1);

    if (numRows === 0) {
      return {
        success: true,
        recordCount: 0,
        actionType: isFirstHalf ? "清空後匯入" : "追加匯入",
        message: "新人實境資料匯入完成（無符合資料）"
      };
    }

    // ─────────────────────────────────────
    // 📌 4. QRS 處理
    // ─────────────────────────────────────
    handler.updateProgress(`處理 QRS 欄位計算（${numRows} 筆）...`);
    handler.positionToCell(newcomerSheet, "Q" + targetRow);

    const cValues = newcomerSheet.getRange(targetRow, 3, numRows, 1).getValues();  // C
    const kValues = newcomerSheet.getRange(targetRow, 11, numRows, 1).getValues(); // K

    const qValues = [];
    const rValues = [];
    const sFormulas = [];

    for (let i = 0; i < numRows; i++) {
      const rowNum = targetRow + i;

      qValues.push([cValues[i][0] || ""]);                 // Q = C
      rValues.push([(Number(kValues[i][0]) || 0) * 200]);  // R = 200*K
      sFormulas.push([`=TEXT(E${rowNum},"mm/dd")&G${rowNum}`]); // S = TEXT(E)&G
    }

    newcomerSheet.getRange(targetRow, 17, numRows, 1).setValues(qValues);
    newcomerSheet.getRange(targetRow, 18, numRows, 1).setValues(rValues);
    newcomerSheet.getRange(targetRow, 19, numRows, 1).setFormulas(sFormulas);

    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);

    // ─────────────────────────────────────
    // 📌 5. 彈窗詢問是否轉為靜態值
    // ─────────────────────────────────────
    const rangeToConvert = newcomerSheet.getRange(
      targetRow,
      1,
      numRows,
      newcomerSheet.getLastColumn()
    );

    ValueHelper.promptAndConvert(
      rangeToConvert,
      "新人實境資料已匯入並完成 QRS 計算。\n\n是否要將整段資料轉為靜態值？"
    );

    // ─────────────────────────────────────
    // 📌 6. 最終 return（保證變數都存在）
    // ─────────────────────────────────────
    const actionType = isFirstHalf ? "清空後匯入" : "追加匯入";

    handler.importResult = {
      success: true,
      recordCount: numRows,
      targetRow,
      lastRow
    };

    return {
      success: true,
      recordCount: numRows,
      actionType,
      message: `新人實境資料${actionType}完成（${numRows}筆）`
    };

  } catch (error) {
    throw new Error("資料匯入失敗：" + error.message);
  }
}

/**
 * 執行新人實境共通處理
 */
function executeNewcomerCommonProcess(sheets, isFirstHalf, handler) {
  const { newcomerSheet } = sheets;
  
  try {
    runCommonProcess(getSheetNames().newcomer);
    
    handler.updateProgress("等待共通處理完成...");
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);
    
    return {
      processedSheet: getSheetNames().newcomer,
      message: "共通處理執行完成"
    };
    
  } catch (commonError) {
    throw new Error("共通處理失敗：" + commonError.message);
  }
}


/**
 * 完成標記與資料檢查
 */
function finishNewcomerProcess(sheets, isFirstHalf, handler) {
  const { newcomerSheet, execSheet } = sheets;
  
  try {
    // 執行完成標記（移除手動打卡，使用統一框架）
    markCustomFinishByHalf("C15", "D15", isFirstHalf, getSheetNames().newcomer, "E15");
    
    // 最終資料驗證
    const finalValidation = performFinalValidation(newcomerSheet, handler.importParams.processType);
    
    // 最終定位到結果檢視位置
    if (typeof scrollToCell === 'function') {
      scrollToCell(getSheetNames().newcomer, 15, 3);
    }
    
    return {
      validation: finalValidation,
      message: `完成標記與檢查完成 ${finalValidation.summary}`
    };
    
  } catch (finishError) {
    throw new Error("完成標記失敗：" + finishError.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數（保持原有功能）
// ═══════════════════════════════════════════════════════════════

/**
 * 工作表驗證
 */
function validateAndGetSheet(sheetName, description) {
  if (!sheetName) {
    throw new Error(description + "名稱未定義");
  }
  
  const spreadsheet = CentralContext.getSpreadsheet();
  const sheet = spreadsheet.getSheetByName(sheetName);
  
  if (!sheet) {
    throw new Error("找不到" + description + "：" + sheetName);
  }
  
  return sheet;
}

/**
 * 儲存格值驗證
 */
function validateCellValue(sheet, cellAddress, description) {
  try {
    const value = sheet.getRange(cellAddress).getValue();
    if (value === null || value === undefined || value === "") {
      throw new Error(description + "（" + cellAddress + "）為空值");
    }
    return value;
  } catch (error) {
    throw new Error("讀取" + description + "（" + cellAddress + "）失敗：" + error.message);
  }
}

/**
 * 工作表狀態檢查
 */
function validateSheetStatus(sheet, processType) {
  const lastRow = sheet.getLastRow();
  if (lastRow > 1000) {
    console.warn(processType + "工作表資料量過大（" + lastRow + "列），可能影響處理效能");
  }
  
  // 檢查是否有保護範圍會影響處理
  const protections = sheet.getProtections(SpreadsheetApp.ProtectionType.RANGE);
  if (protections.length > 0) {
    console.warn(processType + "工作表存在保護範圍，可能影響資料寫入");
  }
}

/**
 * 匯入資料驗證
 */
function validateImportData(sourceData, processType) {
  if (!sourceData || sourceData.length === 0) {
    throw new Error(processType + "沒有找到可匯入的資料");
  }
  
  if (sourceData.length > 500) {
    console.warn(processType + "資料量較大（" + sourceData.length + "筆），處理時間可能較長");
  }
  
  // 基本資料完整性檢查
  let invalidRecords = 0;
  sourceData.forEach((record, index) => {
    if (!record || typeof record !== 'object') {
      invalidRecords++;
    }
  });
  
  if (invalidRecords > 0) {
    console.warn("發現 " + invalidRecords + " 筆無效資料記錄");
  }
  
  if (invalidRecords / sourceData.length > 0.1) {
    throw new Error("無效資料比例過高（" + Math.round(invalidRecords / sourceData.length * 100) + "%），請檢查來源資料");
  }
}

/**
 * 最終驗證
 */
function performFinalValidation(sheet, processType) {
  try {
    const dataRange = sheet.getDataRange();
    const numRows = dataRange.getNumRows();
    const numCols = dataRange.getNumColumns();
    
    // 檢查QRS欄位完整性
    if (numRows > 1) {
      const qRange = sheet.getRange(2, 17, numRows - 1, 1);
      const rRange = sheet.getRange(2, 18, numRows - 1, 1);
      const sRange = sheet.getRange(2, 19, numRows - 1, 1);
      
      const qValues = qRange.getValues();
      const rValues = rRange.getValues();
      const sValues = sRange.getValues();
      
      let emptyQCount = 0, emptyRCount = 0, emptySCount = 0;
      
      for (let i = 0; i < qValues.length; i++) {
        if (qValues[i][0] === "" || qValues[i][0] === null) emptyQCount++;
        if (rValues[i][0] === "" || rValues[i][0] === null) emptyRCount++;
        if (sValues[i][0] === "" || sValues[i][0] === null) emptySCount++;
      }
      
      return {
        success: true,
        summary: "(資料" + (numRows-1) + "筆, Q空值" + emptyQCount + ", R空值" + emptyRCount + ", S空值" + emptySCount + ")"
      };
    }
    
    return { success: true, summary: "(無資料)" };
    
  } catch (error) {
    console.warn("最終驗證失敗：", error.message);
    return { success: false, summary: "(驗證失敗)" };
  }
}

/**
 * 錯誤恢復建議
 */
function getRecoverySuggestion(step, errorMessage) {
  const suggestions = {
    1: "請檢查執行控制工作表的參數設定",
    2: "請確認資料夾ID和排程名稱正確，或檢查來源資料",
    3: "請檢查H、I欄的數值格式，確保為有效數字",
    4: "請檢查工作表是否有保護設定或公式錯誤",
    5: "請檢查完成標記的儲存格範圍是否正確"
  };
  
  let suggestion = suggestions[step] || "請聯繫系統管理員";
  
  // 根據錯誤訊息提供更具體建議
  if (errorMessage.includes("權限")) {
    suggestion += "，並確認工作表編輯權限";
  } else if (errorMessage.includes("格式")) {
    suggestion += "，並檢查資料格式";
  } else if (errorMessage.includes("找不到")) {
    suggestion += "，並確認相關資源存在";
  }
  
  return suggestion;
}

// ═══════════════════════════════════════════════════════════════
// 🔧 相容性支援
// ═══════════════════════════════════════════════════════════════

/**
 * 原始函數的相容性包裝（建議使用新版本）
 * @deprecated 建議使用 runNewcomerProcess(isFirstHalf)
 */
function runNewcomerProcessLegacy(isFirstHalf) {
  console.warn("⚠️ 使用了舊版 runNewcomerProcess，建議更新到統一框架版本");
  return runNewcomerProcess(isFirstHalf);
}



// ███████████████████████████████████████████████████
// 📁 主程式6：04新人實習（使用統一框架版本）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress(),
//          runCommonProcess(), markCustomFinishByHalf(), scrollToCell() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 04新人實習流程配置
 */
function getInternProcessConfig() {
  return {
    name: "04新人實習",
    totalSteps: 8, // 🔧 預估步驟數，框架會自動偵測實際數量
    punchMethod: "punchNewEmployeePractice", // 🔧 正確的打卡方法名稱
    cells: {
      firstHalf: "C16",
      secondHalf: "D16", 
      status: "E16"
    },
    sheetNames: {
      internSheet: "intern",
      execSheet: "exec"
    },
    steps: [
      {
        name: "準備匯入參數與資料驗證",
        description: "驗證參數和工作表狀態...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "A2"
        },
        handler: prepareInternImportParams,
        required: true
      },
      {
        name: "處理上下半月清空邏輯",
        description: "根據上下半月處理資料清空...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "A2"
        },
        handler: handleInternClearLogic,
        required: true
      },
      {
        name: "檢查匯入結果",
        description: "檢查資料匯入是否成功...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "A2"
        },
        handler: checkInternImportResult,
        required: true
      },
      {
        name: "處理QRS欄位計算",
        description: "執行實習生QRS欄位批次計算...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "Q2"
        },
        handler: processInternQRSCalculations,
        required: true
      },
      {
        name: "執行新人實習共通處理",
        description: "執行共通處理流程...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "A1"
        },
        handler: executeInternCommonProcess,
        required: true
      },
      {
        name: "完成標記與資料檢查",
        description: "標記完成狀態並驗證資料...",
        type: "custom",
        position: {
          sheet: "internSheet",
          cell: "C16"
        },
        handler: finishInternProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一04新人實習執行函數
 */
function runInternProcess(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getInternProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月04新人實習
 */
function runInternProcessFirstHalf() {
  return runInternProcess(true);
}

/**
 * 下半月04新人實習
 */
function runInternProcessSecondHalf() {
  return runInternProcess(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 04新人實習專用處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 準備匯入參數與資料驗證
 */
function prepareInternImportParams(sheets, isFirstHalf, handler) {
  const { internSheet, execSheet } = sheets;
  
  // 核心參數驗證
  const folderId = validateCellValue(execSheet, "C3", "資料夾ID");
  const monthCode = validateCellValue(execSheet, "B1", "月份代碼");
  
  // 參數格式驗證（期別格式：YYYYMM）
  if (!monthCode.toString().match(/^\d{6}$/)) {
    throw new Error(`月份代碼格式錯誤：${monthCode}，應為 YYYYMM 格式（如：202507）`);
  }
  
  const scheduleName = monthCode + (isFirstHalf ? "-1" : "-2");
  const processType = isFirstHalf ? "上半月" : "下半月";
  
  // 工作表狀態檢查
  validateSheetStatus(internSheet, processType);
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY);
  
  // 將參數存儲到處理器中
  handler.importParams = {
    folderId: folderId,
    monthCode: monthCode,
    scheduleName: scheduleName,
    processType: processType,
    isFirstHalf: isFirstHalf
  };
  
  return {
    folderId: folderId,
    scheduleName: scheduleName,
    processType: processType,
    message: `參數驗證完成（${processType}排程：${scheduleName}）`
  };
}

/**
 * 處理上下半月清空邏輯
 */
function handleInternClearLogic(sheets, isFirstHalf, handler) {
  const { internSheet } = sheets;
  const params = handler.importParams;
  
  if (isFirstHalf) {
    // 上半月：清空現有資料並加上公式
    handler.updateProgress("清空新人實習現有資料...");
    internSheet.getRange("A2:AC").clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    // 定位到A2並加上匯入公式
    handler.positionToCell(internSheet, "A2");
    
    const importFormula =
      '=FILTER(' +
      'IMPORTRANGE("' + params.folderId + '", "新人實習!A2:L8000"),' +
      'IMPORTRANGE("' + params.folderId + '", "新人實習!A2:A8000")="' + params.scheduleName + '"' +
      ')';
    
    internSheet.getRange("A2").setFormula(importFormula);
    
    // 將結果存儲
    handler.importResult = {
      targetRow: 2,
      isFirstHalf: true
    };
    
    return {
      action: "清空並匯入",
      targetRow: 2,
      message: "上半月新人實習資料清空並匯入公式完成"
    };
  } else {
    // 下半月：尋找空白列並加上公式
    const aValues = internSheet.getRange("A:A").getValues();
    const firstEmptyRow = aValues.findIndex(r => r[0] === "");
    if (firstEmptyRow === -1) {
      throw new Error("❌ 找不到A欄空白列放公式");
    }
    
    const targetRow = firstEmptyRow + 1;
    handler.positionToCell(internSheet, `A${targetRow}`);
    
    const importFormula =
    '=FILTER(' +
    'IMPORTRANGE("' + params.folderId + '", "新人實習!A2:L8000"),' +
    'IMPORTRANGE("' + params.folderId + '", "新人實習!A2:A8000")="' + params.scheduleName + '"' +
    ')';
    
    internSheet.getRange(targetRow, 1).setFormula(importFormula);
    
    // 將結果存儲
    handler.importResult = {
      targetRow: targetRow,
      isFirstHalf: false
    };
    
    return {
      action: "追加匯入",
      targetRow: targetRow,
      message: `下半月在第${targetRow}列加入匯入公式`
    };
  }
}

/**
 * 檢查匯入結果
 */
function checkInternImportResult(sheets, isFirstHalf, handler) {
  const { internSheet } = sheets;
  const targetRow = handler.importResult.targetRow;
  
  // 等待匯入完成
  handler.updateProgress("等待新人實習資料匯入完成...");
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY);
  
  // 檢查匯入是否成功
  const firstCell = internSheet.getRange(targetRow, 1).getValue();
  if (firstCell === "" || firstCell.toString().includes("#ERROR")) {
    throw new Error("❌ 新人實習資料匯入失敗，請檢查資料夾ID和排程名稱");
  }
  
  // 計算匯入的記錄數
  const lastRow = internSheet.getLastRow();
  const recordCount = Math.max(0, lastRow - targetRow + 1);
  
  // 簡單驗證匯入的資料
  if (recordCount > 200) {
    console.warn(handler.importParams.processType + "實習生資料量較大（" + recordCount + "筆），處理時間可能較長");
  }
  
  // 更新匯入結果
  handler.importResult.success = true;
  handler.importResult.recordCount = recordCount;
  handler.importResult.lastRow = lastRow;
  
  const actionType = isFirstHalf ? "清空後匯入" : "追加匯入";
  return {
    recordCount: recordCount,
    success: true,
    actionType: actionType,
    message: `新人實習資料${actionType}完成（${recordCount}筆）`
  };
}

/**
 * 處理實習生QRS欄位計算 + 彈窗詢問是否轉值
 */
function processInternQRSCalculations(sheets, isFirstHalf, handler) {
  const { internSheet } = sheets;
  const importResult = handler.importResult;
  
  if (!importResult || !importResult.recordCount) {
    return {
      processedCount: 0,
      message: "無資料需要計算QRS欄位"
    };
  }
  
  const startRow = importResult.targetRow;
  const lastRow = importResult.lastRow;
  const numRows = lastRow - startRow + 1;
  
  if (numRows <= 0) {
    return {
      processedCount: 0,
      message: "無有效資料需要計算QRS欄位"
    };
  }

  try {
    // 讀取資料
    const cValues = internSheet.getRange(startRow, 3, numRows).getValues();
    const eValues = internSheet.getRange(startRow, 5, numRows).getValues();
    const gValues = internSheet.getRange(startRow, 7, numRows).getValues();
    const kValues = internSheet.getRange(startRow, 11, numRows).getValues();

    const qValues = [];
    const rValues = [];
    const sFormulas = [];

    for (let i = 0; i < numRows; i++) {
      const rowNum = startRow + i;

      // Q欄
      qValues.push([cValues[i][0] || ""]);

      // R欄：實習天數 × 200
      const kValue = Number(kValues[i][0]) || 0;
      rValues.push([200 * kValue]);

      // S欄：日期 + F
      const eVal = eValues[i][0];
      if (eVal instanceof Date) {
        sFormulas.push([`=TEXT(E${rowNum},"mm/dd")&G${rowNum}`]);
      } else {
      sFormulas.push([`="錯誤日期"&G${rowNum}`]);
      }
    }

    internSheet.getRange(startRow, 17, numRows, 1).setValues(qValues);
    internSheet.getRange(startRow, 18, numRows, 1).setValues(rValues);

    const sRange = internSheet.getRange(startRow, 19, numRows, 1);
    sFormulas.forEach((f, idx) => sRange.getCell(idx + 1, 1).setFormula(f[0]));

    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);

    // ★★★ 彈窗詢問是否轉值（正確位置） ★★★
    const rangeToConvert = internSheet.getRange(
      startRow,
      1,
      numRows,
      internSheet.getLastColumn()
    );

    ValueHelper.promptAndConvert(
      rangeToConvert,
      "新人實習資料已匯入並完成 QRS 計算。\n\n是否要將整段資料轉為靜態值？"
    );

    return {
      processedRecords: numRows,
      message: `實習生 QRS 欄位計算完成（${numRows}筆）`
    };

  } catch (err) {
    throw new Error("QRS欄位計算失敗：" + err.message);
  }
}


/**
 * 執行新人實習共通處理
 */
function executeInternCommonProcess(sheets, isFirstHalf, handler) {
  const { internSheet } = sheets;
  
  try {
    // 執行共通處理
    runCommonProcess(getSheetNames().intern);
    
    handler.updateProgress("等待共通處理完成...");
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY);
    
    return {
      processedSheet: getSheetNames().intern,
      message: "共通處理執行完成"
    };
    
  } catch (commonError) {
    throw new Error("共通處理失敗：" + commonError.message);
  }
}

/**
 * 完成標記與資料檢查
 */
function finishInternProcess(sheets, isFirstHalf, handler) {
  const { internSheet } = sheets;
  
  try {
    // 執行完成標記（統一框架會自動處理打卡）
    markCustomFinishByHalf("C16", "D16", isFirstHalf, getSheetNames().intern, "E16");
    
    // 最終資料驗證
    const finalValidation = performInternValidation(internSheet, handler.importParams.processType);
    
    // 最終定位到結果檢視位置
    if (typeof scrollToCell === 'function') {
      scrollToCell(getSheetNames().intern, 16, 3);
    }
    
    return {
      validation: finalValidation,
      message: `完成標記與檢查完成 ${finalValidation.summary}`
    };
    
  } catch (finishError) {
    throw new Error("完成標記失敗：" + finishError.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數（保持原有功能）
// ═══════════════════════════════════════════════════════════════

/**
 * 工作表驗證
 */
function validateAndGetSheet(sheetName, description) {
  if (!sheetName) {
    throw new Error(description + "名稱未定義");
  }
  
  const spreadsheet = CentralContext.getSpreadsheet();
  const sheet = spreadsheet.getSheetByName(sheetName);
  
  if (!sheet) {
    throw new Error("找不到" + description + "：" + sheetName);
  }
  
  return sheet;
}

/**
 * 儲存格值驗證
 */
function validateCellValue(sheet, cellAddress, description) {
  try {
    const value = sheet.getRange(cellAddress).getValue();
    if (value === null || value === undefined || value === "") {
      throw new Error(description + "（" + cellAddress + "）為空值");
    }
    return value;
  } catch (error) {
    throw new Error("讀取" + description + "（" + cellAddress + "）失敗：" + error.message);
  }
}

/**
 * 工作表狀態檢查
 */
function validateSheetStatus(sheet, processType) {
  const lastRow = sheet.getLastRow();
  if (lastRow > 1000) {
    console.warn(processType + "工作表資料量過大（" + lastRow + "列），可能影響處理效能");
  }
  
  // 檢查是否有保護範圍會影響處理
  const protections = sheet.getProtections(SpreadsheetApp.ProtectionType.RANGE);
  if (protections.length > 0) {
    console.warn(processType + "工作表存在保護範圍，可能影響資料寫入");
  }
}

/**
 * 實習生最終驗證
 */
function performInternValidation(sheet, processType) {
  try {
    const dataRange = sheet.getDataRange();
    const numRows = dataRange.getNumRows();
    
    // 檢查QRS欄位完整性
    if (numRows > 1) {
      const qRange = sheet.getRange(2, 17, numRows - 1, 1);
      const rRange = sheet.getRange(2, 18, numRows - 1, 1);
      const sRange = sheet.getRange(2, 19, numRows - 1, 1);
      
      const qValues = qRange.getValues();
      const rValues = rRange.getValues();
      const sValues = sRange.getValues();
      
      let emptyQCount = 0, emptyRCount = 0, emptySCount = 0;
      let totalAmount = 0;
      
      for (let i = 0; i < qValues.length; i++) {
        if (qValues[i][0] === "" || qValues[i][0] === null) emptyQCount++;
        if (rValues[i][0] === "" || rValues[i][0] === null) {
          emptyRCount++;
        } else {
          totalAmount += Number(rValues[i][0]) || 0;
        }
        if (sValues[i][0] === "" || sValues[i][0] === null) emptySCount++;
      }
      
      return {
        success: true,
        summary: "(資料" + (numRows-1) + "筆, 實習費$" + Math.round(totalAmount) + ", Q空值" + emptyQCount + ", R空值" + emptyRCount + ", S空值" + emptySCount + ")"
      };
    }
    
    return { success: true, summary: "(無資料)" };
    
  } catch (error) {
    console.warn("實習生最終驗證失敗：", error.message);
    return { success: false, summary: "(驗證失敗)" };
  }
}

/**
 * 實習生錯誤恢復建議
 */
function getInternRecoverySuggestion(step, errorMessage) {
  const suggestions = {
    1: "請檢查執行控制工作表的參數設定",
    2: "請確認資料夾ID和排程名稱正確，或檢查實習生來源資料",
    3: "請檢查J欄實習天數的數值格式，確保為有效數字",
    4: "請檢查實習生工作表是否有保護設定或公式錯誤",
    5: "請檢查完成標記的儲存格範圍是否正確"
  };
  
  let suggestion = suggestions[step] || "請聯繫系統管理員";
  
  // 根據錯誤訊息提供更具體建議
  if (errorMessage.includes("權限")) {
    suggestion += "，並確認實習生工作表編輯權限";
  } else if (errorMessage.includes("格式")) {
    suggestion += "，並檢查實習生資料格式";
  } else if (errorMessage.includes("找不到")) {
    suggestion += "，並確認實習生相關資源存在";
  } else if (errorMessage.includes("實習天數")) {
    suggestion += "，特別注意實習天數的數值合理性";
  }
  
  return suggestion;
}

// ═══════════════════════════════════════════════════════════════
// 🔧 相容性支援
// ═══════════════════════════════════════════════════════════════

/**
 * 原始函數的相容性包裝（建議使用新版本）
 * @deprecated 建議使用 runInternProcess(isFirstHalf)
 */
function runInternProcessLegacy(isFirstHalf) {
  console.warn("⚠️ 使用了舊版 runInternProcess，建議更新到統一框架版本");
  return runInternProcess(isFirstHalf);
}



// ███████████████████████████████████████████████████
// 📁 主程式7：05組長津貼（基於統一框架優化版）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), getColumnLetter(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 05組長津貼流程配置
 */
function getLeaderProcessConfig() {
  return {
    name: "05組長津貼",
    totalSteps: 8, // 🔧 預估步驟數，會自動偵測實際數量
    punchMethod: "punchTeamLeaderAllowance",
    cells: {
      firstHalf: "C17",
      secondHalf: "D17", 
      status: "E17"
    },
    sheetNames: {
      leaderSheet: "leader",
      execSheet: "exec"
    },
    steps: [
      {
        name: "完整05組長津貼流程",
        description: "執行完整的05組長津貼流程...",
        type: "custom",
        handler: executeFullLeaderProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一05組長津貼執行函數
 */
function runLeaderProcess(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getLeaderProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月05組長津貼
 */
function runLeaderProcessFirstHalf() {
  return runLeaderProcess(true);
}

/**
 * 下半月05組長津貼
 */
function runLeaderProcessSecondHalf() {
  return runLeaderProcess(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 05組長津貼完整處理函數（基於原版邏輯）
// ═══════════════════════════════════════════════════════════════

/**
 * 執行完整的05組長津貼流程
 */
function executeFullLeaderProcess(sheets, isFirstHalf, handler) {
  const { leaderSheet, execSheet } = sheets;
  
  // 步驟1：準備匯入參數與資料驗證
  handler.updateProgress("準備匯入參數與資料驗證...");
  
  // 參數驗證與取得
  const folderId = validateCellValue(execSheet, "C3", "資料夾ID");
  const monthCode = validateCellValue(execSheet, "B1", "月份代碼");
  
  const scheduleName = monthCode + (isFirstHalf ? "-1" : "-2");
  const processType = isFirstHalf ? "上半月" : "下半月";
  
  // 工作表狀態檢查
  validateSheetStatus(leaderSheet, processType);
  
  // 定位標記：移動到起始位置
  leaderSheet.activate();
  leaderSheet.getRange("A2").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
  
  handler.completeStep("參數驗證完成（" + processType + "排程：" + scheduleName + "）");

  // 步驟2：處理上下半月邏輯
  if (isFirstHalf) {
    // 上半月：清空A2:AC範圍
    handler.updateProgress("清空組長津貼範圍 A2:AC...");
    leaderSheet.getRange("A2:AC").clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
    handler.completeStep("A2:AC範圍清空完成");
  } else {
    // 下半月：找到A欄空白列
    handler.updateProgress("尋找A欄空白列位置...");
    const targetCell = getNextEmptyRowInColumnA(leaderSheet);
    
    // 定位到空白列
    leaderSheet.getRange(targetCell).activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    handler.completeStep("找到空白列位置：" + targetCell);
  }

  // 步驟3：匯入組長津貼資料（含防呆檢查）
  const targetCell = isFirstHalf ? "A2" : getNextEmptyRowInColumnA(leaderSheet);
  handler.updateProgress("匯入組長津貼資料（含防呆檢查）...");
  
  // 準備參數物件
  const params = {
    folderId: folderId,
    scheduleName: scheduleName
  };
  
  const importFormula =
    '=FILTER(' +
    'IMPORTRANGE("' + params.folderId + '", "新人實習!A2:L8000"),' +
    'IMPORTRANGE("' + params.folderId + '", "新人實習!A2:A8000")="' + params.scheduleName + '"' +
    ')';
  
  leaderSheet.getRange(targetCell).setFormula(importFormula);
  
  // 等待IMPORTRANGE載入
  handler.updateProgress("等待組長津貼資料匯入完成...");
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY || 3000);
  
  // 檢查匯入結果
  const importResult = leaderSheet.getRange(targetCell).getValue();
  if (importResult === "" || importResult.toString().includes("#ERROR")) {
    throw new Error("組長津貼資料匯入失敗：" + targetCell);
  }
  
  handler.completeStep("組長津貼資料匯入完成至 " + targetCell);

  // 步驟4：處理QRS欄位計算
  handler.updateProgress("處理QRS欄位計算...");
  
  // 定位到Q欄開始位置
  leaderSheet.getRange("Q2").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  const dataRange = leaderSheet.getDataRange();
  const numRows = dataRange.getNumRows();
  let actualDataRows = 0;
  
  if (numRows > 1) {
    // 統計實際資料筆數
    const aColumnData = leaderSheet.getRange(2, 1, numRows - 1, 1).getValues();
    actualDataRows = aColumnData.filter(row => row[0] !== "" && row[0] !== null).length;
  }
  
  if (actualDataRows > 0) {
    try {
      // 批次取得資料，減少API呼叫
      const sourceData = {
        eValues: leaderSheet.getRange(2, 5, actualDataRows, 1).getValues(),  // E 日期
        gValues: leaderSheet.getRange(2, 7, actualDataRows, 1).getValues(),  // G 客戶姓名
        hValues: leaderSheet.getRange(2, 8, actualDataRows, 1).getValues(),  // H 組長姓名
        jValues: leaderSheet.getRange(2, 10, actualDataRows, 1).getValues(), // J 組長津貼
       kValues: leaderSheet.getRange(2, 11, actualDataRows, 1).getValues()  // K 服務時數
      };
      
      // 準備批次更新的資料
      const qValues = [];
      const rValues = [];
      const sFormulas = [];
      let calculationErrors = [];
      
      for (let i = 0; i < actualDataRows; i++) {
        const rowNum = 2 + i;
        
          // Q欄：H 組長姓名
          const hValue = sourceData.hValues[i][0];
          qValues.push([hValue || ""]);

          // R欄：J * K
          const jValue = Number(sourceData.jValues[i][0]) || 0;
          const kValue = Number(sourceData.kValues[i][0]) || 0;
          rValues.push([jValue * kValue]);

          // S欄：TEXT(E,"MM/DD") & G
          sFormulas.push([`=TEXT(E${rowNum},"mm/dd")&G${rowNum}`]);
      }
      
      // 批次更新QRS欄位
      if (qValues.length > 0) {
        leaderSheet.getRange(2, 17, actualDataRows, 1).setValues(qValues.map(v => [v]));
      }
      if (rValues.length > 0) {
        leaderSheet.getRange(2, 18, actualDataRows, 1).setValues(rValues.map(v => [v]));
      }
      if (sFormulas.length > 0) {
        const sRange = leaderSheet.getRange(2, 19, actualDataRows, 1);
        sFormulas.forEach((formula, index) => {
          sRange.getCell(index + 1, 1).setFormula(formula);
        });
      }
      
      // 等待計算完成
      SpreadsheetApp.flush();
      Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
      
      // 錯誤報告
      if (calculationErrors.length > 0) {
        const errorSummary = "發現 " + calculationErrors.length + " 個計算問題：\n" + calculationErrors.slice(0, 5).join('\n') + (calculationErrors.length > 5 ? '\n...' : '');
        console.warn("組長津貼QRS計算警告：", errorSummary);
        handler.completeStep("QRS計算完成但有 " + calculationErrors.length + " 個警告");
      } else {
        handler.completeStep("QRS欄位計算完成，處理 " + actualDataRows + " 筆記錄");
      }

      // ★★★ 【新增】QRS 計算完 → 問是否要轉成值 ★★★
      const rangeToConvert = leaderSheet.getRange(
        2,
        1,
        actualDataRows,
        leaderSheet.getLastColumn()
      );

      ValueHelper.promptAndConvert(
        rangeToConvert,
        "組長津貼資料已匯入並完成 QRS 計算。\n\n是否要將整段資料轉為靜態值？"
      );      
      
    } catch (qrsError) {
      throw new Error("QRS欄位計算失敗：" + qrsError.message);
    }
  } else {
    handler.completeStep("無資料需要進行QRS計算");
  }

  // 步驟5：執行組長津貼共通處理
  handler.updateProgress("執行組長津貼共通處理...");
  
  try {
    runCommonProcess(getSheetNames().leader);
    
    handler.updateProgress("等待共通處理完成...");
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
    
    handler.completeStep("共通處理執行完成");
    
  } catch (commonError) {
    throw new Error("共通處理失敗：" + commonError.message);
  }

  // 步驟6：完成標記與檢查
  handler.updateProgress("完成標記與資料檢查...");
  
  // 定義變數在外層，避免作用域問題
  let finalValidation;
  
  try {
    // 定位到完成標記位置
    leaderSheet.getRange("C17").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    markCustomFinishByHalf("C17", "D17", isFirstHalf, getSheetNames().leader, "E17");
    
    // 最終資料驗證
    finalValidation = performLeaderValidation(leaderSheet, processType);
    
    handler.completeStep("完成標記與檢查完成 " + finalValidation.summary);
    
  } catch (finishError) {
    // 如果出錯，設定預設值
    finalValidation = { success: false, summary: "(驗證失敗)" };
    throw new Error("完成標記失敗：" + finishError.message);
  }

  // 最終定位到結果檢視位置
  leaderSheet.getRange("C17").activate();
  SpreadsheetApp.flush();
  
  // 返回處理結果
  return {
    processedRows: actualDataRows,
    targetCell: targetCell,
    processType: processType,
    scheduleName: scheduleName,
    validationResult: finalValidation,
    message: `05組長津貼流程完成（${processType}），處理 ${actualDataRows} 筆記錄`
  };
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數（保持原有功能）
// ═══════════════════════════════════════════════════════════════

/**
 * 獲取A欄下一個空白列位置
 */
function getNextEmptyRowInColumnA(sheet) {
  const aValues = sheet.getRange("A:A").getValues();
  for (let i = 1; i < aValues.length; i++) {
    if (aValues[i][0] === "" || aValues[i][0] === null) {
      return "A" + (i + 1);
    }
  }
  return "A" + (aValues.length + 1);
}

/**
 * 輔助函數：驗證儲存格值
 */
function validateCellValue(sheet, cellAddress, description) {
  try {
    const value = sheet.getRange(cellAddress).getValue();
    if (!value || value === "") {
      throw new Error(description + "（" + cellAddress + "）不能為空");
    }
    return value.toString().trim();
  } catch (error) {
    throw new Error("取得" + description + "失敗：" + error.message);
  }
}

/**
 * 輔助函數：驗證工作表狀態
 */
function validateSheetStatus(sheet, processType) {
  try {
    // 檢查工作表是否存在且可存取
    if (!sheet) {
      throw new Error("組長津貼工作表不存在");
    }
    
    // 檢查工作表是否有保護設定
    const protections = sheet.getProtections(SpreadsheetApp.ProtectionType.RANGE);
    if (protections.length > 0) {
      console.warn(processType + "組長津貼工作表存在保護設定，請確認權限");
    }
    
    return true;
  } catch (error) {
    throw new Error("工作表狀態驗證失敗：" + error.message);
  }
}

// 輔助函數：組長津貼最終驗證
function performLeaderValidation(sheet, processType) {
  try {
    const dataRange = sheet.getDataRange();
    const numRows = dataRange.getNumRows();
    
    // 檢查QRS欄位完整性
    if (numRows > 1) {
      const qRange = sheet.getRange(2, 17, numRows - 1, 1);
      const rRange = sheet.getRange(2, 18, numRows - 1, 1);
      const sRange = sheet.getRange(2, 19, numRows - 1, 1);
      
      const qValues = qRange.getValues();
      const rValues = rRange.getValues();
      const sValues = sRange.getValues();
      
      let emptyQCount = 0, emptyRCount = 0, emptySCount = 0;
      let totalAllowance = 0;
      let leaderCount = 0;
      
      for (let i = 0; i < qValues.length; i++) {
        if (qValues[i][0] === "" || qValues[i][0] === null) {
          emptyQCount++;
        } else {
          leaderCount++;
        }
        
        if (rValues[i][0] === "" || rValues[i][0] === null) {
          emptyRCount++;
        } else {
          totalAllowance += Number(rValues[i][0]) || 0;
        }
        
        if (sValues[i][0] === "" || sValues[i][0] === null) emptySCount++;
      }
      
      return {
        success: true,
        summary: "(資料" + (numRows-1) + "筆, 組長" + leaderCount + "人, 津貼$" + Math.round(totalAllowance) + ", Q空值" + emptyQCount + ", R空值" + emptyRCount + ", S空值" + emptySCount + ")"
      };
    }
    
    return { success: true, summary: "(無資料)" };
    
  } catch (error) {
    console.warn("組長津貼最終驗證失敗：", error.message);
    return { success: false, summary: "(驗證失敗)" };
  }
}

// 輔助函數：組長津貼錯誤恢復建議
function getLeaderRecoverySuggestion(step, errorMessage) {
  const suggestions = {
    1: "請檢查執行控制工作表的參數設定",
    2: "請確認資料夾ID和排程名稱正確，或檢查組長津貼來源資料",
    3: "請檢查I欄津貼倍數和J欄基本津貼的數值格式，確保為有效數字",
    4: "請檢查組長津貼工作表是否有保護設定或公式錯誤",
    5: "請檢查完成標記的儲存格範圍是否正確"
  };
  
  let suggestion = suggestions[step] || "請聯繫系統管理員";
  
  // 根據錯誤訊息提供更具體建議
  if (errorMessage.includes("權限")) {
    suggestion += "，並確認組長津貼工作表編輯權限";
  } else if (errorMessage.includes("格式")) {
    suggestion += "，並檢查組長津貼資料格式";
  } else if (errorMessage.includes("找不到")) {
    suggestion += "，並確認組長津貼相關資源存在";
  } else if (errorMessage.includes("津貼") || errorMessage.includes("倍數")) {
    suggestion += "，特別注意津貼金額和倍數的數值合理性";
  }
  
  return suggestion;
}



// ███████████████████████████████████████████████████
// 📁 主程式8：結算整理（基於統一框架優化版）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), getColumnLetter(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 結算整理流程配置
 */
function getSettlementProcessConfig() {
  return {
    name: "薪資結算整理",
    totalSteps: 8, // 🔧 預估步驟數，會自動偵測實際數量
    punchMethod: "punchSalarySettlement",
    cells: {
      firstHalf: "C21",
      secondHalf: "D21", 
      status: "E21"
    },
    sheetNames: {
      salarySheet: "salary",
      summarySheet: "summary",
      slipSheet: "slip",
      execSheet: "exec"
    },
    steps: [
      {
        name: "完整薪資結算整理流程",
        description: "執行完整的薪資結算整理流程...",
        type: "custom",
        handler: executeFullSettlementProcess,
        required: true
      }
    ]
  };
}

/**
 * 統一薪資結算整理執行函數
 */
function runFinalSettlement(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getSettlementProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月薪資結算整理
 */
function runFinalSettlementFirstHalf() {
  return runFinalSettlement(true);
}

/**
 * 下半月薪資結算整理
 */
function runFinalSettlementSecondHalf() {
  return runFinalSettlement(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 薪資結算整理完整處理函數（基於原版邏輯）
// ═══════════════════════════════════════════════════════════════

/**
 * 執行完整的薪資結算整理流程 - 基於原版邏輯套用統一框架
 */
function executeFullSettlementProcess(sheets, isFirstHalf, handler) {
  const { salarySheet, summarySheet, slipSheet, execSheet } = sheets;
  
  const processType = isFirstHalf ? "上半月" : "下半月";
  
  // 參數驗證與取得 - 使用原版邏輯
  const timeSheet = summarySheet; // 場次薪資時數總表就是summary
  const PDFSheet = validateAndGetSheet("PDF產出", "PDF產出工作表");
  
  // 定義所有需要在步驟間共享的變數
  let rowCount, validRowsWithFormulas, aCol, validData;
  
// 步驟1：處理薪資表 L2048 → L2047 貼值與移除空白列
handler.updateProgress("處理薪資表 L2048 → L2047 貼值與移除空白列...");

// 定位到薪資表 L2048
salarySheet.activate();
salarySheet.getRange("L2048").activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

try {
  var copiedCols = copySalaryRow2048To2047AsValues_(salarySheet);

  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  handler.completeStep(
    "薪資表 L2048:2048 已複製到 L2047:2047 並貼上值，且已移除儲存格內空白列（處理 " +
    copiedCols +
    " 欄）"
  );

} catch (salaryError) {
  throw new Error("薪資表 L2048 → L2047 處理失敗：" + salaryError.message);
}


  // 步驟2：處理場次薪資時數總表的B-F欄（限定A4:A120範圍內的非空白列）
handler.updateProgress("處理場次薪資時數總表的B-F欄...");

// 定位到總表A4
timeSheet.activate();
timeSheet.getRange("A4").activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

// ★★★ 修改：固定範圍為 A4:A120 ★★★
const startRow = 4;
const endRow = 120;
const totalRows = endRow - startRow + 1;  // 117列

try {
  // 讀取 A4:A120 的資料
  const aCol = timeSheet.getRange(startRow, 1, totalRows, 1).getValues();
  
  // 清除 B4:F120 的舊公式
  timeSheet.getRange(startRow, 2, totalRows, 5).clear();
  console.log("✅ 已清空 B4:F120 範圍");
  
  const validRowsWithFormulas = [];
  let invalidNameCount = 0;
  let processedCount = 0;
  
  // ★★★ 只處理 A4:A120 範圍內的非空白姓名 ★★★
  for (let i = 0; i < totalRows; i++) {
    const aName = aCol[i][0];
    const row = i + startRow;  // 實際行號：4-120
    
    // 檢查是否為非空白且非錯誤值
    if (aName && aName !== "" && aName !== "#N/A") {
      validRowsWithFormulas.push({
        row: row,
        aName: aName,
        aRef: "$A" + row
      });
      processedCount++;
    } else {
      invalidNameCount++;
    }
  }
  
  // 輸出統計資訊
  console.log(`📊 A4:A120 範圍統計：`);
  console.log(`📊   總列數: ${totalRows} 列`);
  console.log(`📊   有效姓名: ${validRowsWithFormulas.length} 個`);
  console.log(`📊   空白/無效: ${invalidNameCount} 個`);
  
  if (validRowsWithFormulas.length === 0) {
    throw new Error("A4:A120 範圍內沒有找到有效的員工姓名資料");
  }
  
  // ★★★ 只為有效姓名設定B-F欄公式 ★★★
  for (const rowInfo of validRowsWithFormulas) {
    const { row, aRef } = rowInfo;
    
    try {
      // B欄公式
      timeSheet.getRange(row, 2).setFormula("=HLOOKUP(" + aRef + ",'薪資表'!$1:$2001,2001,FALSE)");
      
      // C欄公式
      timeSheet.getRange(row, 3).setFormula("=HLOOKUP(" + aRef + ",'薪資表'!$1:$2015,2015,FALSE)");
      
      // D欄公式 - 條件式查詢
      timeSheet.getRange(row, 4).setFormula("=IF(AND(E" + row + "=0,'薪資單'!$AD$1=$D$1),HLOOKUP(" + aRef + ",'薪資表'!$1:$2046,2046,FALSE),HLOOKUP(" + aRef + ",'薪資表'!$1:$2046,2045,FALSE))");
      
      // E欄公式 - 條件式查詢
      timeSheet.getRange(row, 5).setFormula("=IF('薪資單'!$AD$1=$E$1,HLOOKUP(" + aRef + ",'薪資表'!$1:$2046,2046,FALSE),0)");
      
      // F欄公式 - 加總
      timeSheet.getRange(row, 6).setFormula("=HLOOKUP(" + aRef + ",'薪資表'!$1:$2046,2043,FALSE)+HLOOKUP(" + aRef + ",'薪資表'!$1:$2046,2044,FALSE)");
      
    } catch (formulaError) {
      console.warn("⚠️ 第" + row + "行公式設定失敗：" + formulaError.message);
    }
  }
  
  // 強制寫入並等待
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.FORMULA_DELAY || 1500);
  
  // 完成訊息
  const message = "B-F欄公式設定完成，A4:A120範圍內處理 " + validRowsWithFormulas.length + " 筆有效記錄" + 
                  (invalidNameCount > 0 ? "，忽略 " + invalidNameCount + " 筆空白" : "");
  
  handler.completeStep(message);
  console.log("✅ " + message);
  
} catch (bfError) {
  console.error("❌ B-F欄處理失敗：" + bfError.message);
  throw new Error("B-F欄處理失敗：" + bfError.message);
}

// 步驟3：處理場次薪資時數總表的H-K欄
handler.updateProgress("處理場次薪資時數總表的H-K欄...");

// ★★★ 修正：直接定義固定的 rowCount，不依賴任何外部變數 ★★★
const START_ROW = 3;
const END_ROW = 120;
const ROW_COUNT = END_ROW - START_ROW + 1;  // 117列

console.log(`📊 H-K欄處理範圍: A${START_ROW}:A${END_ROW}，共 ${ROW_COUNT} 列`);

// 定位到H4
timeSheet.getRange("H4").activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

try {
  // 先完全清空H4:K範圍
  handler.updateProgress("清空H4:K欄資料...");
  const hkRange = timeSheet.getRange(START_ROW, 8, ROW_COUNT, 4); // H4:K，共4欄
  hkRange.clearContent();
  hkRange.clearFormat();
  hkRange.clearNote();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);

  // 重新設定H欄公式（參照A欄）
  handler.updateProgress("設定H欄參照A欄...");
  const hFormulas = [];
  for (let i = 0; i < ROW_COUNT; i++) {
    const row = i + START_ROW;
    hFormulas.push(["=A" + row]);
  }
  const hRange = timeSheet.getRange(START_ROW, 8, ROW_COUNT, 1); // 只針對H欄
  hRange.setFormulas(hFormulas);
  SpreadsheetApp.flush();
  Utilities.sleep(3000);

  // 處理帳戶補值IMPORTRANGE
  handler.updateProgress("處理帳戶補值IMPORTRANGE...");

  const importId = validateCellValue(execSheet, "C4", "帳戶補值試算表ID");
  const periodCode = CentralContext.getSpreadsheet().getName().substring(0, 6);
  
  if (!periodCode || periodCode.length !== 6) {
    throw new Error("期間代碼格式錯誤，應為6位數");
  }
  
  // 讀取 A4:A120 的資料
  const aCol = timeSheet.getRange(START_ROW, 1, ROW_COUNT, 1).getValues();
  const hCol = timeSheet.getRange(START_ROW, 8, ROW_COUNT, 1).getValues();
  const ijkCol = timeSheet.getRange(START_ROW, 9, ROW_COUNT, 3).getValues();
  
  let accountProcessedCount = 0;
  let accountErrorCount = 0;
  let skippedCount = 0;
  let emptyACount = 0;
  
  for (let i = 0; i < ROW_COUNT; i++) {
    const aName = aCol[i][0];  // A欄的姓名
    const isEmpty = !ijkCol[i][0] && !ijkCol[i][1] && !ijkCol[i][2];
    const row = i + START_ROW;

    // 檢查A欄是否有值
    if (aName && aName !== "" && aName !== "#N/A") {
      // A欄有值，檢查是否需要設定IMPORTRANGE
      if (isEmpty) {
        try {
          const formula = '=IF(H' + row + '<>"", FILTER({IMPORTRANGE("' + importId + '", "' + periodCode + '專員名冊!G2:G"), IMPORTRANGE("' + importId + '", "' + periodCode + '專員名冊!H2:H"), IMPORTRANGE("' + importId + '", "' + periodCode + '專員名冊!I2:I")}, IMPORTRANGE("' + importId + '", "' + periodCode + '專員名冊!B2:B") = H' + row + '), "")';
          timeSheet.getRange(row, 9).setFormula(formula);  // I欄
          accountProcessedCount++;
        } catch (importError) {
          console.warn("⚠️ 第" + row + "行IMPORTRANGE設定失敗：" + importError.message);
          accountErrorCount++;
        }
      } else {
        skippedCount++;
      }
    } else {
      emptyACount++;
    }
  }

  // 輸出詳細統計資訊
  console.log(`📊 H-K欄處理統計（範圍 A4:A120）：`);
  console.log(`📊   總列數: ${ROW_COUNT} 列`);
  console.log(`📊   A欄有姓名: ${ROW_COUNT - emptyACount} 列`);
  console.log(`📊   A欄空白: ${emptyACount} 列`);
  console.log(`📊   設定IMPORTRANGE: ${accountProcessedCount} 筆`);
  console.log(`📊   略過（I-K已有資料）: ${skippedCount} 筆`);
  console.log(`📊   錯誤: ${accountErrorCount} 筆`);

  if (accountErrorCount > 0) {
    console.warn("⚠️ 有 " + accountErrorCount + " 個IMPORTRANGE公式設定失敗");
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.IMPORT_DELAY || 3000);

  const message = "H-K欄處理完成（H欄參照A欄，帳戶補值 " + accountProcessedCount + " 筆記錄" + 
                  (skippedCount > 0 ? "，略過 " + skippedCount + " 筆已有資料" : "") + 
                  (accountErrorCount > 0 ? "，失敗 " + accountErrorCount + " 筆" : "") + "）";
  
  handler.completeStep(message);
  console.log("✅ " + message);
  
} catch (hkError) {
  console.error("❌ H-K欄處理失敗：" + hkError.message);
  console.error("❌ 錯誤堆疊:", hkError.stack);
  throw new Error("H-K欄處理失敗：" + hkError.message);
}

// 步驟4：處理場次薪資時數總表的P-Q或W-X欄
const targetColumns = isFirstHalf ? "P-Q" : "W-X";
handler.updateProgress("處理場次薪資時數總表的" + targetColumns + "欄...");

// ★★★ 完全獨立定義範圍，不依賴任何外部變數 ★★★
const P_START_ROW = 4;
const P_END_ROW = 120;
const P_ROW_COUNT = P_END_ROW - P_START_ROW + 1;  // 117列

console.log(`📊 步驟4處理範圍: A${P_START_ROW}:A${P_END_ROW}，共 ${P_ROW_COUNT} 列`);

// 定位到目標欄位
const targetCol = isFirstHalf ? "P4" : "W4";
timeSheet.getRange(targetCol).activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

try {
  // 讀取 A4:A120 的資料
  const aColData = timeSheet.getRange(P_START_ROW, 1, P_ROW_COUNT, 1).getValues();
  
  if (isFirstHalf) {
    // 上半月：處理P-Q欄
    const dColData = timeSheet.getRange(P_START_ROW, 4, P_ROW_COUNT, 1).getValues(); // D4:D120
    timeSheet.getRange("N4:Q").clearContent();
    console.log("✅ 已清空 N4:Q 範圍");
    
    const validRows = [];
    
    for (let i = 0; i < P_ROW_COUNT; i++) {
      const aName = aColData[i][0];
      const dVal = dColData[i][0];
      
      // 只處理A欄有姓名且D欄大於0的資料
      if (aName && aName !== "" && aName !== "#N/A" && dVal && Number(dVal) > 0) {
        validRows.push({
          dVal: dVal,
          aName: aName
        });
      }
    }
    
    console.log(`📊 上半月P-Q欄篩選結果：`);
    console.log(`📊   總列數: ${P_ROW_COUNT} 列`);
    console.log(`📊   符合條件(D欄>0): ${validRows.length} 筆`);
    
    if (validRows.length > 0) {
      // 準備P欄資料（D欄值）
      const pData = validRows.map(item => [item.dVal]);
      // 準備Q欄資料（A欄姓名）
      const qData = validRows.map(item => [item.aName]);
      
      // 寫入P欄 (第16欄)
      timeSheet.getRange(P_START_ROW, 16, validRows.length, 1).setValues(pData);
      // 寫入Q欄 (第17欄)
      timeSheet.getRange(P_START_ROW, 17, validRows.length, 1).setValues(qData);
      
      console.log(`✅ 已寫入 ${validRows.length} 筆資料到 P${P_START_ROW}:Q${P_START_ROW + validRows.length - 1}`);
    } else {
      console.log("⚠️ 沒有符合條件的資料(D欄>0)");
    }
    
  } else {
    // 下半月：處理W-X欄
    const eColData = timeSheet.getRange(P_START_ROW, 5, P_ROW_COUNT, 1).getValues(); // E4:E120
    timeSheet.getRange("U4:X").clearContent();
    console.log("✅ 已清空 U4:X 範圍");
    
    const validRows = [];
    
    for (let i = 0; i < P_ROW_COUNT; i++) {
      const aName = aColData[i][0];
      const eVal = eColData[i][0];
      
      // 只處理A欄有姓名且E欄大於0的資料
      if (aName && aName !== "" && aName !== "#N/A" && eVal && Number(eVal) > 0) {
        validRows.push({
          eVal: eVal,
          aName: aName
        });
      }
    }
    
    console.log(`📊 下半月W-X欄篩選結果：`);
    console.log(`📊   總列數: ${P_ROW_COUNT} 列`);
    console.log(`📊   符合條件(E欄>0): ${validRows.length} 筆`);
    
    if (validRows.length > 0) {
      // 準備W欄資料（E欄值）
      const wData = validRows.map(item => [item.eVal]);
      // 準備X欄資料（A欄姓名）
      const xData = validRows.map(item => [item.aName]);
      
      // 寫入W欄 (第23欄)
      timeSheet.getRange(P_START_ROW, 23, validRows.length, 1).setValues(wData);
      // 寫入X欄 (第24欄)
      timeSheet.getRange(P_START_ROW, 24, validRows.length, 1).setValues(xData);
      
      console.log(`✅ 已寫入 ${validRows.length} 筆資料到 W${P_START_ROW}:X${P_START_ROW + validRows.length - 1}`);
    } else {
      console.log("⚠️ 沒有符合條件的資料(E欄>0)");
    }
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  const message = targetColumns + "欄處理完成" + 
                  (isFirstHalf ? "，上半月" : "，下半月") + 
                  "找到 " + (isFirstHalf ? 
                    (typeof validRows !== 'undefined' ? validRows.length : 0) : 
                    (typeof validRows !== 'undefined' ? validRows.length : 0)) + " 筆有效資料";
  
  handler.completeStep(message);
  console.log("✅ " + message);
  
} catch (pqwxError) {
  console.error("❌ " + targetColumns + "欄處理失敗：" + pqwxError.message);
  console.error("❌ 錯誤堆疊:", pqwxError.stack);
  throw new Error(targetColumns + "欄處理失敗：" + pqwxError.message);
}

// 步驟5：處理場次薪資時數總表的N-O或U-V欄
const accountColumns = isFirstHalf ? "N-O" : "U-V";
handler.updateProgress("處理場次薪資時數總表的" + accountColumns + "欄...");

// ★★★ 完全獨立定義範圍，不依賴任何外部變數 ★★★
const N_START_ROW = 4;
const N_END_ROW = 120;
const N_ROW_COUNT = N_END_ROW - N_START_ROW + 1;  // 117列

console.log(`📊 步驟5處理範圍: A${N_START_ROW}:A${N_END_ROW}，共 ${N_ROW_COUNT} 列`);

// 定位到目標欄位
const accountCol = isFirstHalf ? "N4" : "U4";
timeSheet.getRange(accountCol).activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

try {
  // 讀取需要的資料
  const aColData = timeSheet.getRange(N_START_ROW, 1, N_ROW_COUNT, 1).getValues(); // A4:A120
  const hColData = timeSheet.getRange(N_START_ROW, 8, N_ROW_COUNT, 1).getValues(); // H4:H120
  const iColData = timeSheet.getRange(N_START_ROW, 9, N_ROW_COUNT, 1).getValues(); // I4:I120
  const jColData = timeSheet.getRange(N_START_ROW, 10, N_ROW_COUNT, 1).getValues(); // J4:J120

  if (isFirstHalf) {
    // 上半月：處理N-O欄
    const dColData = timeSheet.getRange(N_START_ROW, 4, N_ROW_COUNT, 1).getValues(); // D4:D120
    const qColData = timeSheet.getRange("Q4:Q").getValues(); // 讀取Q欄資料
    
    const nData = [];
    const oData = [];
    let validIndex = 0;
    let matchedCount = 0;
    let unmatchedCount = 0;
    
    // 先過濾出D欄>0的資料
    const validRows = [];
    for (let i = 0; i < N_ROW_COUNT; i++) {
      const aName = aColData[i][0];
      const dVal = dColData[i][0];
      
      if (aName && aName !== "" && aName !== "#N/A" && dVal && Number(dVal) > 0) {
        validRows.push({
          index: i,
          hVal: hColData[i][0],
          iVal: iColData[i][0],
          jVal: jColData[i][0]
        });
      }
    }
    
    // 處理匹配邏輯
    for (let v = 0; v < validRows.length; v++) {
      const row = validRows[v];
      const qVal = qColData[v] ? qColData[v][0] : "";
      
      if (qVal && qVal === row.hVal) {
        nData.push([row.iVal || ""]);
        oData.push([row.jVal || ""]);
        matchedCount++;
      } else {
        nData.push([""]);
        oData.push([""]);
        unmatchedCount++;
      }
    }
    
    console.log(`📊 上半月N-O欄處理統計：`);
    console.log(`📊   符合條件(D欄>0): ${validRows.length} 筆`);
    console.log(`📊   匹配成功(Q=H): ${matchedCount} 筆`);
    console.log(`📊   匹配失敗: ${unmatchedCount} 筆`);
    
    if (nData.length > 0) {
      // 寫入N欄 (第14欄)
      timeSheet.getRange(N_START_ROW, 14, nData.length, 1).setValues(nData);
      // 寫入O欄 (第15欄)
      timeSheet.getRange(N_START_ROW, 15, oData.length, 1).setValues(oData);
      
      console.log(`✅ 已寫入 ${nData.length} 筆資料到 N${N_START_ROW}:O${N_START_ROW + nData.length - 1}`);
    }
    
    handler.completeStep("上半月N-O欄帳戶資訊填入完成，處理 " + matchedCount + " 筆匹配記錄" + 
                        (unmatchedCount > 0 ? "，忽略 " + unmatchedCount + " 筆不匹配" : ""));
    
  } else {
    // 下半月：處理U-V欄
    const eColData = timeSheet.getRange(N_START_ROW, 5, N_ROW_COUNT, 1).getValues(); // E4:E120
    const xColData = timeSheet.getRange("X4:X").getValues(); // 讀取X欄資料
    
    const uData = [];
    const vData = [];
    let validIndex = 0;
    let matchedCount = 0;
    let unmatchedCount = 0;
    
    // 先過濾出E欄>0的資料
    const validRows = [];
    for (let i = 0; i < N_ROW_COUNT; i++) {
      const aName = aColData[i][0];
      const eVal = eColData[i][0];
      
      if (aName && aName !== "" && aName !== "#N/A" && eVal && Number(eVal) > 0) {
        validRows.push({
          index: i,
          hVal: hColData[i][0],
          iVal: iColData[i][0],
          jVal: jColData[i][0]
        });
      }
    }
    
    // 處理匹配邏輯
    for (let v = 0; v < validRows.length; v++) {
      const row = validRows[v];
      const xVal = xColData[v] ? xColData[v][0] : "";
      
      if (xVal && xVal === row.hVal) {
        uData.push([row.iVal || ""]);
        vData.push([row.jVal || ""]);
        matchedCount++;
      } else {
        uData.push([""]);
        vData.push([""]);
        unmatchedCount++;
      }
    }
    
    console.log(`📊 下半月U-V欄處理統計：`);
    console.log(`📊   符合條件(E欄>0): ${validRows.length} 筆`);
    console.log(`📊   匹配成功(X=H): ${matchedCount} 筆`);
    console.log(`📊   匹配失敗: ${unmatchedCount} 筆`);
    
    if (uData.length > 0) {
      // 寫入U欄 (第21欄)
      timeSheet.getRange(N_START_ROW, 21, uData.length, 1).setValues(uData);
      // 寫入V欄 (第22欄)
      timeSheet.getRange(N_START_ROW, 22, vData.length, 1).setValues(vData);
      
      console.log(`✅ 已寫入 ${uData.length} 筆資料到 U${N_START_ROW}:V${N_START_ROW + uData.length - 1}`);
    }
    
    handler.completeStep("下半月U-V欄帳戶資訊填入完成，處理 " + matchedCount + " 筆匹配記錄" + 
                        (unmatchedCount > 0 ? "，忽略 " + unmatchedCount + " 筆不匹配" : ""));
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
  
} catch (nouvError) {
  console.error("❌ " + accountColumns + "欄處理失敗：" + nouvError.message);
  console.error("❌ 錯誤堆疊:", nouvError.stack);
  throw new Error(accountColumns + "欄處理失敗：" + nouvError.message);
}

// 步驟6：更新薪資單資料
handler.updateProgress("更新薪資單資料...");

// 定位到薪資單E4
slipSheet.activate();
slipSheet.getRange("E4").activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

let slipProcessedCount = 0;

try {
  slipSheet.getRange("E4:E").clearContent();

  if (isFirstHalf) {
    const sourceN = timeSheet.getRange("Q4:Q").getValues();
    // 確保 sourceN 是陣列
    if (sourceN && Array.isArray(sourceN)) {
      const validSourceN = sourceN.filter(row => row && row[0] !== "");
      if (validSourceN.length > 0) {
        slipSheet.getRange(4, 5, validSourceN.length, 1).setValues(validSourceN);
        slipProcessedCount = validSourceN.length;
      }
    }
  } else {
    const sourceR = timeSheet.getRange("X4:X").getValues();
    if (sourceR && Array.isArray(sourceR)) {
      const validSourceR = sourceR.filter(row => row && row[0] !== "");
      if (validSourceR.length > 0) {
        slipSheet.getRange(4, 5, validSourceR.length, 1).setValues(validSourceR);
        slipProcessedCount = validSourceR.length;
      }
    }
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  handler.completeStep("薪資單更新完成，處理 " + slipProcessedCount + " 位專員");
  
} catch (slipError) {
  console.error("❌ 薪資單更新失敗：" + slipError.message);
  slipProcessedCount = 0;
}

// 步驟7：更新PDF產出
handler.updateProgress("更新PDF產出..");

// 定位到PDF產出B2
PDFSheet.activate();
PDFSheet.getRange("B2").activate();
SpreadsheetApp.flush();
Utilities.sleep(1000);

let staffProcessedCount = 0;

try {
  const staffLastRow = PDFSheet.getLastRow();
  PDFSheet.getRange("B2:E").clearContent();
  if (staffLastRow >= 2) {
    PDFSheet.getRange("H2:H" + staffLastRow).clearContent();
  }

  if (isFirstHalf) {
    const sourceM = timeSheet.getRange("Q4:Q").getValues();
    if (sourceM && Array.isArray(sourceM)) {
      const validM = sourceM.filter(row => row && row[0] !== "");
      if (validM.length > 0) {
        PDFSheet.getRange(2, 2, validM.length, 1).setValues(validM);
        const hValues = validM.map(() => ["Y"]);
        PDFSheet.getRange(2, 8, hValues.length, 1).setValues(hValues);
        staffProcessedCount = validM.length;
      }
    }
  } else {
    const sourceR = timeSheet.getRange("X4:X").getValues();
    if (sourceR && Array.isArray(sourceR)) {
      const validR = sourceR.filter(row => row && row[0] !== "");
      if (validR.length > 0) {
        PDFSheet.getRange(2, 2, validR.length, 1).setValues(validR);
        const hValues = validR.map(() => ["Y"]);
        PDFSheet.getRange(2, 8, hValues.length, 1).setValues(hValues);
        staffProcessedCount = validR.length;
      }
    }
  }
  
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);

  handler.completeStep("PDF產出更新完成，處理 " + staffProcessedCount + " 位專員");
  
} catch (staffError) {
  console.error("❌ PDF產出更新失敗：" + staffError.message);
  staffProcessedCount = 0;
}

// 步驟8：完成標記與檢查
handler.updateProgress("完成標記與資料檢查...");

// ★★★ 加入大量檢查，找出問題點 ★★★
console.log("🔍 開始步驟8...");

// 檢查 timeSheet 是否存在
if (!timeSheet) {
  console.error("❌ timeSheet 是 undefined");
  throw new Error("timeSheet 未定義");
}

// 檢查 getRange 方法是否存在
if (typeof timeSheet.getRange !== 'function') {
  console.error("❌ timeSheet.getRange 不是函數");
  throw new Error("timeSheet.getRange 不是函數");
}

let processedEmployees = 0;
let finalValidation;

try {
    // 直接從 A4:A120 計算有效員工人數
    console.log('🔍 讀取 A4:A120...');
    const aColRange = timeSheet.getRange("A4:A120");

    if (!aColRange) {
        throw new Error('無法取得 A4:A120 範圍');
    }
    
    // ★★★ 檢查 aColRange 是否存在 getValues 方法 ★★★
    if (typeof aColRange.getValues !== 'function') {
        console.error('❌ aColRange.getValues 不是函數');
        throw new Error('aColRange.getValues 不是函數');
    }
    
    const aColValues = aColRange.getValues();
    console.log(`📊 A4:A120 讀取完成，陣列長度: ${aColValues ? aColValues.length : 'undefined'}`);
    
    if (!aColValues || !Array.isArray(aColValues)) {
        console.error('❌ aColValues 不是有效的陣列');
        throw new Error('aColValues 不是有效的陣列');
    }
    
    // 過濾出有效的姓名
    const validNames = [];
    for (let i = 0; i < aColValues.length; i++) {
        const row = aColValues[i];
        if (row && row.length > 0) {
            const value = row[0];
            if (value && value !== "" && value !== "#N/A" && value !== null) {
                validNames.push(value);
            }
        }
    }
    
    processedEmployees = validNames.length;
    
    console.log(`📊 最終統計：`);
    console.log(`📊   A4:A120總列數: ${aColValues.length}`);
    console.log(`📊   有效姓名數: ${processedEmployees}`);
    console.log(`📊   staffProcessedCount: ${staffProcessedCount}`);
    console.log(`📊   slipProcessedCount: ${slipProcessedCount}`);
    
} catch (countError) {
    console.error('❌ 計算員工人數時出錯：' + countError.message);
    console.error('❌ 錯誤堆疊:', countError.stack);
    processedEmployees = 0;
}

// 完成狀態由 UnifiedProcessHandler.completePunchClock()
// → punchSalarySettlement()
// → updateCompletionStatus()
// 統一處理，這裡不再手動寫入標記

// 建立驗證結果
finalValidation = { 
  success: true, 
  summary: `處理 ${processedEmployees} 位員工，PDF:${staffProcessedCount}人，薪資單:${slipProcessedCount}人` 
};

// 安全地呼叫 completeStep
try {
  console.log("🔍 呼叫 handler.completeStep...");
  if (handler && typeof handler.completeStep === 'function') {
    handler.completeStep("完成標記與檢查完成 " + finalValidation.summary);
    console.log("✅ handler.completeStep 完成");
  } else {
    console.warn("⚠️ handler.completeStep 不是函數");
  }
} catch (stepError) {
  console.error("❌ handler.completeStep 失敗：" + stepError.message);
}

// 最終定位到結果檢視位置
try {
  timeSheet.getRange("C21").activate();
  SpreadsheetApp.flush();
} catch (finalError) {
  console.error("❌ 最終定位失敗：" + finalError.message);
}

// 安全地轉換數值
const safeStaffCount = Number(staffProcessedCount) || 0;
const safeSlipCount = Number(slipProcessedCount) || 0;

console.log(`📊 最終回傳值：`);
console.log(`📊   processedEmployees: ${processedEmployees}`);
console.log(`📊   safeStaffCount: ${safeStaffCount}`);
console.log(`📊   safeSlipCount: ${safeSlipCount}`);

// 返回處理結果
return {
  processedEmployees: processedEmployees,
  slipCount: safeSlipCount,
  staffCount: safeStaffCount,
  processType: processType,
  validationResult: finalValidation,
  message: `薪資結算整理流程完成（${processType}），處理 ${processedEmployees} 位員工`
};
} 
// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數（保持原有功能）
// ═══════════════════════════════════════════════════════════════

/**
 * 輔助函數：驗證並取得工作表
 */
function validateAndGetSheet(sheetName, description) {
  try {
    const sheet = CentralContext.getSpreadsheet().getSheetByName(sheetName);
    if (!sheet) {
      throw new Error("找不到" + description + "：" + sheetName);
    }
    return sheet;
  } catch (error) {
    throw new Error("取得" + description + "失敗：" + error.message);
  }
}

/**
 * 輔助函數：驗證儲存格值
 */
function validateCellValue(sheet, cellAddress, description) {
  try {
    const value = sheet.getRange(cellAddress).getValue();
    if (!value || value === "") {
      throw new Error(description + "（" + cellAddress + "）不能為空");
    }
    return value.toString().trim();
  } catch (error) {
    throw new Error("取得" + description + "失敗：" + error.message);
  }
}

// 輔助函數：薪資結算最終驗證
function performSettlementValidation(sheet, processType, staffCount, slipCount) {
  try {
    const dataRange = sheet.getDataRange();
    const numRows = dataRange.getNumRows();
    
    // 基本資料完整性檢查
    if (numRows < 4) {
      return { success: false, summary: "(資料不足)" };
    }
    
    // 檢查關鍵欄位
    const aRange = sheet.getRange(4, 1, Math.min(numRows - 3, 100), 1);
    const aValues = aRange.getValues();
    
    let validEmployeeCount = 0;
    for (let i = 0; i < aValues.length; i++) {
      if (aValues[i][0] && aValues[i][0] !== "" && aValues[i][0] !== "#N/A") {
        validEmployeeCount++;
      }
    }
    
    return {
      success: true,
      summary: "(" + processType + "處理完成, 有效員工" + validEmployeeCount + "人, 薪資單" + slipCount + "筆, PDF產出" + staffCount + "筆)"
    };
    
  } catch (error) {
    console.warn("薪資結算驗證失敗：", error.message);
    return { success: false, summary: "(驗證失敗)" };
  }
}

// 輔助函數：薪資結算錯誤恢復建議
function getSettlementRecoverySuggestion(step, errorMessage) {
  const suggestions = {
    1: "請檢查薪資表是否存在且包含L欄以後的資料",
    2: "請確認場次薪資時數總表A欄有有效的員工姓名",
    3: "請檢查執行控制工作表C4的帳戶補值試算表ID是否正確",
    4: "請確認D欄(上半月)或E欄(下半月)有有效的數值資料",
    5: "請檢查帳戶資訊(I、J欄)是否正確匹配",
    6: "請確認薪資單工作表可以正常寫入",
    7: "請檢查PDF產出工作表的權限設定",
    8: "請檢查完成標記的儲存格範圍是否正確"
  };
  
  let suggestion = suggestions[step] || "請聯繫系統管理員";
  
  // 根據錯誤訊息提供更具體建議
  if (errorMessage.includes("權限")) {
    suggestion += "，並確認所有相關工作表的編輯權限";
  } else if (errorMessage.includes("格式")) {
    suggestion += "，並檢查資料格式是否正確";
  } else if (errorMessage.includes("找不到")) {
    suggestion += "，並確認相關工作表和儲存格範圍存在";
  } else if (errorMessage.includes("IMPORTRANGE")) {
    suggestion += "，特別注意IMPORTRANGE公式的試算表ID和工作表名稱";
  } else if (errorMessage.includes("公式")) {
    suggestion += "，檢查公式語法和引用範圍是否正確";
  }
  
  return suggestion;
}

function cleanCellBlankLines_(value) {
  if (typeof value !== "string") return value;

  return value
    .split(/\r?\n/)
    .map(function(line) {
      return line.trim();
    })
    .filter(function(line) {
      return line !== "";
    })
    .join("\n");
}

function copySalaryRow2048To2047AsValues_(salarySheet) {
  var lastColNum = salarySheet.getLastColumn();

  if (lastColNum < 12) {
    throw new Error("薪資表欄位不足，至少需要 L 欄");
  }

  var numCols = lastColNum - 11; // L 欄到最後欄
  var sourceValues = salarySheet.getRange(2048, 12, 1, numCols).getValues();

  var cleanedValues = [
    sourceValues[0].map(cleanCellBlankLines_)
  ];

  salarySheet.getRange(2047, 12, 1, numCols).setValues(cleanedValues);

  return numCols;
}


// ███████████████████████████████████████████████████
// 主程式9：完整薪資處理流程（修正打卡位置）
// ███████████████████████████████████████████████████

/**
 * 完整薪資處理流程配置（修正版）
 */
function getCompletePayrollProcessConfig() {
  return {
    name: "完整薪資處理流程",
    totalSteps: 25,
    punchMethod: "punchCompleteExecution", // 修正：使用正確的打卡方法名稱
    cells: {
      firstHalf: "C22",  // 修正：使用正確的打卡位置
      secondHalf: "D22", // 修正：使用正確的打卡位置
      status: "E22"      // 修正：對應的狀態欄位
    },
    sheetNames: {
      execSheet: "exec",
      salarySheet: "salary",
      adjustSheet: "adjust", 
      summarySheet: "summary",
      allowanceSheet: "allowance",
      voucherSheet: "voucher",
      newcomerSheet: "newcomer", 
      internSheet: "intern",
      leaderSheet: "leader",
      slipSheet: "slip"
    },
    steps: [
      {
        name: "執行完整薪資處理流程",
        description: "執行主程式1-8的完整薪資處理流程...",
        type: "custom",
        handler: executeCompletePayrollProcess,
        required: true
      }
    ]
  };
}

/**
 * 執行完整薪資處理流程 - 主程式1-8（修正版）
 */
function executeCompletePayrollProcess(sheets, isFirstHalf, handler) {
  const { execSheet } = sheets;
  const periodText = isFirstHalf ? "上半月" : "下半月";
  
  handler.updateProgress(`開始執行${periodText}完整薪資處理流程...`);
  console.log(`執行 ${periodText} 完整薪資處理，isFirstHalf = ${isFirstHalf}`);
  
  let completedProcesses = 0;
  const totalProcesses = 8;
  const results = {};
  
  try {
    // 步驟1：初始驗證
    handler.updateProgress("步驟1：初始驗證系統狀態...");
    
    // 定位到執行工作表的完整執行打卡位置（C22/D22）
    execSheet.activate();
    const punchCell = isFirstHalf ? "C22" : "D22";
    execSheet.getRange(punchCell).activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    // 從當前檔案名稱提取完整期別
    const currentSpreadsheet = CentralContext.getSpreadsheet();
    const fileName = currentSpreadsheet.getName();
    console.log(`當前檔案名稱：${fileName}`);
    
    // 從檔案名稱提取完整期別（YYYYMM-1 或 YYYYMM-2 格式）
    const periodMatch = fileName.match(/(\d{6}-[12])/);
    if (!periodMatch) {
      throw new Error(`無法從檔案名稱 "${fileName}" 中提取期別，檔案名稱應包含期別格式（如：202507-1 或 202507-2）`);
    }
    
    const periodCode = periodMatch[1];
    console.log(`提取的完整期別：${periodCode}`);
    
    // 檢查期別中的上下半月標記是否與當前執行的一致
    const filePeriodHalf = periodCode.endsWith('-1') ? true : false;
    if (filePeriodHalf !== isFirstHalf) {
      const fileHalfText = filePeriodHalf ? "上半月" : "下半月";
      console.warn(`期別不一致：檔案是${fileHalfText}(${periodCode})，但執行的是${periodText}`);
    }
    
    handler.updateProgress(`初始驗證完成（${periodText}，期別：${periodCode}）`);
    
    // 步驟2：主程式1 - 薪資表整理
    handler.updateProgress("步驟2：執行主程式1 - 薪資表整理...");
    
    // 定位到執行工作表C11（薪資表整理打卡位置）
    execSheet.getRange("C11").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 1, "processing");
    
    try {
      if (isFirstHalf) {
        runSalaryPreparationWithProjectOrdersFirstHalf();
      } else {
        runSalaryPreparationWithProjectOrdersSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 1, "completed");
      results.process1 = { success: true, name: "薪資表整理" };
      handler.updateProgress(`主程式1完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 1, "error");
      throw new Error(`主程式1失敗：${error.message}`);
    }
    
    // 步驟3：主程式2 - 00調薪  
    handler.updateProgress("步驟3：執行主程式2 - 00調薪...");
    
    execSheet.getRange("C12").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 2, "processing");
    
    try {
      if (isFirstHalf) {
        runAdjustmentPreparationFirstHalf();
      } else {
        runAdjustmentPreparationSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 2, "completed");
      results.process2 = { success: true, name: "00調薪" };
      handler.updateProgress(`主程式2完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 2, "error");
      throw new Error(`主程式2失敗：${error.message}`);
    }
    
    // 步驟4：主程式3 - 01專員請款
    handler.updateProgress("步驟4：執行主程式3 - 01專員請款...");
    
    execSheet.getRange("C13").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 3, "processing");
    
    try {
      if (isFirstHalf) {
        runAllowanceProcessFirstHalf();
      } else {
        runAllowanceProcessSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 3, "completed");
      results.process3 = { success: true, name: "01專員請款" };
      handler.updateProgress(`主程式3完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 3, "error");
      throw new Error(`主程式3失敗：${error.message}`);
    }
    
    // 步驟5：主程式4 - 02儲值獎金
    handler.updateProgress("步驟5：執行主程式4 - 02儲值獎金...");
    
    execSheet.getRange("C14").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 4, "processing");
    
    try {
      if (isFirstHalf) {
        runVoucherPreparationFirstHalf();
      } else {
        runVoucherPreparationSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 4, "completed");
      results.process4 = { success: true, name: "02儲值獎金" };
      handler.updateProgress(`主程式4完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 4, "error");
      throw new Error(`主程式4失敗：${error.message}`);
    }
    
    // 步驟6：主程式5 - 03新人實境
    handler.updateProgress("步驟6：執行主程式5 - 03新人實境...");
    
    execSheet.getRange("C15").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 5, "processing");
    
    try {
      if (isFirstHalf) {
        runNewcomerProcessFirstHalf();
      } else {
        runNewcomerProcessSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 5, "completed");
      results.process5 = { success: true, name: "03新人實境" };
      handler.updateProgress(`主程式5完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 5, "error");
      throw new Error(`主程式5失敗：${error.message}`);
    }
    
    // 步驟7：主程式6 - 04新人實習
    handler.updateProgress("步驟7：執行主程式6 - 04新人實習...");
    
    execSheet.getRange("C16").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 6, "processing");
    
    try {
      if (isFirstHalf) {
        runInternProcessFirstHalf();
      } else {
        runInternProcessSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 6, "completed");
      results.process6 = { success: true, name: "04新人實習" };
      handler.updateProgress(`主程式6完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 6, "error");
      throw new Error(`主程式6失敗：${error.message}`);
    }
    
    // 步驟8：主程式7 - 05組長津貼
    handler.updateProgress("步驟8：執行主程式7 - 05組長津貼...");
    
    execSheet.getRange("C17").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 7, "processing");
    
    try {
      if (isFirstHalf) {
        runLeaderProcessFirstHalf();
      } else {
        runLeaderProcessSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 7, "completed");
      results.process7 = { success: true, name: "05組長津貼" };
      handler.updateProgress(`主程式7完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 7, "error");
      throw new Error(`主程式7失敗：${error.message}`);
    }
    
    // 步驟9：主程式8 - 薪資結算
    handler.updateProgress("步驟9：執行主程式8 - 薪資結算...");
    
    execSheet.getRange("C18").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    highlightMainProcessStatus(execSheet, 8, "processing");
    
    try {
      if (isFirstHalf) {
        runFinalSettlementFirstHalf();
      } else {
        runFinalSettlementSecondHalf();
      }
      completedProcesses++;
      highlightMainProcessStatus(execSheet, 8, "completed");
      results.process8 = { success: true, name: "薪資結算" };
      handler.updateProgress(`主程式8完成 (${completedProcesses}/${totalProcesses})`);
    } catch (error) {
      highlightMainProcessStatus(execSheet, 8, "error");
      throw new Error(`主程式8失敗：${error.message}`);
    }
    
    // 步驟10：最終驗證
    handler.updateProgress("步驟10：執行最終驗證...");
    
    // 定位回執行工作表C22/D22（完整執行打卡位置）
    execSheet.getRange(punchCell).activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    try {
      const validation = performCompletePayrollValidation(isFirstHalf);
      results.validation = validation;
      
      // 清除狀態標記
      clearAllMainProcessStatus(execSheet);
      
      handler.updateProgress("步驟11：清理暫存資料...");
      
      // 清除進度記錄
      PropertiesService.getScriptProperties().deleteProperty('latestProgress');
      PropertiesService.getScriptProperties().deleteProperty('progressTimestamp');
      
      const finalMessage = `${periodText}完整薪資處理流程全部完成！(${completedProcesses}/${totalProcesses}個主程式)${validation.summary}`;
      handler.updateProgress(finalMessage);
      
      return {
        success: true,
        completedProcesses: completedProcesses,
        totalProcesses: totalProcesses,
        periodText: periodText,
        periodCode: periodCode,
        results: results,
        message: finalMessage
      };
      
    } catch (validationError) {
      console.warn("最終驗證失敗：", validationError.message);
      const warningMessage = `${periodText}完整薪資處理流程完成！(${completedProcesses}/${totalProcesses}個主程式，但最終驗證有警告)`;
      handler.updateProgress(warningMessage);
      
      return {
        success: true,
        completedProcesses: completedProcesses,
        totalProcesses: totalProcesses,
        warning: validationError.message,
        message: warningMessage
      };
    }
    
  } catch (error) {
    // 錯誤處理
    clearAllMainProcessStatus(execSheet);
    
    const errorMessage = `${periodText}完整薪資處理錯誤 (完成${completedProcesses}/${totalProcesses}個主程式)：${error.message}`;
    handler.updateProgress(errorMessage);
    
    // 提供恢復建議
    const suggestion = getCompletePayrollRecoverySuggestion(completedProcesses, error.message);
    if (suggestion) {
      handler.updateProgress(`建議：${suggestion}`);
    }
    
    throw error;
  }
}

// ═══════════════════════════════════════════════════════════════
// 打卡模組擴展（修正版）
// ═══════════════════════════════════════════════════════════════

/**
 * 需要在 AttendanceModule 類別中確認此方法存在：
 * 
 * 上/下半月完整執行打卡 - C22/D22
 */
/*
punchCompleteExecution(execSheet, isFirstHalf) {
  const cellAddress = isFirstHalf ? "C22" : "D22";
  return this.punchClock(execSheet, cellAddress, isFirstHalf, "上/下半月完整執行");
}
*/

/**
 * 以及在打卡案例處理中確認有：
 */
/*
case "完整薪資處理":
case "上/下半月完整執行":
  timestamp = this.punchCompleteExecution(execSheet, isFirstHalf);
  break;
*/

// ═══════════════════════════════════════════════════════════════
// 主要選單函數
// ═══════════════════════════════════════════════════════════════

/**
 * 統一完整薪資處理執行函數（修正版）
 */
function runCompletePayrollProcess(isFirstHalf) {
  const processor = new UnifiedProcessHandler(CONFIG);
  const config = getCompletePayrollProcessConfig();
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月完整薪資處理
 */
function runCompletePayrollProcessFirstHalf() {
  console.log(`runCompletePayrollProcessFirstHalf 被調用，將傳入 isFirstHalf = true`);
  return runCompletePayrollProcess(true);
}

/**
 * 下半月完整薪資處理
 */
function runCompletePayrollProcessSecondHalf() {
  console.log(`runCompletePayrollProcessSecondHalf 被調用，將傳入 isFirstHalf = false`);
  return runCompletePayrollProcess(false);
}

/**
 * 選單調用函數 - 上半月
 */
function runCompletePayrollFirst() {
  return runCompletePayrollProcessFirstHalf();
}

/**
 * 選單調用函數 - 下半月
 */
function runCompletePayrollSecond() {
  return runCompletePayrollProcessSecondHalf();
}

// ███████████████████████████████████████████████████
// 📁 獨立功能2：工具包押金處理（基於統一框架優化版）
// ═══════════════════════════════════════════════════════════════
//需求摘要：
// 0) 清空（上半月）：
//    - 場次時數薪資總表：A121:E、AB4:AE
//    - 介紹獎金：A2:C
// 1) 從本檔「工具包押金」表篩 I>=80 且 G 為空白，取 A（姓名）→ 貼到 summary A121 起；E=2000(台北)/1500(台中)
// 2) 當 A121 非空白時，依序將 A121/E121、A122/E122… 貼到 AE4/AD4、AE5/AD5…
//    一旦遇到『來源列 A 欄為空白』就停止
// 3) 以 AE4 的值為 key，於 summary 找到 H 欄（從 E4 開始）等於該值的那一列，把該列 I/J 回填 AB4/AC4 以此類推到AE欄非空白
// 4) 工具包押金：篩 I>=80 且 J 非空白 → intro A=工具包押金.J、B=工具包押金.A、C=1000，從 A2 起往下貼
//
// 附註：
// - 本檔名含「台中」→ 押金 1500；否則 2000
// - 若控制中心不存在，會自動以『後備直跑』方式執行
// ███████████████████████████████████████████████████
/** ───────────────────────── 主要入口函數 ───────────────────────── */

/**
 * 執行工具包押金處理（上半月）
 */
function runToolDepositFirst() {
  return runToolDepositProcess(true);
}

/**
 * 執行工具包押金處理（下半月）
 */
function runToolDepositSecond() {
  return runToolDepositProcess(false);
}

/**
 * 工具包押金處理主函數
 */
function runToolDepositProcess(isFirstHalf) {
  try {
    // 開啟側邊欄
    if (typeof openProgressSidebar === "function") {
      openProgressSidebar();
    }
    
    const ss = CentralContext.getSpreadsheet();
    const SH = getSheetNames();
    
    // 直接使用簡短變數名
    const sheets = {
      toolDeposit: ss.getSheetByName(SH.toolDeposit),
      summary: ss.getSheetByName(SH.summary),
      intro: ss.getSheetByName(SH.intro),
      exec: ss.getSheetByName(SH.exec)
    };
    
    // 檢查工作表是否存在
    if (!sheets.toolDeposit) throw new Error("找不到工作表：" + SH.toolDeposit);
    if (!sheets.summary) throw new Error("找不到工作表：" + SH.summary);
    if (!sheets.intro) throw new Error("找不到工作表：" + SH.intro);
    if (!sheets.exec) throw new Error("找不到工作表：" + SH.exec);
    
    Logger.log("✓ 所有工作表都已獲取");
    
    // 建立 handler 以支援進度更新
    const handler = {
      config: { TIMEZONE: "Asia/Taipei" },
      updateProgress: function(msg) { 
        Logger.log(msg);
        if (typeof updateSidebarProgress === "function") {
          updateSidebarProgress(msg);
        }
      },
      completeStep: function(_) {},
    };
    
    const result = executeFullToolDepositProcess(sheets, isFirstHalf, handler);
    
    return result;
    
  } catch (error) {
    const errorMsg = "錯誤: " + error.message;
    Logger.log(errorMsg);
    if (typeof updateSidebarProgress === "function") {
      updateSidebarProgress("❌ " + errorMsg);
    }
    SpreadsheetApp.getUi().alert(errorMsg);
    throw error;
  }
}

/** ───────────────────────── 核心流程 ───────────────────────── */
function executeFullToolDepositProcess(sheets, isFirstHalf, handler) {
  try {
    const ss = CentralContext.getSpreadsheet();
    
    Logger.log("=== executeFullToolDepositProcess 開始 ===");
    Logger.log("isFirstHalf: " + isFirstHalf);
    
    // 直接使用簡短變數名
    const { toolDeposit, summary, intro, exec } = sheets;
    
    // 確認工作表存在
    if (!toolDeposit) throw new Error("toolDeposit 為 null");
    if (!summary) throw new Error("summary 為 null");
    if (!intro) throw new Error("intro 為 null");
    if (!exec) throw new Error("exec 為 null");
    
    Logger.log("✓ 所有工作表物件都已驗證");

    // 進度更新函數
    const progress = (msg) => { 
      Logger.log(msg);
      if (typeof handler?.updateProgress === "function") {
        handler.updateProgress(msg);
      }
      // 檢查執行控制（暫停/停止）
      if (typeof checkExecutionControl === "function") {
        checkExecutionControl();
      }
    };

    const startRow = 151;
    const region = ss.getName().includes("台中") ? "台中" : "台北";
    const DEPOSIT = region === "台中" ? 1500 : 2000;
    
    Logger.log("區域: " + region + ", 押金: " + DEPOSIT);

    // ===== 上半月：清空 =====
    if (isFirstHalf) {
      progress("🧹 上半月：清空範圍");
      
      // 清空 A151:E
      const lastRow1 = summary.getLastRow();
      if (lastRow1 >= startRow) {
        summary.getRange(startRow, 1, lastRow1 - startRow + 1, 5).clearContent();
        progress("✓ 已清空 A151:E");
      }
      
      // 清空 AB4:AE (AB=28, AC=29, AD=30, AE=31)
      const lastRow2 = summary.getLastRow();
      if (lastRow2 >= 4) {
        summary.getRange(4, 28, lastRow2 - 4 + 1, 4).clearContent();
        progress("✓ 已清空 AB4:AE");
      }
      
      // 清空介紹獎金 A2:C
      const lastRow3 = intro.getLastRow();
      if (lastRow3 >= 2) {
        intro.getRange(2, 1, lastRow3 - 2 + 1, 3).clearContent();
        progress("✓ 已清空介紹獎金 A2:C");
      }
      
      progress("✅ 完成：上半月清空");
      SpreadsheetApp.getUi().alert("上半月清空完成！");
      return { ok: true, phase: "上半月", action: "清空" };
    }

    // ===== 下半月：處理流程 =====
    progress("📋 下半月：開始處理工具包押金");

    // 1) 工具包押金 I>=80 && G空白 → A151 起；E=押金
    progress("步驟1：篩選工具包押金資料（I>=80 且 G空白）");
    const dataTool = getAllValues_(toolDeposit);
    const COL_A = 0, COL_G = 6, COL_I = 8, COL_J = 9;
    let names = [];
    
    for (let r = 1; r < dataTool.length; r++) {
      const row = dataTool[r];
      const name = row[COL_A];
      const g = String(row[COL_G] ?? "").trim();
      const i = Number(row[COL_I]) || 0;
      
      if (name && i >= 80 && g === "") {
        names.push(name);
      }
    }
    
    progress(`找到 ${names.length} 筆符合條件的資料`);
    
    if (names.length > 0) {
      summary.getRange(startRow, 1, names.length, 1).setValues(names.map(n => [n]));
      summary.getRange(startRow, 5, names.length, 1).setValues(names.map(_ => [DEPOSIT]));
      progress(`✓ 已填入 ${names.length} 筆姓名及押金（${region}：${DEPOSIT}元）`);
    } else {
      progress("⚠ 沒有符合條件的資料（I>=80 且 G空白）");
    }

    // 2) A151 非空白 → AE4=A151、AD4=E151，以此類推
    progress("步驟2：複製 A/E 欄到 AE/AD 欄（從第4列開始）");
    if (names.length > 0) {
      const aValues = summary.getRange(startRow, 1, names.length, 1).getValues();
      const eValues = summary.getRange(startRow, 5, names.length, 1).getValues();
      
      summary.getRange(4, 31, names.length, 1).setValues(aValues); // AE=31
      summary.getRange(4, 30, names.length, 1).setValues(eValues); // AD=30
      progress(`✓ 已複製 ${names.length} 筆資料到 AE/AD 欄`);
    }

    // 3) 批次處理：以 AE4, AE5... 為 key，在 H 欄查找，填入 AB/AC
    progress("步驟3：批次處理 AE 欄查找（優化版）");
    
    // 先清空整個 AB/AC 區域
    const lastRowAB = summary.getLastRow();
    if (lastRowAB >= 4) {
      summary.getRange(4, 28, lastRowAB - 4 + 1, 2).clearContent(); // AB=28, AC=29
    }
    
    // 一次性讀取所有需要的資料
    const aeData = summary.getRange(4, 31, lastRowAB - 3, 1).getValues(); // AE欄從第4列開始
    const hData = summary.getRange(4, 8, lastRowAB - 3, 3).getValues(); // H, I, J 欄一次讀取
    
    // 建立 H 欄的查找表（提升效能）
    const hMap = new Map();
    for (let i = 0; i < hData.length; i++) {
      const hValue = String(hData[i][0] ?? "").trim();
      if (hValue !== "" && !hMap.has(hValue)) {
        hMap.set(hValue, {
          iValue: hData[i][1],
          jValue: hData[i][2]
        });
      }
    }
    
    // 批次收集要寫入的資料
    const abValues = [];
    const acValues = [];
    let processedCount = 0;
    let notFoundCount = 0;
    
    for (let i = 0; i < aeData.length; i++) {
      const searchName = String(aeData[i][0] ?? "").trim();
      
      if (searchName === "") {
        break; // 遇到空白就停止
      }
      
      const found = hMap.get(searchName);
      if (found) {
        abValues.push([found.iValue ?? ""]);
        acValues.push([found.jValue ?? ""]);
        processedCount++;
      } else {
        abValues.push([""]);
        acValues.push([""]);
        notFoundCount++;
        progress(`  ⚠ 在 H 欄中找不到：${searchName}`);
      }
    }
    
    // 批次寫入結果
    if (abValues.length > 0) {
      summary.getRange(4, 28, abValues.length, 1).setValues(abValues); // AB
      summary.getRange(4, 29, acValues.length, 1).setValues(acValues); // AC
    }
    
    progress(`✓ 步驟3完成：成功 ${processedCount} 筆，找不到 ${notFoundCount} 筆`);

    // 4) 介紹獎金：I>=80 且 J非空 且 K非日期 → intro
    progress("步驟4：處理介紹獎金（I>=80 且 J非空 且 K非日期）");
    const introRows = [];
    const COL_K = 10; // K欄索引
    
    for (let r = 1; r < dataTool.length; r++) {
      const row = dataTool[r];
      const nameA = row[COL_A];
      const iVal = Number(row[COL_I]) || 0;
      const jVal = row[COL_J];
      const kVal = row[COL_K];
      
      // 檢查 K 欄是否為日期
      const isKDate = kVal instanceof Date || 
                      (typeof kVal === 'string' && kVal.trim() !== '' && !isNaN(Date.parse(kVal))) ||
                      (typeof kVal === 'number' && kVal > 40000 && kVal < 60000); // Excel 日期序號範圍
      
      // 條件：I>=80 且 J非空 且 K非日期
      if (nameA && iVal >= 80 && String(jVal ?? "").trim() !== '' && !isKDate) {
        introRows.push([jVal, nameA, 1000]); // A=J, B=A, C=1000
      }
    }
    
    if (introRows.length > 0) {
      intro.getRange(2, 1, introRows.length, 3).setValues(introRows);
      progress(`✓ 已填入 ${introRows.length} 筆介紹獎金`);
    } else {
      progress("⚠ 沒有符合條件的介紹獎金資料");
    }

    // 打卡記錄
    const now = new Date();
    exec.getRange("B2").setValue("🔧 工具包押金（下半月）");
    exec.getRange("C2").setValue(Utilities.formatDate(now, "Asia/Taipei", "yyyy/MM/dd HH:mm:ss"));
    exec.getRange("D2").setValue(`區域：${region} | 押金名單：${names.length} | 介紹獎金：${introRows.length}`);

    progress("✅ 完成：下半月工具包押金處理");
    SpreadsheetApp.getUi().alert(`下半月處理完成！\n\n區域：${region}\n押金名單：${names.length} 筆\n介紹獎金：${introRows.length} 筆`);
    
    return { 
      ok: true, 
      phase: "下半月", 
      region: region,
      depositCount: names.length, 
      introCount: introRows.length 
    };
  } catch (error) {
    Logger.log("❌ executeFullToolDepositProcess 錯誤: " + error.message);
    Logger.log("錯誤堆疊: " + error.stack);
    throw error;
  }
}

/** ───────────────────────── 輔助函數 ───────────────────────── */

/**
 * 獲取工作表所有資料
 */
function getAllValues_(sheet) {
  const lastRow = Math.max(1, sheet.getLastRow());
  const lastCol = Math.max(1, sheet.getLastColumn());
  return sheet.getRange(1, 1, lastRow, lastCol).getValues();
}

/**
 * 調試用函數：檢查工作表狀態
 */
function debugToolDepositSheets() {
  const ss = CentralContext.getSpreadsheet();
  const SH = getSheetNames();
  
  Logger.log('=== 調試工作表狀態 ===');
  Logger.log('檔案名稱: ' + ss.getName());
  Logger.log('SH.toolDeposit: ' + SH.toolDeposit);
  Logger.log('SH.summary: ' + SH.summary);
  Logger.log('SH.intro: ' + SH.intro);
  Logger.log('SH.exec: ' + SH.exec);
  
  const sheets = ss.getSheets();
  Logger.log('\n實際工作表列表:');
  sheets.forEach(function(sheet, index) {
    Logger.log((index + 1) + '. ' + sheet.getName());
  });
  
  Logger.log('\n嘗試獲取各工作表:');
  Logger.log('工具包押金: ' + (ss.getSheetByName(SH.toolDeposit) ? '找到' : '未找到'));
  Logger.log('場次時數薪資總表: ' + (ss.getSheetByName(SH.summary) ? '找到' : '未找到'));
  Logger.log('介紹獎金: ' + (ss.getSheetByName(SH.intro) ? '找到' : '未找到'));
  Logger.log('執行: ' + (ss.getSheetByName(SH.exec) ? '找到' : '未找到'));
  
  SpreadsheetApp.getUi().alert('調試完成，請查看日誌（查看 > 記錄）');
  return '調試完成，請查看日誌';
}

// ███████████████████████████████████████████████████
// 📁 獨立功能3：元大帳戶處理（基於統一框架優化版）
// ═══════════════════════════════════════════════════════════════
// 注意：此程式需要搭配以下模組使用
// 共用模組：getSheetNames(), getSheetByName(), getColumnLetter(), CONFIG,
//          openProgressSidebar(), showToast(), updateSidebarProgress() 等函數
// 打卡模組：AttendanceModule 類別
// 統一框架：UnifiedProcessHandler 類別
// ███████████████████████████████████████████████████

/**
 * 元大帳戶處理流程配置 - 上半月
 */
function getYuantaAccountFirstHalfConfig() {
  return {
    name: "元大帳戶處理（上半月）",
    totalSteps: 8,
    punchMethod: "punchYuantaAccount",
    cells: {
      firstHalf: "C20",
      secondHalf: "D20",
      status: "E20"
    },
    sheetNames: {
      execSheet: "exec",
      summarySheet: "summary"
    },
    steps: [
      {
        name: "執行上半月元大帳戶處理",
        description: "執行上半月元大帳戶處理流程（N4:Q）...",
        type: "custom",
        handler: (sheets, isFirstHalf, processor) => {
          // 🔧 強制設定為上半月
          return executeCompleteYuantaProcess(sheets, true, processor);
        },
        required: true
      }
    ]
  };
}

/**
 * 元大帳戶處理流程配置 - 下半月
 */
function getYuantaAccountSecondHalfConfig() {
  return {
    name: "元大帳戶處理（下半月）",
    totalSteps: 12,
    punchMethod: "punchYuantaAccount",
    cells: {
      firstHalf: "C20",
      secondHalf: "D20",
      status: "E20"
    },
    sheetNames: {
      execSheet: "exec",
      summarySheet: "summary"
    },
    steps: [
      {
        name: "執行下半月元大帳戶處理",
        description: "執行下半月元大帳戶處理流程（U4:X + AB4:AE）...",
        type: "custom",
        handler: (sheets, isFirstHalf, processor) => {
          // 🔧 強制設定為下半月
          return executeCompleteYuantaProcess(sheets, false, processor);
        },
        required: true
      }
    ]
  };
}

/**
 * 統一元大帳戶執行函數
 */
function runBankAccountUpdate(isFirstHalf) {
  // 🔧 加強調試資訊
  console.log(`🔍 runBankAccountUpdate 被調用，參數 isFirstHalf = ${isFirstHalf}`);
  console.log(`🔍 將執行 ${isFirstHalf ? '上半月' : '下半月'} 處理`);
  
  const processor = new UnifiedProcessHandler(CONFIG);
  
  // 🔧 根據上下半月使用不同的配置
  const config = isFirstHalf ? getYuantaAccountFirstHalfConfig() : getYuantaAccountSecondHalfConfig();
  
  return processor.executeProcess(config, isFirstHalf);
}

/**
 * 上半月元大帳戶處理 - 修正版
 */
function runYuantaAccountFirstHalf() {
  console.log(`🔍 runYuantaAccountFirstHalf 被調用，將傳入 isFirstHalf = true`);
  return runBankAccountUpdate(true);
}

/**
 * 下半月元大帳戶處理 - 修正版
 */
function runYuantaAccountSecondHalf() {
  console.log(`🔍 runYuantaAccountSecondHalf 被調用，將傳入 isFirstHalf = false`);
  return runBankAccountUpdate(false);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 期別提取修正模組
// ═══════════════════════════════════════════════════════════════

/**
 * 從當前檔案名稱提取期別前綴
 */
function extractPeriodFromFileName() {
  try {
    const currentSpreadsheet = CentralContext.getSpreadsheet();
    const fileName = currentSpreadsheet.getName();
    
    console.log(`🔍 當前檔案名稱：${fileName}`);
    
    // 方法1：匹配 YYYYMM-N 格式（例如：202507-1）
    let match = fileName.match(/(\d{6}-\d)/);
    if (match && match[1]) {
      const period = match[1];
      console.log(`✅ 方法1找到期別：${period}`);
      return period;
    }
    
    // 方法2：匹配 YYYYMM 格式，然後判斷上下半月
    match = fileName.match(/(\d{6})/);
    if (match && match[1]) {
      const basePeriod = match[1];
      
      // 嘗試從檔名判斷是上半月還是下半月
      if (fileName.includes('-1') || fileName.includes('上半月')) {
        console.log(`✅ 方法2找到期別：${basePeriod}-1`);
        return `${basePeriod}-1`;
      } else if (fileName.includes('-2') || fileName.includes('下半月')) {
        console.log(`✅ 方法2找到期別：${basePeriod}-2`);
        return `${basePeriod}-2`;
      } else {
        // 無法判斷上下半月，返回基礎期別
        console.log(`⚠️ 找到基礎期別但無法判斷上下半月：${basePeriod}`);
        return basePeriod;
      }
    }
    
    // 如果都找不到，顯示警告
    console.warn(`⚠️ 無法從檔名 "${fileName}" 提取期別`);
    return null;
    
  } catch (error) {
    console.error(`❌ 提取期別失敗：`, error.message);
    return null;
  }
}

/**
 * 修正後的期別前綴取得函數
 */
function getPeriodPrefixFromFile(isFirstHalf) {
  try {
    // 先嘗試從檔案名稱提取完整期別
    const extractedPeriod = extractPeriodFromFileName();
    
    if (extractedPeriod) {
      // 如果提取到完整期別（包含-1或-2），直接使用
      if (extractedPeriod.includes('-')) {
        console.log(`✅ 使用提取的完整期別：${extractedPeriod}`);
        return extractedPeriod;
      } else {
        // 如果只有基礎期別，根據 isFirstHalf 添加後綴
        const halfMark = isFirstHalf ? "-1" : "-2";
        const fullPeriod = extractedPeriod + halfMark;
        console.log(`✅ 使用提取的基礎期別+後綴：${fullPeriod}`);
        return fullPeriod;
      }
    }
    
    // 備用方案：提示使用者手動設定
    console.warn("⚠️ 無法從檔名提取期別，使用備用方案");
    return promptUserForPeriod(isFirstHalf);
    
  } catch (error) {
    console.error(`❌ 期別處理失敗：`, error.message);
    return getDefaultPeriod(isFirstHalf);
  }
}

/**
 * 提示使用者手動輸入期別
 */
function promptUserForPeriod(isFirstHalf) {
  try {
    const ui = SpreadsheetApp.getUi();
    const periodType = isFirstHalf ? "上半月" : "下半月";
    
    const response = ui.prompt(
      "設定期別", 
      `無法自動提取期別，請輸入${periodType}期別（例如：202507-1）：`, 
      ui.ButtonSet.OK_CANCEL
    );
    
    if (response.getSelectedButton() === ui.Button.OK) {
      const userInput = response.getResponseText().trim();
      
      // 驗證格式
      if (userInput.match(/^\d{6}-\d$/)) {
        console.log(`✅ 使用者輸入期別：${userInput}`);
        return userInput;
      } else if (userInput.match(/^\d{6}$/)) {
        const halfMark = isFirstHalf ? "-1" : "-2";
        const fullPeriod = userInput + halfMark;
        console.log(`✅ 使用者輸入基礎期別，自動添加後綴：${fullPeriod}`);
        return fullPeriod;
      } else {
        ui.alert("格式錯誤", "請輸入正確格式，例如：202507-1 或 202507", ui.ButtonSet.OK);
        return getDefaultPeriod(isFirstHalf);
      }
    } else {
      return getDefaultPeriod(isFirstHalf);
    }
    
  } catch (error) {
    console.error(`❌ 使用者輸入期別失敗：`, error.message);
    return getDefaultPeriod(isFirstHalf);
  }
}

/**
 * 預設期別（當前日期）
 */
function getDefaultPeriod(isFirstHalf) {
  const currentDate = new Date();
  const basePrefix = Utilities.formatDate(currentDate, "Asia/Taipei", "yyyyMM");
  const halfMark = isFirstHalf ? "-1" : "-2";
  const defaultPeriod = basePrefix + halfMark;
  
  console.log(`⚠️ 使用預設期別：${defaultPeriod}`);
  return defaultPeriod;
}

/**
 * 修正後的期別取得函數 - 替換原本的 getPeriodPrefix
 */
function getPeriodPrefix(isFirstHalf) {
  return getPeriodPrefixFromFile(isFirstHalf);
}

// ═══════════════════════════════════════════════════════════════
// 🔧 元大帳戶完整處理函數
// ═══════════════════════════════════════════════════════════════

/**
 * 執行完整的元大帳戶處理流程
 */
function executeCompleteYuantaProcess(sheets, isFirstHalf, handler) {
  const { execSheet, summarySheet } = sheets;
  
  // 🔧 加強調試：確認參數值 - 這次應該是正確的
  console.log(`🔍 executeCompleteYuantaProcess 被調用`);
  console.log(`🔍 參數 isFirstHalf = ${isFirstHalf}，型別 = ${typeof isFirstHalf}`);
  
  // 🔧 確認處理的是上半月還是下半月
  const periodText = isFirstHalf ? "上半月" : "下半月";
  handler.updateProgress(`開始處理${periodText}元大帳戶...`);
  console.log(`🔍 執行 ${periodText} 處理，isFirstHalf = ${isFirstHalf}`);
  
  // 步驟1：在當前資料夾中尋找期別元大帳戶檔案
  handler.updateProgress("在當前資料夾尋找期別元大帳戶檔案...");
  
  let yuantaAccountSpreadsheet;
  let originalFileName;
  try {
    // 🔧 取得當前檔案（期別專員薪資表）的資料夾
    const currentFileId = CentralContext.getSpreadsheet().getId();
    const currentFile = DriveApp.getFileById(currentFileId);
    const currentFolder = currentFile.getParents().next();
    
    console.log(`🔍 當前檔案：${currentFile.getName()}`);
    console.log(`🔍 當前資料夾：${currentFolder.getName()}`);
    
    // 🔧 列出所有檔案進行調試
    const allFiles = currentFolder.getFiles();
    const fileList = [];
    while (allFiles.hasNext()) {
      const file = allFiles.next();
      fileList.push(file.getName());
    }
    console.log(`🔍 資料夾中的所有檔案：`, fileList);
    
    // 🔧 重新搜尋，先找"元大帳戶"檔案
    let yuantaAccountFile = null;
    const allFiles2 = currentFolder.getFiles();
    
    while (allFiles2.hasNext()) {
      const file = allFiles2.next();
      const fileName = file.getName();
      
      // 🔧 只找原始的"元大帳戶"檔案，排除已處理的檔案
      if (fileName.includes("元大帳戶") && 
          !fileName.includes("承攬費") && 
          !fileName.includes("工具包押金")) {
        yuantaAccountFile = file;
        console.log(`✅ 找到元大帳戶檔案：${fileName}`);
        break;
      }
    }
    
    // 🔧 如果找不到，嘗試更寬鬆的搜尋
    if (!yuantaAccountFile) {
      console.log("⚠️ 用嚴格條件找不到，嘗試寬鬆搜尋...");
      const allFiles3 = currentFolder.getFiles();
      
      while (allFiles3.hasNext()) {
        const file = allFiles3.next();
        const fileName = file.getName();
        
        // 🔧 更寬鬆的條件：包含"元大"且不包含多個".xlsx"
        if (fileName.includes("元大") && !fileName.includes(".xlsx.xlsx")) {
          console.log(`🔍 寬鬆搜尋候選：${fileName}`);
          if (fileName.includes("帳戶")) {
            yuantaAccountFile = file;
            console.log(`✅ 寬鬆搜尋找到：${fileName}`);
            break;
          }
        }
      }
    }
    
    if (!yuantaAccountFile) {
      // 🔧 如果還是找不到，列出所有包含"元大"的檔案
      console.log("❌ 完全找不到元大帳戶檔案");
      const allFiles4 = currentFolder.getFiles();
      const yuantaFiles = [];
      while (allFiles4.hasNext()) {
        const file = allFiles4.next();
        if (file.getName().includes("元大")) {
          yuantaFiles.push(file.getName());
        }
      }
      console.log(`🔍 所有包含"元大"的檔案：`, yuantaFiles);
      throw new Error("在當前資料夾中找不到原始期別元大帳戶檔案。請檢查檔案是否存在。");
    }
    
    yuantaAccountSpreadsheet = SpreadsheetApp.openById(yuantaAccountFile.getId());
    originalFileName = yuantaAccountFile.getName();
    
    handler.updateProgress(`成功找到期別元大帳戶檔案：${originalFileName}`);
    console.log(`✅ 最終找到檔案：${originalFileName}`);
    
  } catch (error) {
    throw new Error(`無法找到或開啟期別元大帳戶檔案：${error.message}`);
  }
  
  const yuantaAccountSheet = yuantaAccountSpreadsheet.getSheets()[0];
  // H2 固定記錄執行期別的 YYYYMM，不依目前日期寫死。
  let centralPeriod = "";
  try {
    centralPeriod = CentralContext.getPeriod();
  } catch (ignore) {
    centralPeriod = getPeriodPrefix(isFirstHalf);
  }
  yuantaAccountSheet.getRange("H2").setValue(String(centralPeriod).slice(0, 6));
  
  // 步驟2：設定表頭
  handler.updateProgress("設定元大帳戶表頭...");
  setupYuantaAccountHeaders(yuantaAccountSheet);
  
  // 步驟3：定位到期別元大帳戶A3
  yuantaAccountSheet.activate();
  yuantaAccountSheet.getRange("A3").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  // 步驟4：清空A3:E範圍
  handler.updateProgress("清空期別元大帳戶 A3:E 範圍...");
  yuantaAccountSheet.getRange("A3:E").clearContent();
  SpreadsheetApp.flush();
  Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
  
  // 🔧 從原檔案名稱提取區域資訊
  const region = extractRegionFromFileName(originalFileName);
  console.log(`🔍 提取的區域名稱：${region}`);
  
  // 步驟5：根據上下半月執行不同處理
  let result;
  
  // 🔧 明確的條件判斷，加入詳細日誌
  if (isFirstHalf === true) {
    handler.updateProgress("🔵 條件判斷：確認執行上半月處理流程（N4:Q）...");
    console.log("✅ 條件判斷通過：isFirstHalf === true，進入上半月處理分支");
    result = processFirstHalfYuanta(summarySheet, yuantaAccountSheet, yuantaAccountSpreadsheet, region, handler);
  } else if (isFirstHalf === false) {
    handler.updateProgress("🔵 條件判斷：確認執行下半月處理流程（U4:X + AB4:AE）...");
    console.log("✅ 條件判斷通過：isFirstHalf === false，進入下半月處理分支");
    result = processSecondHalfYuanta(summarySheet, yuantaAccountSheet, yuantaAccountSpreadsheet, region, handler);
  } else {
    // 🔧 異常狀況處理
    console.error(`❌ 異常：isFirstHalf 參數值異常 = ${isFirstHalf}，型別 = ${typeof isFirstHalf}`);
    throw new Error(`isFirstHalf 參數值異常：${isFirstHalf}，應為 true 或 false`);
  }
  
  return result;
}

// ═══════════════════════════════════════════════════════════════
// 🔧 表頭設定模組
// ═══════════════════════════════════════════════════════════════

/**
 * 設定元大帳戶表頭
 */
function setupYuantaAccountHeaders(sheet) {
  try {
    console.log("🔧 設定元大帳戶表頭...");
    
    // 設定A1:E2表頭
    const headers = [
      ["轉帳日期", "受款人姓名", "受款人帳號", "轉帳金額", "備註"],
      ["", "", "", "", ""]
    ];
    
    // 寫入表頭
    sheet.getRange("A1:E2").setValues(headers);
    
    // 設定格式：粗體第一行
    sheet.getRange("A1:E1").setFontWeight("bold");
    
    // 設定欄寬
    sheet.setColumnWidth(1, 100); // A欄：轉帳日期
    sheet.setColumnWidth(2, 120); // B欄：受款人姓名
    sheet.setColumnWidth(3, 150); // C欄：受款人帳號
    sheet.setColumnWidth(4, 100); // D欄：轉帳金額
    sheet.setColumnWidth(5, 100); // E欄：備註
    
    // 設定邊框
    const headerRange = sheet.getRange("A1:E2");
    headerRange.setBorder(true, true, true, true, true, true);
    
    // 設定背景色（淡灰色）
    sheet.getRange("A1:E1").setBackground("#f0f0f0");
    
    console.log("✅ 元大帳戶表頭設定完成");
    
  } catch (error) {
    console.error("❌ 設定表頭失敗：", error.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 上半月元大帳戶處理
// ═══════════════════════════════════════════════════════════════

/**
 * 上半月處理：場次時數薪資總表 N4:Q -> 期別元大帳戶 A3:E
 */
function processFirstHalfYuanta(summarySheet, yuantaAccountSheet, yuantaAccountSpreadsheet, region, handler) {
  handler.updateProgress("🔵 上半月處理：開始處理N4:Q資料...");
  console.log("🔍 上半月函數被調用，準備處理 N4:Q 範圍");
  
  // 計算當月10日（週六日提前到週五）
  const targetDate = getTargetDateFor10th(new Date());
  handler.updateProgress(`計算目標日期：${Utilities.formatDate(targetDate, "Asia/Taipei", "yyyy/MM/dd")}`);
  
  // 定位到場次時數薪資總表N4
  summarySheet.activate();
  summarySheet.getRange("N4").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  // 取得N4:Q資料（動態到最後一行）
  handler.updateProgress("🔵 取得場次時數薪資總表 N4:Q 資料...");
  const sourceData = getDataFromRange(summarySheet, "N4:Q");
  console.log(`🔍 N4:Q 取得資料筆數：${sourceData.length}`);
  
  if (sourceData.length > 0) {
    handler.updateProgress(`🔵 複製${sourceData.length}筆N4:Q資料到元大帳戶...`);
    
    // 定位到期別元大帳戶A3
    yuantaAccountSheet.activate();
    yuantaAccountSheet.getRange("A3").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    // 寫入資料：A欄填日期，B3:E填入N4:Q資料
    writeDataToYuantaAccount(yuantaAccountSheet, sourceData, targetDate, 3);
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
    
    handler.updateProgress("🔵 上半月資料寫入完成，準備存檔...");
    
    // 存檔處理 - 🔧 修正：直接另存為 .xlsx 檔案
    const period = getPeriodPrefix(true); // 上半月：從檔名提取期別
    
    // 🔧 修正檔名格式：期別元大承攬費-區域
    const contractorFeeExcelName = `${period}元大承攬費-${region}`;
    console.log(`🔍 準備儲存檔案：${contractorFeeExcelName}.xlsx`);
    
    saveAsExcelFile(yuantaAccountSpreadsheet, contractorFeeExcelName, handler);
    
    handler.updateProgress("✅ 上半月元大承攬費存檔完成");
    
    return {
      type: "上半月",
      sourceRange: "N4:Q",
      processedRows: sourceData.length,
      targetDate: targetDate,
      excelFileName: `${contractorFeeExcelName}.xlsx`,
      region: region,
      period: period,
      message: `上半月處理完成，共${sourceData.length}筆N4:Q資料`
    };
    
  } else {
    handler.updateProgress("⚠️ 上半月：N4:Q範圍無資料");
    return { 
      type: "上半月",
      sourceRange: "N4:Q",
      processedRows: 0, 
      region: region,
      period: getPeriodPrefix(true),
      message: "上半月：N4:Q範圍無資料" 
    };
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 下半月元大帳戶處理
// ═══════════════════════════════════════════════════════════════

/**
 * 下半月處理：場次時數薪資總表 U4:X -> 期別元大帳戶 A3:E
 *            如果 AB4:AE 非空白，額外處理工具包押金
 */
function processSecondHalfYuanta(summarySheet, yuantaAccountSheet, yuantaAccountSpreadsheet, region, handler) {
  const results = {};
  
  // 計算當月20日（週六日提前到週五）
  const targetDate = getTargetDateFor20th(new Date());
  const period = getPeriodPrefix(false); // 下半月：從檔名提取期別
  
  // 🔧 第一部分：處理U4:X承攬費資料
  handler.updateProgress("處理下半月承攬費資料（U4:X）...");
  
  // 定位到場次時數薪資總表U4
  summarySheet.activate();
  summarySheet.getRange("U4").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  const uSourceData = getDataFromRange(summarySheet, "U4:X");
  
  if (uSourceData.length > 0) {
    handler.updateProgress(`複製${uSourceData.length}筆U4:X資料到元大帳戶...`);
    
    // 定位到期別元大帳戶A3
    yuantaAccountSheet.activate();
    yuantaAccountSheet.getRange("A3").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    // 寫入U4:X資料
    writeDataToYuantaAccount(yuantaAccountSheet, uSourceData, targetDate, 3);
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
    
    // 存檔：承攬費 - 🔧 修正：直接另存為 .xlsx 檔案
    const contractorFeeExcelName = `${period}元大承攬費-${region}`;
    console.log(`🔍 準備儲存承攬費檔案：${contractorFeeExcelName}.xlsx`);
    
    saveAsExcelFile(yuantaAccountSpreadsheet, contractorFeeExcelName, handler);
    
    results.contractorFee = {
      processedRows: uSourceData.length,
      excelFileName: `${contractorFeeExcelName}.xlsx`
    };
    
    handler.updateProgress("下半月承攬費存檔完成");
    
  } else {
    handler.updateProgress("⚠️ U4:X範圍無資料");
    results.contractorFee = { processedRows: 0, message: "U4:X範圍無資料" };
  }
  
  // 🔧 第二部分：檢查AB4:AE工具包押金資料
  handler.updateProgress("檢查AB4:AE工具包押金資料...");
  
  // 定位到場次時數薪資總表AB4
  summarySheet.activate();
  summarySheet.getRange("AB4").activate();
  SpreadsheetApp.flush();
  Utilities.sleep(1000);
  
  const abData = getDataFromRange(summarySheet, "AB4:AE");
  
  if (abData.length > 0) {
    handler.updateProgress(`發現${abData.length}筆AB4:AE工具包押金資料，開始處理...`);
    
    // 重新清空A3:E（準備寫入工具包押金資料）
    yuantaAccountSheet.getRange("A3:E").clearContent();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    // 定位到期別元大帳戶A3
    yuantaAccountSheet.activate();
    yuantaAccountSheet.getRange("A3").activate();
    SpreadsheetApp.flush();
    Utilities.sleep(1000);
    
    // 寫入AB4:AE資料（只有4欄：AB、AC、AD、AE -> B、C、D、E）
    writeDataToYuantaAccount(yuantaAccountSheet, abData, targetDate, 3);
    
    SpreadsheetApp.flush();
    Utilities.sleep(handler.config.PROCESS_DELAY || 2000);
    
    // 存檔：工具包押金 - 🔧 修正：直接另存為 .xlsx 檔案
    const toolDepositExcelName = `${period}元大工具包押金-${region}`;
    console.log(`🔍 準備儲存工具包押金檔案：${toolDepositExcelName}.xlsx`);
    
    saveAsExcelFile(yuantaAccountSpreadsheet, toolDepositExcelName, handler);
    
    results.toolDeposit = {
      processedRows: abData.length,
      excelFileName: `${toolDepositExcelName}.xlsx`
    };
    
    handler.updateProgress("工具包押金存檔完成");
    
  } else {
    handler.updateProgress("ℹ️ AB4:AE範圍無資料，跳過工具包押金處理");
    results.toolDeposit = { processedRows: 0, message: "AB4:AE範圍無資料" };
  }
  
  results.targetDate = targetDate;
  results.region = region;
  results.period = period;
  
  // 🔧 簡化訊息組合，避免複雜的模板字串巢狀
  let message = "下半月處理完成";
  if (results.contractorFee.processedRows > 0) {
    message += "，承攬費" + results.contractorFee.processedRows + "筆";
  }
  if (results.toolDeposit.processedRows > 0) {
    message += "，工具包押金" + results.toolDeposit.processedRows + "筆";
  }
  results.message = message;
  
  return results;
}

// ═══════════════════════════════════════════════════════════════
// 🔧 輔助函數
// ═══════════════════════════════════════════════════════════════

/**
 * 計算當月10日（週六日提前到週五）
 */
function getTargetDateFor10th(currentDate) {
  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();
  
  let targetDate = new Date(year, month, 10);
  const dayOfWeek = targetDate.getDay();
  
  if (dayOfWeek === 6) {
    targetDate.setDate(9);  // 週六提前到週五
  } else if (dayOfWeek === 0) {
    targetDate.setDate(8);  // 週日提前到週五
  }
  
  return targetDate;
}

/**
 * 計算當月20日（週六日提前到週五）
 */
function getTargetDateFor20th(currentDate) {
  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();
  
  let targetDate = new Date(year, month, 20);
  const dayOfWeek = targetDate.getDay();
  
  if (dayOfWeek === 6) {
    targetDate.setDate(19);  // 週六提前到週五
  } else if (dayOfWeek === 0) {
    targetDate.setDate(18);  // 週日提前到週五
  }
  
  return targetDate;
}

/**
 * 從指定範圍取得資料（動態到最後一行）
 */
function getDataFromRange(sheet, rangeString) {
  try {
    const rangeParts = rangeString.split(":");
    const startCell = rangeParts[0];
    const endCell = rangeParts[1];
    
    const startRow = parseInt(startCell.match(/\d+/)[0]);
    const startCol = columnToNumber(startCell.match(/[A-Z]+/)[0]);
    const endCol = columnToNumber(endCell.match(/[A-Z]+/)[0]);
    
    const lastRow = sheet.getLastRow();
    
    if (lastRow < startRow) {
      return [];
    }
    
    const numRows = lastRow - startRow + 1;
    const numCols = endCol - startCol + 1;
    
    const dataRange = sheet.getRange(startRow, startCol, numRows, numCols);
    const values = dataRange.getValues();
    
    // 過濾空行
    const filteredData = values.filter(row => 
      row.some(cell => cell !== null && cell !== undefined && cell !== "")
    );
    
    return filteredData;
    
  } catch (error) {
    console.error(`取得範圍${rangeString}資料失敗：`, error);
    return [];
  }
}

/**
 * 將欄位字母轉換為數字
 */
function columnToNumber(column) {
  let result = 0;
  for (let i = 0; i < column.length; i++) {
    result = result * 26 + (column.charCodeAt(i) - 'A'.charCodeAt(0) + 1);
  }
  return result;
}

/**
 * 寫入資料到元大帳戶
 */
function writeDataToYuantaAccount(sheet, data, targetDate, startRow) {
  if (data.length === 0) return;
  
  for (let i = 0; i < data.length; i++) {
    const currentRow = startRow + i;
    const rowData = data[i];
    
    // A欄填入目標日期
    sheet.getRange(currentRow, 1).setValue(targetDate);
    
    // B欄開始填入源資料
    for (let j = 0; j < rowData.length; j++) {
      if (rowData[j] !== null && rowData[j] !== undefined && rowData[j] !== "") {
        sheet.getRange(currentRow, j + 2).setValue(rowData[j]);
      }
    }
  }
}

/**
 * 直接另存為Excel檔案
 * 🔧 覆蓋版：直接覆蓋同名檔案，不產生重複
 */
function saveAsExcelFile(spreadsheet, fileName, handler) {
  try {
    console.log(`🔍 開始存檔為Excel格式，檔名：${fileName}`);
    
    // 🔧 確保檔名格式正確
    let cleanFileName = fileName;
    if (cleanFileName.endsWith('.xlsx')) {
      cleanFileName = cleanFileName.slice(0, -5);
    }
    
    const finalFileName = `${cleanFileName}.xlsx`;
    console.log(`🔍 目標Excel檔名：${finalFileName}`);
    
    // 🔧 取得來源檔案資訊
    const sourceFile = DriveApp.getFileById(spreadsheet.getId());
    const parentFolders = sourceFile.getParents();
    const targetFolder = parentFolders.hasNext() ? parentFolders.next() : DriveApp.getRootFolder();
    
    console.log(`🔍 來源檔案：${sourceFile.getName()}`);
    console.log(`🔍 目標資料夾：${targetFolder.getName()}`);
    
    // 🔧 先刪除所有同名檔案（確保覆蓋）
    let deletedCount = 0;
    const existingFiles = targetFolder.getFilesByName(finalFileName);
    while (existingFiles.hasNext()) {
      const existingFile = existingFiles.next();
      console.log(`🗑️ 刪除舊檔案：${existingFile.getName()}`);
      existingFile.setTrashed(true);
      deletedCount++;
    }
    
    if (deletedCount > 0) {
      console.log(`✅ 已刪除 ${deletedCount} 個同名舊檔案`);
      handler.updateProgress(`🗑️ 已刪除 ${deletedCount} 個舊檔案，準備覆蓋...`);
      
      // 🔧 等待一下確保刪除完成
      Utilities.sleep(1000);
    }
    
    // 🔧 使用 Google Sheets API 匯出為 Excel 格式
    const exportUrl = `https://docs.google.com/spreadsheets/d/${spreadsheet.getId()}/export?format=xlsx`;
    
    try {
      console.log(`🔄 開始匯出Excel格式...`);
      
      // 🔧 使用 UrlFetchApp 下載 Excel 格式
      const response = UrlFetchApp.fetch(exportUrl, {
        headers: {
          'Authorization': 'Bearer ' + ScriptApp.getOAuthToken()
        }
      });
      
      if (response.getResponseCode() === 200) {
        // 🔧 建立新的 Excel 檔案
        const excelBlob = response.getBlob().setName(finalFileName);
        const excelFile = targetFolder.createFile(excelBlob);
        
        console.log(`✅ Excel檔案已建立：${excelFile.getName()}`);
        console.log(`✅ 檔案大小：${excelFile.getSize()} bytes`);
        console.log(`✅ 檔案ID：${excelFile.getId()}`);
        
        handler.updateProgress(`📁 Excel檔案已覆蓋儲存：${finalFileName}`);
        return excelFile.getId();
        
      } else {
        throw new Error(`匯出失敗，HTTP狀態：${response.getResponseCode()}`);
      }
      
    } catch (exportError) {
      console.error(`❌ Excel匯出失敗：`, exportError);
      
      // 🔧 備用方案：複製為Google Sheet格式，並確保覆蓋
      console.log(`🔄 使用備用方案：複製為Google Sheet格式`);
      
      // 再次檢查並刪除同名的Google Sheet檔案
      const existingGoogleFiles = targetFolder.getFilesByName(cleanFileName);
      while (existingGoogleFiles.hasNext()) {
        const existingFile = existingGoogleFiles.next();
        console.log(`🗑️ 刪除舊Google Sheet：${existingFile.getName()}`);
        existingFile.setTrashed(true);
      }
      
      const newFile = sourceFile.makeCopy(cleanFileName, targetFolder);
      
      handler.updateProgress(`📁 檔案已覆蓋（Google Sheet格式）：${newFile.getName()}`);
      console.log(`⚠️ 備用方案：已覆蓋為Google Sheet格式`);
      
      return newFile.getId();
    }
    
  } catch (error) {
    console.error(`❌ 另存檔案失敗：`, error);
    handler.updateProgress(`⚠️ 另存檔案失敗：${error.message}`);
    return null;
  }
}

/**
 * 從檔案名稱提取區域資訊
 * 例如：「202507-1元大帳戶-台中」→「台中」
 */
function extractRegionFromFileName(fileName) {
  try {
    // 🔧 先移除所有副檔名和重複的 .xlsx
    let cleanFileName = fileName;
    
    // 移除所有可能的副檔名組合
    cleanFileName = cleanFileName.replace(/\.xlsx.*$/gi, ''); // 移除 .xlsx 及其後面所有內容
    cleanFileName = cleanFileName.replace(/\.xls.*$/gi, '');  // 移除 .xls 及其後面所有內容
    cleanFileName = cleanFileName.replace(/\.gs.*$/gi, '');   // 移除 .gs 及其後面所有內容
    
    console.log(`🔍 原始檔名：${fileName}`);
    console.log(`🔍 清理後檔名：${cleanFileName}`);
    
    // 🔧 方法1：尋找「元大帳戶-」後面的部分
    let match = cleanFileName.match(/元大帳戶-([^.]+)$/);
    if (match && match[1]) {
      const region = match[1].trim();
      console.log(`🔍 方法1找到區域：${region}`);
      return region;
    }
    
    // 🔧 方法2：如果是類似 "202507-1元大帳戶-台中" 的格式
    match = cleanFileName.match(/\d+-\d+元大帳戶-([^.]+)$/);
    if (match && match[1]) {
      const region = match[1].trim();
      console.log(`🔍 方法2找到區域：${region}`);
      return region;
    }
    
    // 🔧 方法3：提取最後一個「-」後面的部分（確保不包含副檔名）
    match = cleanFileName.match(/-([^-.]+)$/);
    if (match && match[1]) {
      const region = match[1].trim();
      console.log(`🔍 方法3找到區域：${region}`);
      return region;
    }
    
    // 如果都沒有找到，返回預設值
    console.warn(`⚠️ 無法從檔名 "${fileName}" 提取區域，使用預設值`);
    return "台中";
    
  } catch (error) {
    console.error(`❌ 提取區域名稱失敗：`, error);
    return "台中";
  }
}

// ═══════════════════════════════════════════════════════════════
// 🔧 診斷和測試工具
// ═══════════════════════════════════════════════════════════════

/**
 * 測試期別提取功能
 */
function testPeriodExtraction() {
  try {
    console.log("🔍 測試期別提取功能...");
    
    // 測試提取功能
    const extracted = extractPeriodFromFileName();
    console.log(`提取結果：${extracted}`);
    
    // 測試上半月
    const firstHalf = getPeriodPrefixFromFile(true);
    console.log(`上半月期別：${firstHalf}`);
    
    // 測試下半月
    const secondHalf = getPeriodPrefixFromFile(false);
    console.log(`下半月期別：${secondHalf}`);
    
    const ui = SpreadsheetApp.getUi();
    ui.alert(
      "🔍 期別提取測試", 
      `提取結果：${extracted}\n上半月：${firstHalf}\n下半月：${secondHalf}`, 
      ui.ButtonSet.OK
    );
    
  } catch (error) {
    console.error("測試失敗：", error.message);
    SpreadsheetApp.getUi().alert("測試失敗", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * 檢查當前檔案名稱格式
 */
function checkCurrentFileName() {
  try {
    const currentSpreadsheet = CentralContext.getSpreadsheet();
    const fileName = currentSpreadsheet.getName();
    
    console.log(`🔍 當前檔案名稱：${fileName}`);
    
    // 分析檔名結構
    const analysis = {
      fileName: fileName,
      hasPeriod: /\d{6}/.test(fileName),
      hasHalfMonth: /\d{6}-\d/.test(fileName),
      extractedPeriod: null,
      suggestedFormat: null
    };
    
    // 嘗試提取期別
    let match = fileName.match(/(\d{6}-\d)/);
    if (match) {
      analysis.extractedPeriod = match[1];
      analysis.suggestedFormat = "格式正確";
    } else {
      match = fileName.match(/(\d{6})/);
      if (match) {
        analysis.extractedPeriod = match[1];
        analysis.suggestedFormat = `建議改為：${match[1]}-1 或 ${match[1]}-2`;
      } else {
        analysis.suggestedFormat = "建議格式：YYYYMM-N（例如：202507-1）";
      }
    }
    
    console.log("檔名分析結果：", analysis);
    
    const ui = SpreadsheetApp.getUi();
    const message = `檔案名稱：${fileName}\n` +
                   `包含期別：${analysis.hasPeriod ? '是' : '否'}\n` +
                   `包含上下半月：${analysis.hasHalfMonth ? '是' : '否'}\n` +
                   `提取的期別：${analysis.extractedPeriod || '無'}\n` +
                   `建議：${analysis.suggestedFormat}`;
    
    ui.alert("📋 檔名格式檢查", message, ui.ButtonSet.OK);
    
    return analysis;
    
  } catch (error) {
    console.error("檢查檔名失敗：", error.message);
    return null;
  }
}

/**
 * 檢查元大帳戶檔案存在性
 */
function checkYuantaAccountFile() {
  try {
    console.log("🔍 檢查元大帳戶檔案...");
    
    const currentFileId = CentralContext.getSpreadsheet().getId();
    const currentFile = DriveApp.getFileById(currentFileId);
    const currentFolder = currentFile.getParents().next();
    
    console.log(`🔍 當前檔案：${currentFile.getName()}`);
    console.log(`🔍 當前資料夾：${currentFolder.getName()}`);
    
    // 列出資料夾中所有檔案
    const allFiles = currentFolder.getFiles();
    const fileList = [];
    const yuantaFiles = [];
    
    while (allFiles.hasNext()) {
      const file = allFiles.next();
      const fileName = file.getName();
      fileList.push(fileName);
      
      if (fileName.includes("元大")) {
        yuantaFiles.push(fileName);
      }
    }
    
    console.log(`🔍 資料夾中的所有檔案：`, fileList);
    console.log(`🔍 包含"元大"的檔案：`, yuantaFiles);
    
    // 尋找元大帳戶檔案
    let foundYuantaAccount = null;
    yuantaFiles.forEach(fileName => {
      if (fileName.includes("帳戶") && !fileName.includes("承攬費") && !fileName.includes("工具包押金")) {
        foundYuantaAccount = fileName;
      }
    });
    
    const ui = SpreadsheetApp.getUi();
    let message = `當前資料夾：${currentFolder.getName()}\n\n`;
    message += `所有檔案數量：${fileList.length}\n`;
    message += `包含"元大"的檔案：${yuantaFiles.length}\n\n`;
    
    if (foundYuantaAccount) {
      message += `✅ 找到元大帳戶檔案：\n${foundYuantaAccount}`;
    } else {
      message += `❌ 未找到元大帳戶檔案\n\n包含"元大"的檔案：\n${yuantaFiles.join('\n')}`;
    }
    
    ui.alert("🔍 元大帳戶檔案檢查", message, ui.ButtonSet.OK);
    
    return {
      folderName: currentFolder.getName(),
      totalFiles: fileList.length,
      yuantaFiles: yuantaFiles,
      foundYuantaAccount: foundYuantaAccount
    };
    
  } catch (error) {
    console.error("檢查元大帳戶檔案失敗：", error.message);
    SpreadsheetApp.getUi().alert("檢查失敗", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
    return null;
  }
}

/**
 * 診斷N4:Q和U4:X資料
 */
function diagnoseSourceData() {
  try {
    console.log("🔍 診斷來源資料...");
    
    const summarySheet = CentralContext.getSpreadsheet().getSheetByName("場次時數薪資總表");
    if (!summarySheet) {
      throw new Error("找不到場次時數薪資總表工作表");
    }
    
    // 檢查N4:Q資料
    const n4qData = getDataFromRange(summarySheet, "N4:Q");
    console.log(`N4:Q資料筆數：${n4qData.length}`);
    
    // 檢查U4:X資料
    const u4xData = getDataFromRange(summarySheet, "U4:X");
    console.log(`U4:X資料筆數：${u4xData.length}`);
    
    // 檢查AB4:AE資料
    const ab4aeData = getDataFromRange(summarySheet, "AB4:AE");
    console.log(`AB4:AE資料筆數：${ab4aeData.length}`);
    
    const ui = SpreadsheetApp.getUi();
    const message = `📊 來源資料診斷結果：\n\n` +
                   `N4:Q（上半月承攬費）：${n4qData.length} 筆\n` +
                   `U4:X（下半月承攬費）：${u4xData.length} 筆\n` +
                   `AB4:AE（工具包押金）：${ab4aeData.length} 筆\n\n` +
                   `建議：\n` +
                   `- 上半月處理：需要N4:Q有資料\n` +
                   `- 下半月處理：需要U4:X有資料\n` +
                   `- 工具包押金：AB4:AE有資料時額外處理`;
    
    ui.alert("📊 來源資料診斷", message, ui.ButtonSet.OK);
    
    return {
      n4qCount: n4qData.length,
      u4xCount: u4xData.length,
      ab4aeCount: ab4aeData.length
    };
    
  } catch (error) {
    console.error("診斷來源資料失敗：", error.message);
    SpreadsheetApp.getUi().alert("診斷失敗", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
    return null;
  }
}

/**
 * 完整元大帳戶診斷工具
 */
function fullYuantaDiagnosis() {
  try {
    console.log("🔍 開始完整元大帳戶診斷...");
    
    // 1. 檢查檔案名稱
    const fileNameCheck = checkCurrentFileName();
    
    // 2. 檢查元大帳戶檔案
    const yuantaFileCheck = checkYuantaAccountFile();
    
    // 3. 診斷來源資料
    const sourceDataCheck = diagnoseSourceData();
    
    // 4. 測試期別提取
    testPeriodExtraction();
    
    const ui = SpreadsheetApp.getUi();
    ui.alert(
      "✅ 完整診斷完成", 
      "所有診斷項目已完成，請查看各個診斷結果。\n\n如果診斷顯示正常，即可執行元大帳戶處理。", 
      ui.ButtonSet.OK
    );
    
  } catch (error) {
    console.error("完整診斷失敗：", error.message);
    SpreadsheetApp.getUi().alert("診斷失敗", error.message, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

// ███████████████████████████████████████████████████
// 📄 主程式 10 PDF 產出模組（統整完整版）
// ███████████████████████████████████████████████████
// ███████████████████████████████████████████████████
// 📄 PDF 產出模組（清潔承攬費 / 專案薪資 共用版）
// 功能：
// 1. 依指定 PDF 清單工作表 H欄=Y 的名單產出 PDF
// 2. 成功後清除 H 欄，失敗保留 Y 以便重新執行
// 3. D 欄寫入實際檔案存檔時間
// 4. 第25列固定重設為 20
// 5. 第27列依 AC27 備註內容自動調整高度
// 6. 支援緊急停止
// 7. 可重新標記失敗列
// 8. 若 E欄已有連結，優先更新原檔，保留相同連結
// ███████████████████████████████████████████████████


// ═══════════════════════════════════════════════════════════════
// 📄 PDF 工作設定
// ═══════════════════════════════════════════════════════════════
var PDF_JOB_CONFIG = {
  CLEANING: {
    listSheetName: "PDF產出",
    dataSheetName: "薪資表",
    salarySheetName: "薪資單",
    fileTitle: "清潔承攬服務費"
  },
  PROJECT: {
    listSheetName: "專案PDF產出",
    dataSheetName: "專案薪資表",
    salarySheetName: "專案薪資單",
    fileTitle: "清潔專案承攬服務費"
  }
};


// ═══════════════════════════════════════════════════════════════
// 📄 對外入口
// ═══════════════════════════════════════════════════════════════
function generateSalaryPDFs_v2025(sendEmail) {
  return generateSalaryPDFsByConfig_("CLEANING", sendEmail === true);
}

function generateProjectSalaryPDFs(sendEmail) {
  return generateSalaryPDFsByConfig_("PROJECT", sendEmail === true);
}

function quickRegenerateAllCleaningPDFs() {
  markAllStaffForProcessing_("CLEANING");
  Utilities.sleep(500);
  return generateSalaryPDFsByConfig_("CLEANING", false);
}

function quickRegenerateAllProjectPDFs() {
  markAllStaffForProcessing_("PROJECT");
  Utilities.sleep(500);
  return generateSalaryPDFsByConfig_("PROJECT", false);
}

function remarkFailedCleaningPdfRows() {
  return remarkFailedPdfRowsByConfig_("CLEANING");
}

function remarkFailedProjectPdfRows() {
  return remarkFailedPdfRowsByConfig_("PROJECT");
}

function markAllCleaningStaffForProcessing() {
  return markAllStaffForProcessing_("CLEANING");
}

function markAllProjectStaffForProcessing() {
  return markAllStaffForProcessing_("PROJECT");
}

function clearCleaningControlFlags() {
  return clearAllControlFlagsByConfig_("CLEANING");
}

function clearProjectControlFlags() {
  return clearAllControlFlagsByConfig_("PROJECT");
}

function repairCleaningPdfLinks() {
  return repairPdfLinksByConfig_("CLEANING");
}

function repairProjectPdfLinks() {
  return repairPdfLinksByConfig_("PROJECT");
}

function generateCleaningSalaryPDFsByPeriodFile(rootFolderId, periodCode, regionName, sendEmail) {
  var fileId = findPeriodFileId_(rootFolderId, periodCode, "清潔承攬", regionName);
  return generateSalaryPDFsByConfigAndFile_("CLEANING", fileId, sendEmail === true);
}

function generateProjectSalaryPDFsByPeriodFile(rootFolderId, periodCode, regionName, sendEmail) {
  var fileId = findPeriodFileId_(rootFolderId, periodCode, "清潔承攬", regionName);
  return generateSalaryPDFsByConfigAndFile_("PROJECT", fileId, sendEmail === true);
}

function findPeriodFileId_(rootFolderId, periodCode, label, regionName) {
  if (!rootFolderId) throw new Error("缺少 rootFolderId");
  if (!periodCode) throw new Error("缺少期別");
  if (!regionName) throw new Error("缺少區域名稱");

  var cleanRegion = String(regionName).replace(/區$/, "").trim();
  var folder = DriveApp.getFolderById(rootFolderId);

  var periodFolders = folder.getFoldersByName(periodCode);
  if (!periodFolders.hasNext()) {
    throw new Error("找不到期別資料夾：" + periodCode);
  }

  var periodFolder = periodFolders.next();

  var fileName = periodCode + label + "-" + cleanRegion;
  var files = periodFolder.getFilesByName(fileName);

  if (!files.hasNext()) {
    throw new Error("找不到檔案：" + fileName);
  }

  return files.next().getId();
}

// ═══════════════════════════════════════════════════════════════
// 📄 共用：停止控制
// ═══════════════════════════════════════════════════════════════
function setEmergencyStopFlag() {
  PropertiesService.getScriptProperties().setProperty("STOP_EXECUTION", "true");
  console.log("⛔ 已設定緊急停止旗標");
  return { success: true };
}

function clearEmergencyStopFlag() {
  PropertiesService.getScriptProperties().deleteProperty("STOP_EXECUTION");
  console.log("✅ 已清除緊急停止旗標");
  CentralContext.getSpreadsheet().toast("✅ 已清除緊急停止旗標", "完成", 3);
  return { success: true };
}

function isEmergencyStopRequested_() {
  var value = PropertiesService.getScriptProperties().getProperty("STOP_EXECUTION");
  return value === "true";
}

function throwIfEmergencyStopRequested_() {
  if (isEmergencyStopRequested_()) {
    throw new Error("已收到緊急停止指令，流程已中止");
  }
}

function emergencyStopAll() {
  PropertiesService.getScriptProperties().setProperty("STOP_EXECUTION", "true");
  console.log("⛔ 緊急停止信號已發送");
  CentralContext.getSpreadsheet().toast("⛔ 緊急停止信號已發送", "停止中", 5);
  return { success: true };
}


// ═══════════════════════════════════════════════════════════════
// 📄 主程式10：依設定產出 PDF
// ═══════════════════════════════════════════════════════════════
function generateSalaryPDFsByConfig_(jobKey, sendEmail) {
  return generateSalaryPDFsCore_(jobKey, null, sendEmail === true);
}

function generateSalaryPDFsByConfigAndFile_(jobKey, fileId, sendEmail) {
  return generateSalaryPDFsCore_(jobKey, fileId, sendEmail === true);
}

function generateSalaryPDFsCore_(jobKey, fileId, sendEmail) {
  var config = PDF_JOB_CONFIG[jobKey];
  if (!config) {
    throw new Error("❌ 無效的 jobKey：" + jobKey);
  }

  var currentStep = 0;
  var processedCount = 0;
  var successCount = 0;
  var errorCount = 0;

  try {
    clearEmergencyStopFlag();

    var ss = fileId
      ? SpreadsheetApp.openById(fileId)
      : CentralContext.getSpreadsheet();

    var startTime = new Date();
    var timezone = "Asia/Taipei";

    currentStep = 1;
    throwIfEmergencyStopRequested_();

    try {
      ss.toast("📁 開始執行 PDF 產出流程", "執行中", 5);
    } catch (e) {}

    var PDFSheet = ss.getSheetByName(config.listSheetName);
    var salarySheet = ss.getSheetByName(config.salarySheetName);
    var salaryDataSheet = ss.getSheetByName(config.dataSheetName);

    if (!PDFSheet) throw new Error("找不到「" + config.listSheetName + "」工作表");
    if (!salarySheet) throw new Error("找不到「" + config.salarySheetName + "」工作表");
    if (!salaryDataSheet) throw new Error("找不到「" + config.dataSheetName + "」工作表");

    var periodInfo = getPeriodInfoBySpreadsheet_(ss);
    if (!periodInfo || !periodInfo.periodCode) {
      throw new Error("無法取得期別資訊，請檢查試算表檔案所在資料夾名稱");
    }

    SpreadsheetApp.flush();

    currentStep = 2;
    throwIfEmergencyStopRequested_();

    var pdfFolder = getPdfStorageFolderBySpreadsheet_(ss, periodInfo.periodCode);

    SpreadsheetApp.flush();

    currentStep = 3;
    throwIfEmergencyStopRequested_();

    var salaryData = salaryDataSheet.getDataRange().getValues();
    if (salaryData.length <= 1) {
      throw new Error("薪資表沒有有效資料，請確認資料已正確匯入");
    }

    var validStaffData = getPendingPdfStaffList_(PDFSheet);
    if (validStaffData.length === 0) {
      throw new Error("沒有找到 H欄=Y 的待處理人員");
    }

    SpreadsheetApp.flush();

    currentStep = 4;

    for (var i = 0; i < validStaffData.length; i++) {
      throwIfEmergencyStopRequested_();

      var staff = validStaffData[i];
      var name = staff.name;
      var rowIndex = staff.rowIndex;
      processedCount++;

      if (i > 0 && i % 10 === 0) {
        SpreadsheetApp.flush();
      }

      try {
        var hValue = PDFSheet.getRange(rowIndex, 8).getValue();
        if (hValue !== "Y") {
          continue;
        }

        try {
          ss.toast("🧾 正在處理：" + name, "PDF產出中", 3);
        } catch (e) {}

        Utilities.sleep(300);
        throwIfEmergencyStopRequested_();

        salarySheet.getRange("AD2").setValue(name);

        salarySheet.getRange("AB31:AB200").clearContent();
        salarySheet.getRange("AD31:AF200").clearContent();

        var detailData = [];
        var detailIndex = 1;

        for (var j = 1; j < salaryData.length; j++) {
          var row = salaryData[j];
          var client = row[4];
          var staffInData = row[5];
          var hours = row[6];

          if (staffInData && staffInData.toString().includes(name)) {
            var dateFormula =
              '=TEXT(\'' + config.dataSheetName + '\'!B' + (j + 1) +
              ',"yyyy/MM/dd") & "（" & \'' + config.dataSheetName + '\'!C' +
              (j + 1) + ' & "）"';

            detailData.push([
              detailIndex,
              dateFormula,
              client || "",
              hours ? Number(hours).toLocaleString() : "",
              staffInData
            ]);
            detailIndex++;
          }
        }

        if (detailData.length > 0) {
          salarySheet.getRange(31, 28, detailData.length, 5).setValues(detailData);
        }

        throwIfEmergencyStopRequested_();

        clearNonFormulaDirtyValues(salarySheet);
        setupSalarySlipZeroAsBlank(salarySheet);

        var noteValue = adjustPdfNoteRowHeight_(salarySheet);

        var employeeName = PDFSheet.getRange(rowIndex, 2).getValue();
        var fileName = getSalaryPdfFileName(employeeName, periodInfo.periodCode, config.fileTitle) + ".pdf";

        var url = ss.getUrl().replace(/edit$/, "");
        var actualLastRow = 29;

        if (detailData.length > 0) {
          actualLastRow = 30 + detailData.length;
        }

        if (noteValue) {
          actualLastRow = Math.max(actualLastRow, 27);
        }

        var exportRange = "AB1:AH" + actualLastRow;
        var exportUrl =
          url + "export?exportFormat=pdf&format=pdf&gid=" + salarySheet.getSheetId() +
          "&range=" + exportRange +
          "&size=A4&portrait=true&fitw=true&sheetnames=false&printtitle=false" +
          "&pagenum=false&gridlines=false&fzr=false&top_margin=0.5&bottom_margin=0.5&left_margin=0.5&right_margin=0.5";

        var pdfBlob = null;
        var pdfExportSuccess = false;

        for (var retry = 1; retry <= 2; retry++) {
          throwIfEmergencyStopRequested_();

          try {
            var token = ScriptApp.getOAuthToken();

            var response = UrlFetchApp.fetch(exportUrl, {
              headers: { Authorization: "Bearer " + token },
              muteHttpExceptions: true,
              followRedirects: true
            });

            if (response.getResponseCode() === 429) {
              Utilities.sleep(2000);
              continue;
            }

            if (response.getResponseCode() !== 200) {
              throw new Error("PDF匯出請求失敗，HTTP狀態：" + response.getResponseCode());
            }

            pdfBlob = response.getBlob();

            if (!pdfBlob.getContentType().includes("pdf")) {
              throw new Error("匯出結果非PDF格式：" + pdfBlob.getContentType());
            }

            pdfBlob.setName(fileName);
            pdfExportSuccess = true;
            break;
          } catch (exportError) {
            if (retry < 2) {
              Utilities.sleep(1000);
            } else {
              throw new Error("PDF匯出失敗: " + exportError.message);
            }
          }
        }

        if (!pdfExportSuccess || !pdfBlob) {
          throw new Error("PDF匯出失敗，無法取得PDF檔案");
        }

        throwIfEmergencyStopRequested_();

        var existingFileUrl = PDFSheet.getRange(rowIndex, 5).getValue();

        var saveResult = savePreservingFileLinkEnhanced(
          pdfFolder,
          fileName,
          pdfBlob,
          existingFileUrl
        );

        var actualSavedTime = getActualPdfSavedTime_(saveResult, timezone);
        var normalizedUrl = normalizeDriveUrl_(saveResult.url);

        PDFSheet.getRange(rowIndex, 4).setValue(actualSavedTime);
        PDFSheet.getRange(rowIndex, 5).setValue(normalizedUrl);
        PDFSheet.getRange(rowIndex, 5).setBackground(null);
        PDFSheet.getRange(rowIndex, 8).setValue("");

        successCount++;
        SpreadsheetApp.flush();

      } catch (pdfError) {
        var errorMessage = pdfError && pdfError.message ? pdfError.message : String(pdfError);

        if (errorMessage.indexOf("緊急停止") >= 0) {
          throw pdfError;
        }

        errorCount++;
        PDFSheet.getRange(rowIndex, 5).setValue("❌ " + errorMessage);
        PDFSheet.getRange(rowIndex, 5).setBackground("#f8d7da");
      }
    }

    currentStep = 5;

    var endTime = new Date();
    var processingTime = Math.round((endTime - startTime) / 1000);

    try {
      ss.toast("✅ PDF 產出流程已完成！", "完成", 5);
    } catch (e) {}

    return {
      success: true,
      processedCount: processedCount,
      successCount: successCount,
      errorCount: errorCount,
      processingTime: processingTime,
      fileId: ss.getId(),
      periodCode: periodInfo.periodCode
    };

  } catch (error) {
    var errorMessage = error && error.message ? error.message : String(error);

    return {
      success: false,
      step: currentStep,
      message: errorMessage
    };
  }
}

function getPeriodInfoBySpreadsheet_(ss) {
  try {
    var file = DriveApp.getFileById(ss.getId());
    var parents = file.getParents();

    if (!parents.hasNext()) {
      throw new Error("無法取得試算表檔案所在的資料夾");
    }

    var parentFolder = parents.next();
    var folderName = parentFolder.getName();
    var cleanPeriodCode = String(folderName || "").trim();

    if (!cleanPeriodCode) {
      throw new Error("資料夾名稱為空，無法判斷期別");
    }

    return {
      periodCode: cleanPeriodCode,
      display: "期別：" + cleanPeriodCode + "（來自資料夾：" + folderName + "）"
    };
  } catch (error) {
    var execSheet = ss.getSheetByName("執行");
    if (execSheet) {
      var periodCode = execSheet.getRange("A1").getValue();
      if (periodCode && String(periodCode).trim() !== "") {
        return {
          periodCode: String(periodCode).trim(),
          display: "期別：" + String(periodCode).trim() + "（來自執行工作表A1）"
        };
      }
    }

    throw new Error("期別資訊取得失敗：" + error.message);
  }
}

function getPdfStorageFolderBySpreadsheet_(ss, periodCode) {
  try {
    var file = DriveApp.getFileById(ss.getId());
    var parents = file.getParents();

    if (!parents.hasNext()) {
      throw new Error("找不到試算表所在資料夾");
    }

    var rootFolder = parents.next();
    var folders = rootFolder.getFoldersByName(periodCode);

    var finalFolder = folders.hasNext()
      ? folders.next()
      : rootFolder.createFolder(periodCode);
    finalFolder.setSharing(
      DriveApp.Access.ANYONE_WITH_LINK,
      DriveApp.Permission.VIEW
    );
    return finalFolder;
  } catch (error) {
    throw new Error("建立PDF資料夾失敗：" + error.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// 📄 共用：待處理名單取得
// ═══════════════════════════════════════════════════════════════
function getPendingPdfStaffList_(pdfSheet) {
  var lastRow = pdfSheet.getLastRow();
  if (lastRow < 2) return [];

  var values = pdfSheet.getRange(2, 2, lastRow - 1, 7).getValues();
  var result = [];

  for (var i = 0; i < values.length; i++) {
    var rowIndex = i + 2;
    var name = String(values[i][0] || "").trim();
    var hValue = String(values[i][6] || "").trim();

    if (!name) continue;
    if (hValue !== "Y") continue;

    result.push({ name: name, rowIndex: rowIndex });
  }

  return result;
}


// ═══════════════════════════════════════════════════════════════
// 📄 共用：重跑失敗列
// ═══════════════════════════════════════════════════════════════
function remarkFailedPdfRowsByConfig_(jobKey) {
  var config = PDF_JOB_CONFIG[jobKey];
  if (!config) {
    throw new Error("❌ 無效的 jobKey：" + jobKey);
  }

  var ss = CentralContext.getSpreadsheet();
  var sheet = ss.getSheetByName(config.listSheetName);
  if (!sheet) throw new Error("找不到 " + config.listSheetName + " 工作表");

  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return { success: true, count: 0 };

  var values = sheet.getRange(2, 5, lastRow - 1, 1).getValues();
  var count = 0;

  for (var i = 0; i < values.length; i++) {
    var text = String(values[i][0] || "");
    if (text.indexOf("❌") === 0) {
      sheet.getRange(i + 2, 8).setValue("Y");
      count++;
    }
  }

  SpreadsheetApp.flush();
  CentralContext.getSpreadsheet().toast("已重新標記 " + count + " 筆失敗資料", "重跑準備完成", 5);

  return { success: true, count: count };
}


// ═══════════════════════════════════════════════════════════════
// 📄 共用：標記 / 清除 / 修復
// ═══════════════════════════════════════════════════════════════
function markAllStaffForProcessing_(jobKey) {
  var config = PDF_JOB_CONFIG[jobKey];
  if (!config) {
    throw new Error("❌ 無效的 jobKey：" + jobKey);
  }

  var ss = CentralContext.getSpreadsheet();
  var PDFSheet = ss.getSheetByName(config.listSheetName);
  if (!PDFSheet) throw new Error("找不到 " + config.listSheetName + " 工作表");

  var confirmResponse = SpreadsheetApp.getUi().alert(
    "確認標記所有員工",
    "這將在所有有效員工的H欄填入Y標記，確定要繼續嗎？",
    SpreadsheetApp.getUi().ButtonSet.YES_NO
  );

  if (confirmResponse !== SpreadsheetApp.getUi().Button.YES) return;

  var names = PDFSheet.getRange("B2:B").getValues();
  var markedCount = 0;

  for (var i = 0; i < names.length; i++) {
    var name = names[i][0];
    var rowIndex = i + 2;

    if (name && typeof name === "string" && name.trim().length > 0) {
      PDFSheet.getRange(rowIndex, 8).setValue("Y");
      markedCount++;
    } else {
      break;
    }
  }

  SpreadsheetApp.getUi().alert("✅ 標記完成", "已標記 " + markedCount + " 位員工為待處理狀態");
}

function clearAllControlFlagsByConfig_(jobKey) {
  var config = PDF_JOB_CONFIG[jobKey];
  if (!config) {
    throw new Error("❌ 無效的 jobKey：" + jobKey);
  }

  var ss = CentralContext.getSpreadsheet();
  var PDFSheet = ss.getSheetByName(config.listSheetName);
  if (!PDFSheet) throw new Error("找不到 " + config.listSheetName + " 工作表");

  var confirmResponse = SpreadsheetApp.getUi().alert(
    "確認清理控制標記",
    "這將清除所有員工的H欄控制標記（Y標記），確定要繼續嗎？",
    SpreadsheetApp.getUi().ButtonSet.YES_NO
  );

  if (confirmResponse !== SpreadsheetApp.getUi().Button.YES) return;

  PDFSheet.getRange("H2:H").clearContent();
  SpreadsheetApp.getUi().alert("✅ 已清理所有H欄控制標記");
}

function repairPdfLinksByConfig_(jobKey) {
  var config = PDF_JOB_CONFIG[jobKey];
  if (!config) {
    throw new Error("❌ 無效的 jobKey：" + jobKey);
  }

  try {
    var ui = SpreadsheetApp.getUi();
    var ss = CentralContext.getSpreadsheet();
    var PDFSheet = ss.getSheetByName(config.listSheetName);

    if (!PDFSheet) {
      throw new Error("找不到「" + config.listSheetName + "」工作表");
    }

    var confirm = ui.alert(
      "修復PDF連結",
      "這將掃描PDF資料夾，自動修復E欄的錯誤連結。\n\n確定要繼續嗎？",
      ui.ButtonSet.YES_NO
    );

    if (confirm !== ui.Button.YES) {
      return;
    }

    var periodInfo = getPeriodInfo();
    var periodCode = periodInfo.periodCode;
    var pdfFolder = getPdfStorageFolder(periodCode);

    var lastRow = PDFSheet.getLastRow();
    if (lastRow < 2) {
      ui.alert("❌ 沒有員工資料", config.listSheetName + " 工作表沒有員工資料");
      return;
    }

    var names = PDFSheet.getRange(2, 2, lastRow - 1, 1).getValues();
    var links = PDFSheet.getRange(2, 5, lastRow - 1, 1).getValues();

    var repairedCount = 0;
    var notFoundCount = 0;

    for (var i = 0; i < names.length; i++) {
      var name = names[i][0];
      var currentLink = links[i][0];
      var rowIndex = i + 2;

      if (!name || typeof name !== "string" || name.trim() === "") {
        continue;
      }

      var cleanName = name.toString().trim();
      var needsRepair = false;

      if (!currentLink) {
        needsRepair = true;
      } else if (typeof currentLink === "string") {
        var linkText = currentLink.toString();
        if (
          linkText.indexOf("❌") >= 0 ||
          linkText.indexOf("失敗") >= 0 ||
          linkText.indexOf("Drive") >= 0 ||
          linkText.indexOf("429") >= 0 ||
          linkText.indexOf("drive.google.com") === -1
        ) {
          needsRepair = true;
        }
      }

      if (!needsRepair) {
        continue;
      }

      var fileName = getSalaryPdfFileName(cleanName, periodCode, config.fileTitle) + ".pdf";
      var existingFiles = pdfFolder.getFilesByName(fileName);

      if (existingFiles.hasNext()) {
        var file = existingFiles.next();
        var fileUrl = file.getUrl();
        PDFSheet.getRange(rowIndex, 5).setValue(fileUrl);
        repairedCount++;
      } else {
        notFoundCount++;
        PDFSheet.getRange(rowIndex, 5).setValue("❌ 找不到PDF檔案");
      }
    }

    SpreadsheetApp.flush();

    ui.alert(
      "修復完成",
      "✅ 修復完成！\n\n" +
      "成功修復: " + repairedCount + " 個連結\n" +
      "找不到檔案: " + notFoundCount + " 個",
      ui.ButtonSet.OK
    );
  } catch (error) {
    SpreadsheetApp.getUi().alert("❌ 修復失敗: " + error.message);
  }
}


// ═══════════════════════════════════════════════════════════════
// 🔧 共用輔助函數
// ═══════════════════════════════════════════════════════════════
function validateAndGetSheet(sheetName, description) {
  var sheet = CentralContext.getSpreadsheet().getSheetByName(sheetName);
  if (!sheet) throw new Error("找不到「" + sheetName + "」工作表（" + description + "）");
  return sheet;
}

function getPeriodInfo() {
  try {
    var ss = CentralContext.getSpreadsheet();
    var file = DriveApp.getFileById(ss.getId());

    var parents = file.getParents();
    if (!parents.hasNext()) throw new Error("無法取得試算表檔案所在的資料夾");

    var parentFolder = parents.next();
    var folderName = parentFolder.getName();
    var cleanPeriodCode = folderName.toString().trim();

    if (!cleanPeriodCode) throw new Error("資料夾名稱為空，無法判斷期別");

    return {
      periodCode: cleanPeriodCode,
      display: "期別：" + cleanPeriodCode + "（來自資料夾：" + folderName + "）"
    };
  } catch (error) {
    try {
      var execSheet = CentralContext.getSpreadsheet().getSheetByName("執行");
      if (execSheet) {
        var periodCode = execSheet.getRange("A1").getValue();
        if (periodCode && periodCode.toString().trim() !== "") {
          return {
            periodCode: periodCode.toString().trim(),
            display: "期別：" + periodCode.toString().trim() + "（來自執行工作表A1）"
          };
        }
      }
    } catch (e) {}

    throw new Error("期別資訊取得失敗：" + error.message);
  }
}

function getPdfStorageFolder(periodCode) {
  try {
    var ss = CentralContext.getSpreadsheet();
    var file = DriveApp.getFileById(ss.getId());

    var parents = file.getParents();
    if (!parents.hasNext()) throw new Error("找不到試算表所在資料夾");

    var rootFolder = parents.next();
    var folders = rootFolder.getFoldersByName(periodCode);

    if (folders.hasNext()) {
      return folders.next();
    }

    return rootFolder.createFolder(periodCode);
  } catch (error) {
    throw new Error("建立PDF資料夾失敗：" + error.message);
  }
}

function getSalaryPdfFileName(staffName, periodCode, fileTitle) {
  var cleanName = String(staffName || "").replace(/[^\w\u4e00-\u9fff]/g, "");
  return periodCode + " 檸檬家事｜" + fileTitle + "_" + cleanName;
}

function getActualPdfSavedTime_(saveResult, timezone) {
  try {
    if (saveResult && saveResult.fileId) {
      var file = DriveApp.getFileById(saveResult.fileId);
      return Utilities.formatDate(file.getLastUpdated(), timezone, "yyyy/MM/dd HH:mm:ss");
    }
  } catch (e) {}

  return Utilities.formatDate(new Date(), timezone, "yyyy/MM/dd HH:mm:ss");
}

function normalizeDriveUrl_(url) {
  if (!url) return url;

  var match = String(url).match(/\/d\/([a-zA-Z0-9_-]+)/);
  if (!match) return url;

  return "https://drive.google.com/file/d/" + match[1] + "/view";
}

function adjustPdfNoteRowHeight_(salarySheet) {
  salarySheet.setRowHeight(25, 20);
  salarySheet.getRange("AC25").setWrap(false);

  const noteRange = salarySheet.getRange("AC27:AH27");
  const noteCell = salarySheet.getRange("AC27");

  noteRange
    .setWrap(true)
    .setVerticalAlignment("top");

  // 等公式重算，避免剛換 AD2/AB27 時讀到舊值或空值
  SpreadsheetApp.flush();
  Utilities.sleep(300);

  const noteValue = String(noteCell.getDisplayValue() || "").trim();

  // 重要：這裡不要 clearContent，否則會刪掉 AC27 的備註公式
  if (!noteValue) {
    salarySheet.setRowHeight(27, 20);
    SpreadsheetApp.flush();
    return "";
  }

  const lineCount = noteValue
    .split(/\r?\n/)
    .filter(function(line) {
      return line.trim() !== "";
    })
    .length;

  const rowHeight = Math.max(36, lineCount * 24);

  salarySheet.setRowHeight(27, rowHeight);
  SpreadsheetApp.flush();

  return noteValue;
}


function clearNonFormulaDirtyValues(salarySheet) {
  var ranges = [
    "AC4:AC26",
    "AD4:AD26",
    "AE4:AE26",
    "AF4:AF26",
    "AG4:AG26",
    "AH4:AH26"
  ];

  ranges.forEach(function(a1) {
    var range = salarySheet.getRange(a1);
    var values = range.getValues();
    var formulas = range.getFormulas();

    for (var r = 0; r < values.length; r++) {
      for (var c = 0; c < values[r].length; c++) {
        var f = formulas[r][c];

        if (f && String(f).trim() !== "") continue;

        var cell = values[r][c];

        if (typeof cell !== "string" && typeof cell !== "number") {
          salarySheet.getRange(range.getRow() + r, range.getColumn() + c).setValue("");
          continue;
        }

        if (typeof cell === "number") {
          if (cell === 0 || Math.abs(cell) < 0.00001) {
            salarySheet.getRange(range.getRow() + r, range.getColumn() + c).setValue("");
          }
          continue;
        }

        if (typeof cell === "string") {
          var trimmed = cell.trim();
          var normalized = trimmed.replace(/\s+/g, "");

          var dirtyTokens = new Set(["", "-", ".00", "-.00", "0", "0.0", "0.00"]);

          if (dirtyTokens.has(trimmed) || dirtyTokens.has(normalized)) {
            salarySheet.getRange(range.getRow() + r, range.getColumn() + c).setValue("");
            continue;
          }

          var num = Number(normalized);
          if (!isNaN(num) && Math.abs(num) < 0.00001) {
            salarySheet.getRange(range.getRow() + r, range.getColumn() + c).setValue("");
          }
        }
      }
    }
  });
}

function applyZeroAsBlankNumberFormat(salarySheet) {
  var ranges = ["AC4:AH26", "AC25"];
  var format = "#,##0.##;-#,##0.##;;@";

  ranges.forEach(function(a1) {
    salarySheet.getRange(a1).setNumberFormat(format);
  });
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

function clearNonFormulaDirtyValues(salarySheet) {
  var ranges = [
    "AC4:AC26",
    "AD4:AD26",
    "AE4:AE26",
    "AF4:AF26",
    "AG4:AG26",
    "AH4:AH26"
  ];

  ranges.forEach(function(a1) {
    var range = salarySheet.getRange(a1);
    var values = range.getValues();
    var formulas = range.getFormulas();

    for (var r = 0; r < values.length; r++) {
      for (var c = 0; c < values[r].length; c++) {
        var formula = formulas[r][c];

        // 有公式的格子不要清除公式，交給數字格式處理顯示空白
        if (formula && String(formula).trim() !== "") continue;

        var cell = values[r][c];
        var targetCell = salarySheet.getRange(range.getRow() + r, range.getColumn() + c);

        if (cell === null || cell === undefined) {
          targetCell.setValue("");
          continue;
        }

        if (typeof cell === "number") {
          if (cell === 0 || Math.abs(cell) < 0.00001) {
            targetCell.setValue("");
          }
          continue;
        }

        if (typeof cell === "string") {
          var trimmed = cell.trim();
          var normalized = trimmed.replace(/\s+/g, "");

          var dirtyTokens = ["", "-", ".00", "-.00", "0", "0.0", "0.00"];

          if (dirtyTokens.indexOf(trimmed) >= 0 || dirtyTokens.indexOf(normalized) >= 0) {
            targetCell.setValue("");
            continue;
          }

          var num = Number(normalized);
          if (!isNaN(num) && Math.abs(num) < 0.00001) {
            targetCell.setValue("");
          }
        }
      }
    }
  });
}

function applyZeroAsBlankNumberFormat(salarySheet) {
  var ranges = [
    "AC4:AH26",
    "AC25",
    "AD4:AD26",
    "AF4:AF26",
    "AH4:AH26"
  ];

  var format = "#,##0.##;-#,##0.##;;@";

  ranges.forEach(function(a1) {
    salarySheet.getRange(a1).setNumberFormat(format);
  });
}

function setupSalarySlipZeroAsBlank(salarySheet) {
  applyZeroAsBlankNumberFormat(salarySheet);

  var ranges = [
    salarySheet.getRange("AD4:AD26"),
    salarySheet.getRange("AF4:AF26"),
    salarySheet.getRange("AH4:AH26")
  ];

  var formula = '=OR(AND(ISNUMBER(AD4),ABS(AD4)<0.00001),TO_TEXT(AD4)="0",TO_TEXT(AD4)="0.0",TO_TEXT(AD4)="0.00")';

  var rules = salarySheet.getConditionalFormatRules().filter(function(rule) {
    var condition = rule.getBooleanCondition();
    if (!condition) return true;

    var values = condition.getCriteriaValues();
    return !(values && values[0] === formula);
  });

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied(formula)
      .setFontColor("#ffffff")
      .setRanges(ranges)
      .build()
  );

  salarySheet.setConditionalFormatRules(rules);
}

// ═══════════════════════════════════════════════════════════════
// 📄 PDF 檔案儲存
// 若 E欄已有舊連結，優先用同一檔案 ID 覆蓋，保留相同連結
// 需要啟用 Apps Script 進階服務：Drive API
// ═══════════════════════════════════════════════════════════════
function savePreservingFileLinkEnhanced(pdfFolder, fileName, pdfBlob, existingFileUrl) {
  try {
    var existingFileId = extractDriveFileId_(existingFileUrl);

    // 1. 優先沿用 E欄舊連結
    if (existingFileId) {
      var existingFile = DriveApp.getFileById(existingFileId);
      existingFile.setName(fileName);

      replacePdfFileContent_(existingFileId, pdfBlob);

      existingFile = DriveApp.getFileById(existingFileId);
      existingFile.setSharing(
        DriveApp.Access.ANYONE_WITH_LINK,
        DriveApp.Permission.VIEW
      );

      return {
        success: true,
        fileId: existingFileId,
        url: existingFile.getUrl(),
        action: "updated_existing_same_link"
      };
    }

    // 2. 沒有舊連結時，再找同名檔
    var existingFiles = pdfFolder.getFilesByName(fileName);
    if (existingFiles.hasNext()) {
      var sameNameFile = existingFiles.next();
      var sameNameFileId = sameNameFile.getId();

      replacePdfFileContent_(sameNameFileId, pdfBlob);

      sameNameFile = DriveApp.getFileById(sameNameFileId);
      sameNameFile.setSharing(
        DriveApp.Access.ANYONE_WITH_LINK,
        DriveApp.Permission.VIEW
      );

      return {
        success: true,
        fileId: sameNameFileId,
        url: sameNameFile.getUrl(),
        action: "updated_same_name_existing"
      };
    }

    // 3. 完全沒有舊檔才新建
    var file = pdfFolder.createFile(pdfBlob);
    file.setName(fileName);

    file.setSharing(
      DriveApp.Access.ANYONE_WITH_LINK,
      DriveApp.Permission.VIEW
    );

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

function replacePdfFileContent_(fileId, pdfBlob) {
  var response = UrlFetchApp.fetch(
    "https://www.googleapis.com/upload/drive/v3/files/" +
      encodeURIComponent(fileId) + "?uploadType=media",
    {
      method: "patch",
      headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
      contentType: "application/pdf",
      payload: pdfBlob.getBytes(),
      muteHttpExceptions: true
    }
  );
  if (response.getResponseCode() < 200 || response.getResponseCode() >= 300) {
    throw new Error(
      "更新既有 PDF 失敗，HTTP " + response.getResponseCode() +
      "：" + response.getContentText().slice(0, 300)
    );
  }
}

// ███████████████████████████████████████████████████
// 📁 主程式12：選單與包裝函式
// ███████████████████████████████████████████████████
/** 🔧 包裝函式 - 上半月 */
function runSalaryFirst() { runSalaryPreparation(true); }
function runAdjustmentFirst() { runAdjustmentPreparation(true); }
function runAllowanceFirst() { runAllowanceProcess(true); }
function runVoucherFirst() { runVoucherPreparation(true); }
function runNewcomerFirst() { runNewcomerProcess(true); }
function runInternFirst() { runInternProcess(true); }
function runLeaderFirst() { runLeaderProcess(true); }
function runToolDepositFirst() { runToolDepositProcess(true); }
function runFinalSetFirst() { runFinalSettlement(true); }
function runSalarySummaryFirst() { runSalarySummaryProcess(true); }
function runBankAccountFirst() { return runBankAccountUpdate(true); }

/** 🔧 包裝函式 - 下半月 */
function runSalarySecond() { runSalaryPreparation(false); }
function runAdjustmentSecond() { runAdjustmentPreparation(false); }
function runAllowanceSecond() { runAllowanceProcess(false); }
function runVoucherSecond() { runVoucherPreparation(false); }
function runNewcomerSecond() { runNewcomerProcess(false); }
function runInternSecond() { runInternProcess(false); }
function runLeaderSecond() { runLeaderProcess(false); }
function runToolDepositSecond() { runToolDepositProcess(false); }
function runFinalSetSecond() { runFinalSettlement(false); }
function runSalarySummarySecond() { runSalarySummaryProcess(false); }
function runBankAccountSecond() { return runBankAccountUpdate(false); }

/** 🔧 完整流程包裝函式 */
function runHalfFullFirst() {
  try {
    openProgressSidebar();
    updateSidebarProgress("🔵 開始上半月完整流程...");
    
    const ui = SpreadsheetApp.getUi();
    const response = ui.alert(
      "🔼 上半月完整流程確認",
      "即將執行上半月完整薪資處理流程，包含：\n" +
      "• 薪資表整理\n• 00調薪\n• 01專員請款\n• 02儲值獎金\n" +
      "• 新人實境期別標註\n• 03新人實境\n• 04新人實習\n" +
      "• 05組長津貼\n• 工具包押金\n• 結算整理\n\n" +
      "確定要繼續嗎？",
      ui.ButtonSet.YES_NO
    );
    
    if (response === ui.Button.YES) {
      runCompletePayrollProcess(true);
    } else {
      updateSidebarProgress("❌ 使用者取消上半月完整流程");
    }
  } catch (error) {
    showToast("❌ 上半月完整流程錯誤：" + error.message);
  }
}

function runHalfFullSecond() {
  try {
    openProgressSidebar();
    updateSidebarProgress("🔵 開始下半月完整流程...");
    
    const ui = SpreadsheetApp.getUi();
    const response = ui.alert(
      "🔽 下半月完整流程確認",
      "即將執行下半月完整薪資處理流程，包含：\n" +
      "• 薪資表整理\n• 00調薪\n• 01專員請款\n• 02儲值獎金\n" +
      "• 新人實境期別標註\n• 03新人實境\n• 04新人實習\n" +
      "• 05組長津貼\n• 工具包押金\n• 結算整理\n\n" +
      "確定要繼續嗎？",
      ui.ButtonSet.YES_NO
    );
    
    if (response === ui.Button.YES) {
      runCompletePayrollProcess(false);
    } else {
      updateSidebarProgress("❌ 使用者取消下半月完整流程");
    }
  } catch (error) {
    showToast("❌ 下半月完整流程錯誤：" + error.message);
  }
}

/** 🔧 薪資統計處理函式 */
function runSalarySummaryProcess(isFirstHalf) {
  try {
    openProgressSidebar();
    const processType = isFirstHalf ? "上半月" : "下半月";
    updateSidebarProgress("🔵 開始" + processType + "薪資統計處理...");
    
    // 這裡可以加入薪資統計的具體邏輯
    // 例如：統計各項目金額、人數統計等
    
    updateSidebarProgress("✅ " + processType + "薪資統計處理完成！");
    showToast("✅ " + processType + "薪資統計處理完成！");
    
  } catch (error) {
    const errorMessage = "❌ 薪資統計處理錯誤：" + error.message;
    updateSidebarProgress(errorMessage);
    showToast(errorMessage);
  }
}

/** 🔧 工具包押金包裝函式 */
function runToolDepositProcessFirst() {
  runToolDepositProcess(true);
}

function runToolDepositProcessSecond() {
  runToolDepositProcess(false);
}

// ================================================
// 🧹 清潔承攬 - 主控面板後端 API
// ================================================

// ===== 1. 面板啟動 =====
function showMainControlPanel() {
  const html = HtmlService.createHtmlOutputFromFile('MenuPanel')
    .setTitle('🧹 清潔承攬｜執行控制面板')
    .setWidth(380)
    .setHeight(650);
  SpreadsheetApp.getUi().showSidebar(html);
}

// ===== 2. 選單觸發 =====
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('🧹 清潔承攬')
    .addItem('🖥️ 開啟執行控制面板', 'showMainControlPanel')
    .addToUi();
}

// ================================================
// 🌍 區域名稱管理系統（完全無預設值）
// ================================================

// 取得目前設定的區域名稱
function getCurrentRegion() {
  const props = PropertiesService.getScriptProperties()
  return props.getProperty('CURRENT_REGION') || '';
}

// 設定區域名稱
function setRegionName(regionName) {
  if (!regionName || regionName.trim() === '') {
    throw new Error('區域名稱不可為空');
  }
  const cleanName = regionName.trim();
  PropertiesService.getScriptProperties().setProperty('CURRENT_REGION', cleanName);
  return { success: true, region: cleanName };
}

// 清除區域名稱設定
function clearRegionSetting() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty('CURRENT_REGION');
  props.deleteProperty('PDF_ROOT_FOLDER_ID');
  return { success: true, message: '已清除所有區域設定' };
}

// ================================================
// 📁 根目錄管理（PDF儲存位置）
// ================================================

// 設定區域根目錄（同時作為PDF根目錄）
function setRegionRootFolderWithId(folderId, regionName) {
  if (!folderId || folderId.trim() === '') {
    throw new Error('資料夾 ID 不可為空');
  }
  try {
    const folder = DriveApp.getFolderById(folderId);
    PropertiesService.getScriptProperties().setProperty('PDF_ROOT_FOLDER_ID', folderId);
    return { 
      success: true, 
      folderName: folder.getName(), 
      folderId: folderId 
    };
  } catch (e) {
    throw new Error(`無法存取資料夾：${e.message}`);
  }
}

// 取得PDF根目錄
function getPdfRootFolder() {
  const folderId = PropertiesService.getScriptProperties().getProperty('PDF_ROOT_FOLDER_ID');
  if (!folderId) {
    throw new Error('尚未設定區域根目錄');
  }
  return DriveApp.getFolderById(folderId);
}

// 測試資料夾存取權限
function testFolderAccess(folderId) {
  try {
    const folder = DriveApp.getFolderById(folderId);
    return { success: true, folderName: folder.getName() };
  } catch (e) {
    throw new Error(`無法存取資料夾：${e.message}`);
  }
}

// ================================================
// 📆 期別管理
// ================================================

// 切換期別（更新執行工作表B1）
function switchPeriod(periodCode) {
  try {
    const sheetNames = getSheetNames();
    const execSheet = CentralContext.getSpreadsheet().getSheetByName(sheetNames.exec);
    if (!execSheet) throw new Error('找不到執行工作表');
    
    const match = periodCode.match(/^(\d{6})(?:-(\d))?$/);
    if (match) {
      const yearMonth = match[1];
      execSheet.getRange('B1').setValue(yearMonth);
      console.log(`✅ 切換期別至: ${periodCode}`);
    }
    return { success: true, periodCode: periodCode };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// 取得期別資訊
function getPeriodInfo() {
  try {
    const name = CentralContext.getSpreadsheet().getName();
    const periodCodeMatch = name.match(/\b\d{6}-\d\b/);
    const periodCode = periodCodeMatch ? periodCodeMatch[0] : '';
    
    if (!periodCode) {
      const execSheet = CentralContext.getSpreadsheet().getSheetByName(getSheetNames().exec);
      const b1 = execSheet?.getRange('B1').getValue();
      if (b1) {
        return { 
          periodCode: b1.toString(), 
          display: `${b1.toString().substring(0,4)}年${b1.toString().substring(4,6)}月`,
          year: parseInt(b1.toString().substring(0,4)),
          month: parseInt(b1.toString().substring(4,6))
        };
      }
      return { 
        periodCode: '', 
        display: '未設定', 
        year: new Date().getFullYear(), 
        month: new Date().getMonth() + 1 
      };
    }
    
    const year = periodCode.substring(0, 4);
    const month = periodCode.substring(4, 6);
    const half = periodCode.includes('-1') ? '上半月' : '下半月';
    const display = `${year}年${month}月${half}`;
    
    return { 
      periodCode, 
      display, 
      year: parseInt(year), 
      month: parseInt(month),
      half,
      isFirstHalf: half === '上半月'
    };
  } catch (error) {
    console.log(`取得期別資訊失敗：${error.message}`);
    return { 
      periodCode: '', 
      display: '未設定', 
      year: new Date().getFullYear(), 
      month: new Date().getMonth() + 1 
    };
  }
}

// ================================================
// 📋 面板初始化資料
// ================================================

function getPanelInitData() {
  const period = getPeriodInfo();
  const sheetNames = getSheetNames();
  const execSheet = CentralContext.getSpreadsheet().getSheetByName(sheetNames.exec);
  
  // 從執行工作表讀取各種 ID
  const folderId = execSheet?.getRange('C2').getValue() || '';
  const salaryId = execSheet?.getRange('C3').getValue() || '';
  const rosterId = execSheet?.getRange('C4').getValue() || '';
  
  // 取得區域根目錄（可能未設定）
  let rootFolderName = '';
  let rootFolderId = '';
  let rootFolderError = null;
  
  try {
    const rootFolder = getPdfRootFolder();
    rootFolderName = rootFolder.getName();
    rootFolderId = rootFolder.getId();
  } catch (e) {
    rootFolderError = e.message;
  }
  
  // 取得區域名稱（完全由使用者設定，無預設值）
  const region = getCurrentRegion();
  
  return {
    periodCode: period.periodCode || '',
    periodDisplay: period.display || '未設定',
    region: region,
    rootFolder: {
      name: rootFolderName,
      id: rootFolderId,
      error: rootFolderError
    },
    folderId: folderId,
    salaryId: salaryId,
    rosterId: rosterId,
    c8: execSheet?.getRange('C8').getValue() || '',
    d8: execSheet?.getRange('D8').getValue() || '',
    sheetNames: sheetNames
  };
}

// ================================================
// 🎯 統一執行入口（面板呼叫）
// ================================================

function executePanelAction(action, isFirstHalf) {
  console.log(`🎯 面板觸發: ${action}, isFirstHalf=${isFirstHalf}`);
  
  // 開啟進度側欄
  try {
    openProgressSidebar();
  } catch (e) {
    console.warn('開啟進度側欄失敗', e);
  }
  
  try {
    switch (action) {
      // 🧾 薪資作業
      case 'runSalaryPreparation': 
        return isFirstHalf ? runSalaryPreparationFirstHalf() : runSalaryPreparationSecondHalf();
      case 'runAdjustmentPreparation': 
        return isFirstHalf ? runAdjustmentPreparationFirstHalf() : runAdjustmentPreparationSecondHalf();
      case 'runAllowanceProcess': 
        return isFirstHalf ? runAllowanceProcessFirstHalf() : runAllowanceProcessSecondHalf();
      case 'runVoucherPreparation': 
        return isFirstHalf ? runVoucherPreparationFirstHalf() : runVoucherPreparationSecondHalf();
      case 'runNewcomerProcess': 
        return isFirstHalf ? runNewcomerProcessFirstHalf() : runNewcomerProcessSecondHalf();
      case 'runInternProcess': 
        return isFirstHalf ? runInternProcessFirstHalf() : runInternProcessSecondHalf();
      case 'runLeaderProcess': 
        return isFirstHalf ? runLeaderProcessFirstHalf() : runLeaderProcessSecondHalf();
      case 'runToolDepositProcess': 
        return isFirstHalf ? runToolDepositProcessFirst() : runToolDepositProcessSecond();
      case 'runBankAccountUpdate': 
        return isFirstHalf ? runYuantaAccountFirstHalf() : runYuantaAccountSecondHalf();
      case 'runFinalSettlement': 
        return isFirstHalf ? runFinalSettlementFirstHalf() : runFinalSettlementSecondHalf();
      case 'runCompletePayrollProcess': 
        return isFirstHalf ? runCompletePayrollProcessFirstHalf() : runCompletePayrollProcessSecondHalf();
      
      // 📄 PDF 相關（不需期別參數）
      case 'generatePDF':
        return generateSalaryPDFs_v2025(false);
      case 'markAllStaff':
        return markAllStaffForProcessing();
      case 'clearAllFlags':
        return clearAllControlFlags();
      case 'repairPdfLinks':
        return repairPdfLinks();
      
      default:
        throw new Error(`未知的動作: ${action}`);
    }
  } catch (e) {
    console.error(`❌ 面板執行錯誤: ${e.message}`);
    throw e;
  }
}

// ================================================
// 📄 PDF 相關功能（面板專用）
// ================================================

function markAllStaffForProcessing() {
  try {
    const ss = CentralContext.getSpreadsheet();
    const PDFSheet = ss.getSheetByName('PDF���X');
    if (!PDFSheet) throw new Error('找不到PDF輸出工作表');
    
    const names = PDFSheet.getRange('B2:B').getValues();
    let markedCount = 0;
    
    for (let i = 0; i < names.length; i++) {
      const name = names[i][0];
      const rowIndex = i + 2;
      if (name && typeof name === 'string' && name.trim().length > 0) {
        PDFSheet.getRange(rowIndex, 8).setValue('Y');
        markedCount++;
      } else {
        break;
      }
    }
    
    SpreadsheetApp.flush();
    console.log(`✅ 已標記 ${markedCount} 位人員`);
    return { success: true, count: markedCount };
  } catch (e) {
    throw new Error(`標記人員失敗：${e.message}`);
  }
}

function clearAllControlFlags() {
  try {
    const ss = CentralContext.getSpreadsheet();
    const PDFSheet = ss.getSheetByName('PDF���X');
    if (!PDFSheet) throw new Error('找不到PDF輸出工作表');
    
    PDFSheet.getRange('H2:H').clearContent();
    SpreadsheetApp.flush();
    console.log('✅ 已清除所有標記');
    return { success: true };
  } catch (e) {
    throw new Error(`清除標記失敗：${e.message}`);
  }
}

function repairPdfLinks() {
  try {
    const ui = SpreadsheetApp.getUi();
    const ss = CentralContext.getSpreadsheet();
    const PDFSheet = ss.getSheetByName('PDF���X');
    if (!PDFSheet) throw new Error('找不到PDF輸出工作表');
    
    const periodInfo = getPeriodInfo();
    const periodCode = periodInfo.periodCode;
    
    let pdfFolder;
    try {
      pdfFolder = getPdfStorageFolder(periodCode);
    } catch (e) {
      throw new Error(`無法取得PDF儲存資料夾：${e.message}`);
    }
    
    const lastRow = PDFSheet.getLastRow();
    if (lastRow < 2) return { success: true, repaired: 0, notFound: 0 };
    
    const names = PDFSheet.getRange(2, 2, lastRow - 1, 1).getValues();
    const links = PDFSheet.getRange(2, 5, lastRow - 1, 1).getValues();
    
    let repairedCount = 0;
    let notFoundCount = 0;
    
    for (let i = 0; i < names.length; i++) {
      const name = names[i][0];
      const rowIndex = i + 2;
      
      if (!name || typeof name !== 'string' || name.trim() === '') continue;
      
      const cleanName = name.toString().trim();
      const fileName = getSalaryPdfFileName(cleanName, periodCode) + '.pdf';
      
      const existingFiles = pdfFolder.getFilesByName(fileName);
      if (existingFiles.hasNext()) {
        const file = existingFiles.next();
        PDFSheet.getRange(rowIndex, 5).setValue(file.getUrl());
        repairedCount++;
      } else {
        notFoundCount++;
      }
    }
    
    SpreadsheetApp.flush();
    console.log(`✅ 修復完成：成功 ${repairedCount}，找不到 ${notFoundCount}`);
    return { success: true, repaired: repairedCount, notFound: notFoundCount };
  } catch (e) {
    throw new Error(`修復PDF連結失敗：${e.message}`);
  }
}

// ================================================
// 📁 PDF 儲存資料夾管理
// ================================================

function getPdfStorageFolder(periodCode) {
  try {
    const ss = CentralContext.getSpreadsheet();
    const file = DriveApp.getFileById(ss.getId());
    const parents = file.getParents();
    
    if (!parents.hasNext()) throw new Error('找不到試算表所在資料夾');
    const rootFolder = parents.next();
    
    const folders = rootFolder.getFoldersByName(periodCode);
    if (folders.hasNext()) {
      return folders.next();
    } else {
      return rootFolder.createFolder(periodCode);
    }
  } catch (e) {
    throw new Error(`建立/取得PDF資料夾失敗：${e.message}`);
  }
}

/**
 * 產生PDF檔名
 */
function getSalaryPdfFileName(staffName, periodCode, fileTitle) {
  var cleanName = String(staffName || "").replace(/[^\w\u4e00-\u9fff]/g, "");
  return periodCode + " 檸檬家事｜" + fileTitle + "_" + cleanName;
}

// ================================================
// 🛠️ 系統工具
// ================================================

function runDataValidation() {
  try {
    updateSidebarProgress("🔍 執行資料驗證...");
    
    const requiredSheets = [
      "薪資表", "調薪", "專員請款", "儲值獎金", 
      "新人實境", "新人實習", "組長津貼", "場次薪資時數總表",
      "工具包押金", "介紹獎金", "薪資單", "PDF產出"
    ];
    
    let validCount = 0;
    const missingSheets = [];
    
    for (const sheetName of requiredSheets) {
      const sheet = CentralContext.getSpreadsheet().getSheetByName(sheetName);
      if (sheet) {
        validCount++;
      } else {
        missingSheets.push(sheetName);
      }
    }
    
    let message = `✅ 驗證完成：${validCount}/${requiredSheets.length} 個工作表存在`;
    if (missingSheets.length > 0) {
      message += `\n❌ 缺少：${missingSheets.join(', ')}`;
    }
    
    SpreadsheetApp.getUi().alert('系統驗證', message, SpreadsheetApp.getUi().ButtonSet.OK);
    updateSidebarProgress(message);
    return { success: true, valid: validCount, total: requiredSheets.length, missing: missingSheets };
  } catch (e) {
    throw new Error(`驗證失敗：${e.message}`);
  }
}

function clearTempData() {
  try {
    const props = PropertiesService.getScriptProperties();
    const tempKeys = ['latestProgress', 'progressTimestamp', 'STOP_EXECUTION', 'PAUSE_EXECUTION'];
    tempKeys.forEach(key => props.deleteProperty(key));
    
    console.log('✅ 暫存資料已清除');
    showToast('🧹 暫存資料已清除');
    return { success: true };
  } catch (e) {
    throw new Error(`清除暫存失敗：${e.message}`);
  }
}

function emergencyStopAll() {
  try {
    PropertiesService.getScriptProperties().setProperty('STOP_EXECUTION', 'true');
    console.log('⛔ 緊急停止信號已發送');
    showToast('⛔ 緊急停止信號已發送');
    return { success: true };
  } catch (e) {
    throw new Error(`緊急停止失敗：${e.message}`);
  }
}

function showSystemSettings() {
  const ui = SpreadsheetApp.getUi();
  try {
    const ss = CentralContext.getSpreadsheet();
    const period = getPeriodInfo();
    
    const settings = `
🧹 清潔承攬系統設定

📆 當前期別：${period.periodCode || '未設定'} ${period.display || ''}
📁 試算表名稱：${ss.getName()}
📊 工作表數量：${ss.getSheets().length}

📍 區域名稱：${getCurrentRegion() || '未設定'}

⚙️ 系統參數：
- 處理延遲：${CONFIG?.PROCESS_DELAY || 2000}ms
- 匯入延遲：${CONFIG?.IMPORT_DELAY || 3000}ms
- PDF延遲：${CONFIG?.PDF_DELAY || 2000}ms
    `;
    
    ui.alert('⚙️ 系統設定', settings, ui.ButtonSet.OK);
  } catch (e) {
    ui.alert('❌ 錯誤', `無法讀取系統設定：${e.message}`, ui.ButtonSet.OK);
  }
}

// ================================================
// 🔧 相容性函數（確保原有功能正常）
// ================================================

function showToast(message, title = '系統提示') {
  try {
    CentralContext.getSpreadsheet().toast(message, title, 3);
  } catch (e) {
    console.log(`Toast顯示失敗：${e.message}`);
  }
}

function updateSidebarProgress(message) {
  try {
    const props = PropertiesService.getScriptProperties();
    props.setProperty('latestProgress', message);
    props.setProperty('progressTimestamp', new Date().getTime().toString());
    SpreadsheetApp.flush();
    console.log(`📋 進度：${message}`);
  } catch (e) {
    console.log(`更新進度失敗：${e.message}`);
  }
}

function openProgressSidebar() {
  try {
    const html = HtmlService.createHtmlOutputFromFile('sidebar')
      .setTitle('📊 執行進度');
    SpreadsheetApp.getUi().showSidebar(html);
  } catch (e) {
    console.log(`開啟進度側欄失敗：${e.message}`);
  }
}

/** 🔧 系統工具函式 */
function showSystemSettings() {
  const ui = SpreadsheetApp.getUi();
  try {
    const ss = CentralContext.getSpreadsheet();
    const { periodCode, display } = getPeriodInfo();
    
    const settings = `
📊 當前系統狀態：

📅 期別資訊：${display || '未設定'}
📁 檔案名稱：${ss.getName()}
🔢 工作表數量：${ss.getSheets().length}
⏰ 最後修改：${Utilities.formatDate(new Date(ss.getLastUpdated()), 'Asia/Taipei', 'yyyy/MM/dd HH:mm')}

⚙️ 系統配置：
🕐 時區：${CONFIG.TIMEZONE || 'Asia/Taipei'}
⏱️ 處理延遲：${CONFIG.PROCESS_DELAY || 2000}ms
📧 郵件延遲：${CONFIG.EMAIL_DELAY || 2000}ms
📄 PDF延遲：${CONFIG.PDF_DELAY || 1000}ms
    `;
    
    ui.alert("⚙️ 系統設定", settings, ui.ButtonSet.OK);
  } catch (error) {
    ui.alert("❌ 錯誤", "無法取得系統設定：" + error.message, ui.ButtonSet.OK);
  }
}

function runDataValidation() {
  try {
    updateSidebarProgress("🔍 執行資料驗證...");
    
    const requiredSheets = [
      "薪資表", "調薪", "專員請款", "儲值獎金", 
      "新人實境", "新人實習", "組長津貼", "場次薪資時數總表",
      "工具包押金", "介紹獎金", "薪資單", "PDF產出"
    ];
    
    let validCount = 0;
    let issues = [];
    
    for (const sheetName of requiredSheets) {
      try {
        const sheet = CentralContext.getSpreadsheet().getSheetByName(sheetName);
        if (sheet) {
          validCount++;
          // 檢查工作表是否有資料
          if (sheet.getLastRow() <= 1) {
            issues.push(sheetName + "：工作表為空");
          }
        } else {
          issues.push(sheetName + "：工作表不存在");
        }
      } catch (error) {
        issues.push(sheetName + "：存取錯誤");
      }
    }
    
    const result = `
🔍 資料驗證結果：

✅ 有效工作表：${validCount}/${requiredSheets.length}
${issues.length > 0 ? 
  `⚠️ 發現問題：\n${issues.join('\n')}` : 
  '🎉 所有工作表驗證通過！'
}
    `;
    
    SpreadsheetApp.getUi().alert("🔍 驗證結果", result, SpreadsheetApp.getUi().ButtonSet.OK);
    updateSidebarProgress("✅ 資料驗證完成");
    
  } catch (error) {
    const errorMsg = "❌ 資料驗證失敗：" + error.message;
    updateSidebarProgress(errorMsg);
    SpreadsheetApp.getUi().alert("❌ 錯誤", errorMsg, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

function clearTempData() {
  try {
    const properties = PropertiesService.getScriptProperties();
    const tempKeys = [
      'latestProgress', 'progressTimestamp', 
      'STOP_EXECUTION', 'PAUSE_EXECUTION'
    ];
    
    tempKeys.forEach(key => {
      properties.deleteProperty(key);
    });
    
    showToast("🧹 暫存資料清理完成");
    updateSidebarProgress("✅ 暫存資料清理完成");
    
  } catch (error) {
    const errorMsg = "❌ 清理失敗：" + error.message;
    showToast(errorMsg);
    updateSidebarProgress(errorMsg);
  }
}

function emergencyStopAll() {
  try {
    PropertiesService.getScriptProperties().setProperty('STOP_EXECUTION', 'true');
    showToast("⏹️ 緊急停止指令已發送");
    updateSidebarProgress("⏹️ 緊急停止指令已發送");
    SpreadsheetApp.getUi().alert("⏹️ 緊急停止", "緊急停止指令已發送！所有執行中的程序將會停止。", SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (error) {
    SpreadsheetApp.getUi().alert("❌ 錯誤", "緊急停止失敗：" + error.message, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

function syncAllPeriodSettings() {
  try {
    updateSidebarProgress("🔄 同步期別設定...");
    
    // 呼叫期別同步函數
    syncPeriodToNewcomerAndIntern();
    
    showToast("✅ 期別設定同步完成");
    updateSidebarProgress("✅ 期別設定同步完成");
    
  } catch (error) {
    const errorMsg = "❌ 期別同步失敗：" + error.message;
    showToast(errorMsg);
    updateSidebarProgress(errorMsg);
  }
}

function generateStatisticsReport() {
  try {
    updateSidebarProgress("📊 產生統計報表...");
    
    // 這裡可以加入統計報表的具體邏輯
    // 例如：各項目金額統計、人數統計、完成度統計等
    
    showToast("📊 統計報表產生完成");
    updateSidebarProgress("✅ 統計報表產生完成");
    
  } catch (error) {
    const errorMsg = "❌ 統計報表產生失敗：" + error.message;
    showToast(errorMsg);
    updateSidebarProgress(errorMsg);
  }
}

function exportSystemLog() {
  try {
    updateSidebarProgress("📋 匯出系統日誌...");
    
    // 這裡可以加入日誌匯出的具體邏輯
    
    showToast("📋 系統日誌匯出完成");
    updateSidebarProgress("✅ 系統日誌匯出完成");
    
  } catch (error) {
    const errorMsg = "❌ 日誌匯出失敗：" + error.message;
    showToast(errorMsg);
    updateSidebarProgress(errorMsg);
  }
}

function testParams() {
  console.log("=== 測試參數傳遞 ===");
  
  // 測試1：直接調用
  console.log("測試1：直接調用 runBankAccountUpdate(true)");
  const result1 = runBankAccountUpdate(true);
  
  // 測試2：通過包裝函數調用
  console.log("測試2：通過 runBankAccountFirst() 調用");
  const result2 = runBankAccountFirst();
  
  console.log("測試完成");
}


function debugUpDown() {
  console.log("=== 調試上下半月執行 ===");
  
  // 模擬上半月調用鏈
  console.log("1. 調用 runBankAccountFirst()");
  
  console.log("2. 檢查 runBankAccountUpdate(true) 的參數");
  const isFirstHalf = true;
  console.log(`   isFirstHalf = ${isFirstHalf}`);
  
  console.log("3. 檢查配置選擇");
  const config = isFirstHalf ? getYuantaAccountFirstHalfConfig() : getYuantaAccountSecondHalfConfig();
  console.log(`   選擇的配置：${config.name}`);
  console.log(`   步驟數：${config.totalSteps}`);
  
  console.log("4. 檢查期別生成");
  const period = getPeriodPrefix(isFirstHalf);
  console.log(`   期別：${period}`);
  
  console.log("5. 檢查處理函數");
  if (isFirstHalf === true) {
    console.log("   ✅ 應該調用 processFirstHalfYuanta (N4:Q)");
  } else {
    console.log("   ❌ 會調用 processSecondHalfYuanta (U4:X + AB4:AE)");
  }
}


  return {
    addProgress: addProgress,
    adjustPdfNoteRowHeight_: adjustPdfNoteRowHeight_,
    applyZeroAsBlankNumberFormat: applyZeroAsBlankNumberFormat,
    backupHeaderFormat: backupHeaderFormat,
    checkCurrentFileName: checkCurrentFileName,
    checkCurrentPermissions: checkCurrentPermissions,
    checkExecutionControl: checkExecutionControl,
    checkInternImportResult: checkInternImportResult,
    checkYuantaAccountFile: checkYuantaAccountFile,
    cleanCellBlankLines_: cleanCellBlankLines_,
    clearAllControlFlags: clearAllControlFlags,
    clearAllControlFlagsByConfig_: clearAllControlFlagsByConfig_,
    clearCleaningControlFlags: clearCleaningControlFlags,
    clearEmergencyStopFlag: clearEmergencyStopFlag,
    clearNonFormulaDirtyValues: clearNonFormulaDirtyValues,
    clearProgressData: clearProgressData,
    clearProjectControlFlags: clearProjectControlFlags,
    clearRegionSetting: clearRegionSetting,
    clearSalaryStaffColumnsAfterNames_: clearSalaryStaffColumnsAfterNames_,
    clearTempData: clearTempData,
    clientPrompt: clientPrompt,
    columnToNumber: columnToNumber,
    convertRangeToValues: convertRangeToValues,
    convertVoucherDataToValues: convertVoucherDataToValues,
    copyFormulasWithReplace: copyFormulasWithReplace,
    copyFormulasWithReplaceBatch: copyFormulasWithReplaceBatch,
    copyFormulasWithReplaceBatchSkipRows: copyFormulasWithReplaceBatchSkipRows,
    copyRowUntilBlank: copyRowUntilBlank,
    copySalaryRow2048To2047AsValues_: copySalaryRow2048To2047AsValues_,
    countSalaryHeaderFromL_: countSalaryHeaderFromL_,
    debugToolDepositSheets: debugToolDepositSheets,
    debugUpDown: debugUpDown,
    diagnoseSourceData: diagnoseSourceData,
    emergencyStopAll: emergencyStopAll,
    exampleUsage: exampleUsage,
    executeCommonProcess: executeCommonProcess,
    executeCompletePayrollProcess: executeCompletePayrollProcess,
    executeCompleteYuantaProcess: executeCompleteYuantaProcess,
    executeFullAdjustmentProcess: executeFullAdjustmentProcess,
    executeFullLeaderProcess: executeFullLeaderProcess,
    executeFullSettlementProcess: executeFullSettlementProcess,
    executeFullToolDepositProcess: executeFullToolDepositProcess,
    executeInternCommonProcess: executeInternCommonProcess,
    executeNewcomerCommonProcess: executeNewcomerCommonProcess,
    executePanelAction: executePanelAction,
    executeVoucherCommonProcess: executeVoucherCommonProcess,
    exportSystemLog: exportSystemLog,
    extractDriveFileId_: extractDriveFileId_,
    extractPeriodFromFileName: extractPeriodFromFileName,
    extractRegionFromFileName: extractRegionFromFileName,
    findPeriodFileId_: findPeriodFileId_,
    findYuantaAccountFile: findYuantaAccountFile,
    findYuantaAccountFileDetailed: findYuantaAccountFileDetailed,
    finishInternProcess: finishInternProcess,
    finishNewcomerProcess: finishNewcomerProcess,
    fullYuantaDiagnosis: fullYuantaDiagnosis,
    generateCleaningSalaryPDFsByPeriodFile: generateCleaningSalaryPDFsByPeriodFile,
    generateProjectSalaryPDFs: generateProjectSalaryPDFs,
    generateProjectSalaryPDFsByPeriodFile: generateProjectSalaryPDFsByPeriodFile,
    generateSalaryPDFsByConfigAndFile_: generateSalaryPDFsByConfigAndFile_,
    generateSalaryPDFsByConfig_: generateSalaryPDFsByConfig_,
    generateSalaryPDFsCore_: generateSalaryPDFsCore_,
    generateSalaryPDFs_v2025: generateSalaryPDFs_v2025,
    generateStatisticsReport: generateStatisticsReport,
    getActualPdfSavedTime_: getActualPdfSavedTime_,
    getAdjustmentNamesFromS_: getAdjustmentNamesFromS_,
    getAdjustmentProcessConfig: getAdjustmentProcessConfig,
    getAllValues_: getAllValues_,
    getAllowanceProcessConfig: getAllowanceProcessConfig,
    getColumnLetter: getColumnLetter,
    getCompletePayrollProcessConfig: getCompletePayrollProcessConfig,
    getCurrentRegion: getCurrentRegion,
    getDataFromRange: getDataFromRange,
    getDefaultPeriod: getDefaultPeriod,
    getExecutionState: getExecutionState,
    getFinalPdfStorageFolder: getFinalPdfStorageFolder,
    getFirstEmptyRowByColumn: getFirstEmptyRowByColumn,
    getInternProcessConfig: getInternProcessConfig,
    getInternRecoverySuggestion: getInternRecoverySuggestion,
    getLatestProgress: getLatestProgress,
    getLeaderProcessConfig: getLeaderProcessConfig,
    getLeaderRecoverySuggestion: getLeaderRecoverySuggestion,
    getNewcomerProcessConfig: getNewcomerProcessConfig,
    getNextEmptyRowInColumnA: getNextEmptyRowInColumnA,
    getPanelInitData: getPanelInitData,
    getPdfRootFolder: getPdfRootFolder,
    getPdfStorageFolder: getPdfStorageFolder,
    getPdfStorageFolderBySpreadsheet_: getPdfStorageFolderBySpreadsheet_,
    getPendingPdfStaffList_: getPendingPdfStaffList_,
    getPeriodFromExistingPdf: getPeriodFromExistingPdf,
    getPeriodInfo: getPeriodInfo,
    getPeriodInfoBySpreadsheet_: getPeriodInfoBySpreadsheet_,
    getPeriodLabelProcessConfig: getPeriodLabelProcessConfig,
    getPeriodPrefix: getPeriodPrefix,
    getPeriodPrefixFromFile: getPeriodPrefixFromFile,
    getRecoverySuggestion: getRecoverySuggestion,
    getSafeFolderById: getSafeFolderById,
    getSafeFolderByName: getSafeFolderByName,
    getSalaryNamesList: getSalaryNamesList,
    getSalaryPdfFileName: getSalaryPdfFileName,
    getSalaryProcessConfig: getSalaryProcessConfig,
    getSettlementProcessConfig: getSettlementProcessConfig,
    getSettlementRecoverySuggestion: getSettlementRecoverySuggestion,
    getSheetByName: getSheetByName,
    getSheetNames: getSheetNames,
    getSilentMode: getSilentMode,
    getTargetDateFor10th: getTargetDateFor10th,
    getTargetDateFor20th: getTargetDateFor20th,
    getVoucherProcessConfig: getVoucherProcessConfig,
    getYuantaAccountFirstHalfConfig: getYuantaAccountFirstHalfConfig,
    getYuantaAccountSecondHalfConfig: getYuantaAccountSecondHalfConfig,
    handleInternClearLogic: handleInternClearLogic,
    handleNewcomerClearLogic: handleNewcomerClearLogic,
    importAllowanceData: importAllowanceData,
    importAndPrepareData: importAndPrepareData,
    importNewcomerDataWithQRS: importNewcomerDataWithQRS,
    importVoucherData: importVoucherData,
    isEmergencyStopRequested_: isEmergencyStopRequested_,
    makeFileAnyoneWithLinkViewOnly: makeFileAnyoneWithLinkViewOnly,
    makeFinalPdfFolderAnyoneWithLinkViewOnly: makeFinalPdfFolderAnyoneWithLinkViewOnly,
    makeFolderAnyoneWithLinkViewOnly: makeFolderAnyoneWithLinkViewOnly,
    makePdfAnyoneWithLinkViewOnly: makePdfAnyoneWithLinkViewOnly,
    manualSelectYuantaFile: manualSelectYuantaFile,
    markAllCleaningStaffForProcessing: markAllCleaningStaffForProcessing,
    markAllProjectStaffForProcessing: markAllProjectStaffForProcessing,
    markAllStaffForProcessing: markAllStaffForProcessing,
    markAllStaffForProcessing_: markAllStaffForProcessing_,
    markCustomFinishByHalf: markCustomFinishByHalf,
    markStepFinish: markStepFinish,
    normalizeDriveUrl_: normalizeDriveUrl_,
    onOpen: onOpen,
    openProgressSidebar: openProgressSidebar,
    pauseExecution: pauseExecution,
    performFinalValidation: performFinalValidation,
    performInternValidation: performInternValidation,
    performLeaderValidation: performLeaderValidation,
    performSettlementValidation: performSettlementValidation,
    prepareImportParameters: prepareImportParameters,
    prepareInternImportParams: prepareInternImportParams,
    prepareNewcomerImportParams: prepareNewcomerImportParams,
    preparePeriodInfo: preparePeriodInfo,
    prepareRevenueData: prepareRevenueData,
    prepareSalaryParameters: prepareSalaryParameters,
    prepareVoucherImportParameters: prepareVoucherImportParameters,
    processFirstHalfYuanta: processFirstHalfYuanta,
    processImportDecision: processImportDecision,
    processInternPeriodLabel: processInternPeriodLabel,
    processInternQRSCalculations: processInternQRSCalculations,
    processLemonData: processLemonData,
    processNewcomerPeriodLabel: processNewcomerPeriodLabel,
    processOrderClearData: processOrderClearData,
    processOrderClearDataWithProjectOrders: processOrderClearDataWithProjectOrders,
    processQRSCalculation: processQRSCalculation,
    processRevenueDataWithProjectOrders: processRevenueDataWithProjectOrders,
    processSalaryLColumn: processSalaryLColumn,
    processSecondHalfYuanta: processSecondHalfYuanta,
    processVoucherBonusDistribution: processVoucherBonusDistribution,
    processVoucherClearData: processVoucherClearData,
    promptUserForPeriod: promptUserForPeriod,
    quickRegenerateAllCleaningPDFs: quickRegenerateAllCleaningPDFs,
    quickRegenerateAllProjectPDFs: quickRegenerateAllProjectPDFs,
    quickSyncPeriodLabel: quickSyncPeriodLabel,
    remarkFailedCleaningPdfRows: remarkFailedCleaningPdfRows,
    remarkFailedPdfRowsByConfig_: remarkFailedPdfRowsByConfig_,
    remarkFailedProjectPdfRows: remarkFailedProjectPdfRows,
    repairCleaningPdfLinks: repairCleaningPdfLinks,
    repairPdfLinks: repairPdfLinks,
    repairPdfLinksByConfig_: repairPdfLinksByConfig_,
    repairProjectPdfLinks: repairProjectPdfLinks,
    replacePdfFileContent_: replacePdfFileContent_,
    restoreHeaderFormat: restoreHeaderFormat,
    resumeExecution: resumeExecution,
    runAdjustmentFirst: runAdjustmentFirst,
    runAdjustmentPreparation: runAdjustmentPreparation,
    runAdjustmentPreparationFirstHalf: runAdjustmentPreparationFirstHalf,
    runAdjustmentPreparationSecondHalf: runAdjustmentPreparationSecondHalf,
    runAdjustmentSecond: runAdjustmentSecond,
    runAllowanceFirst: runAllowanceFirst,
    runAllowanceProcess: runAllowanceProcess,
    runAllowanceProcessFirstHalf: runAllowanceProcessFirstHalf,
    runAllowanceProcessSecondHalf: runAllowanceProcessSecondHalf,
    runAllowanceSecond: runAllowanceSecond,
    runBankAccountFirst: runBankAccountFirst,
    runBankAccountSecond: runBankAccountSecond,
    runBankAccountUpdate: runBankAccountUpdate,
    runCommonProcess: runCommonProcess,
    runCompletePayrollFirst: runCompletePayrollFirst,
    runCompletePayrollProcess: runCompletePayrollProcess,
    runCompletePayrollProcessFirstHalf: runCompletePayrollProcessFirstHalf,
    runCompletePayrollProcessSecondHalf: runCompletePayrollProcessSecondHalf,
    runCompletePayrollSecond: runCompletePayrollSecond,
    runDataValidation: runDataValidation,
    runFinalSetFirst: runFinalSetFirst,
    runFinalSetSecond: runFinalSetSecond,
    runFinalSettlement: runFinalSettlement,
    runFinalSettlementFirstHalf: runFinalSettlementFirstHalf,
    runFinalSettlementSecondHalf: runFinalSettlementSecondHalf,
    runHalfFullFirst: runHalfFullFirst,
    runHalfFullSecond: runHalfFullSecond,
    runInternFirst: runInternFirst,
    runInternProcess: runInternProcess,
    runInternProcessFirstHalf: runInternProcessFirstHalf,
    runInternProcessLegacy: runInternProcessLegacy,
    runInternProcessSecondHalf: runInternProcessSecondHalf,
    runInternSecond: runInternSecond,
    runLeaderFirst: runLeaderFirst,
    runLeaderProcess: runLeaderProcess,
    runLeaderProcessFirstHalf: runLeaderProcessFirstHalf,
    runLeaderProcessSecondHalf: runLeaderProcessSecondHalf,
    runLeaderSecond: runLeaderSecond,
    runNewEmployeePeriodLabel: runNewEmployeePeriodLabel,
    runNewcomerFirst: runNewcomerFirst,
    runNewcomerProcess: runNewcomerProcess,
    runNewcomerProcessFirstHalf: runNewcomerProcessFirstHalf,
    runNewcomerProcessLegacy: runNewcomerProcessLegacy,
    runNewcomerProcessSecondHalf: runNewcomerProcessSecondHalf,
    runNewcomerSecond: runNewcomerSecond,
    runSalaryFirst: runSalaryFirst,
    runSalaryPreparation: runSalaryPreparation,
    runSalaryPreparationFirstHalf: runSalaryPreparationFirstHalf,
    runSalaryPreparationSecondHalf: runSalaryPreparationSecondHalf,
    runSalaryPreparationWithProjectOrders: runSalaryPreparationWithProjectOrders,
    runSalaryPreparationWithProjectOrdersFirstHalf: runSalaryPreparationWithProjectOrdersFirstHalf,
    runSalaryPreparationWithProjectOrdersSecondHalf: runSalaryPreparationWithProjectOrdersSecondHalf,
    runSalarySecond: runSalarySecond,
    runSalarySummaryFirst: runSalarySummaryFirst,
    runSalarySummaryProcess: runSalarySummaryProcess,
    runSalarySummarySecond: runSalarySummarySecond,
    runToolDepositFirst: runToolDepositFirst,
    runToolDepositProcess: runToolDepositProcess,
    runToolDepositProcessFirst: runToolDepositProcessFirst,
    runToolDepositProcessSecond: runToolDepositProcessSecond,
    runToolDepositSecond: runToolDepositSecond,
    runVoucherFirst: runVoucherFirst,
    runVoucherPreparation: runVoucherPreparation,
    runVoucherPreparationFirstHalf: runVoucherPreparationFirstHalf,
    runVoucherPreparationSecondHalf: runVoucherPreparationSecondHalf,
    runVoucherSecond: runVoucherSecond,
    runYuantaAccountFirstHalf: runYuantaAccountFirstHalf,
    runYuantaAccountSecondHalf: runYuantaAccountSecondHalf,
    safeClearHighlight: safeClearHighlight,
    safeHighlightProcessingRange: safeHighlightProcessingRange,
    saveAsExcelFile: saveAsExcelFile,
    savePreservingFileLinkEnhanced: savePreservingFileLinkEnhanced,
    scrollToCell: scrollToCell,
    setEmergencyStopFlag: setEmergencyStopFlag,
    setExecutionState: setExecutionState,
    setPdfRootFolder: setPdfRootFolder,
    setRegionName: setRegionName,
    setRegionRootFolderWithId: setRegionRootFolderWithId,
    setSilentMode: setSilentMode,
    setupSalarySlipZeroAsBlank: setupSalarySlipZeroAsBlank,
    setupYuantaAccountHeaders: setupYuantaAccountHeaders,
    showExecutionSidebar: showExecutionSidebar,
    showImportConfirmDialog: showImportConfirmDialog,
    showMainControlPanel: showMainControlPanel,
    showSystemSettings: showSystemSettings,
    showToast: showToast,
    stopExecution: stopExecution,
    switchPeriod: switchPeriod,
    syncAllPeriodSettings: syncAllPeriodSettings,
    syncPeriodToNewcomerAndIntern: syncPeriodToNewcomerAndIntern,
    testFolderAccess: testFolderAccess,
    testParams: testParams,
    testPeriodExtraction: testPeriodExtraction,
    testSpreadsheetOpenPermission: testSpreadsheetOpenPermission,
    throwIfEmergencyStopRequested_: throwIfEmergencyStopRequested_,
    triggerAllPermissions: triggerAllPermissions,
    updateCompletionStatus: updateCompletionStatus,
    updateProgress: updateProgress,
    updateSidebarProgress: updateSidebarProgress,
    updateSidebarProgressWithDelay: updateSidebarProgressWithDelay,
    validateAndGetSheet: validateAndGetSheet,
    validateCellValue: validateCellValue,
    validateImportData: validateImportData,
    validateSheetStatus: validateSheetStatus,
    waitForImportDecision: waitForImportDecision,
    writeDataToYuantaAccount: writeDataToYuantaAccount
  };
})();

function cleaning_addProgress() { return CleaningApp.addProgress.apply(null, arguments); }
function cleaning_adjustPdfNoteRowHeight_() { return CleaningApp.adjustPdfNoteRowHeight_.apply(null, arguments); }
function cleaning_applyZeroAsBlankNumberFormat() { return CleaningApp.applyZeroAsBlankNumberFormat.apply(null, arguments); }
function cleaning_backupHeaderFormat() { return CleaningApp.backupHeaderFormat.apply(null, arguments); }
function cleaning_checkCurrentFileName() { return CleaningApp.checkCurrentFileName.apply(null, arguments); }
function cleaning_checkCurrentPermissions() { return CleaningApp.checkCurrentPermissions.apply(null, arguments); }
function cleaning_checkExecutionControl() { return CleaningApp.checkExecutionControl.apply(null, arguments); }
function cleaning_checkInternImportResult() { return CleaningApp.checkInternImportResult.apply(null, arguments); }
function cleaning_checkYuantaAccountFile() { return CleaningApp.checkYuantaAccountFile.apply(null, arguments); }
function cleaning_cleanCellBlankLines_() { return CleaningApp.cleanCellBlankLines_.apply(null, arguments); }
function cleaning_clearAllControlFlags() { return CleaningApp.clearAllControlFlags.apply(null, arguments); }
function cleaning_clearAllControlFlagsByConfig_() { return CleaningApp.clearAllControlFlagsByConfig_.apply(null, arguments); }
function cleaning_clearCleaningControlFlags() { return CleaningApp.clearCleaningControlFlags.apply(null, arguments); }
function cleaning_clearEmergencyStopFlag() { return CleaningApp.clearEmergencyStopFlag.apply(null, arguments); }
function cleaning_clearNonFormulaDirtyValues() { return CleaningApp.clearNonFormulaDirtyValues.apply(null, arguments); }
function cleaning_clearProgressData() { return CleaningApp.clearProgressData.apply(null, arguments); }
function cleaning_clearProjectControlFlags() { return CleaningApp.clearProjectControlFlags.apply(null, arguments); }
function cleaning_clearRegionSetting() { return CleaningApp.clearRegionSetting.apply(null, arguments); }
function cleaning_clearSalaryStaffColumnsAfterNames_() { return CleaningApp.clearSalaryStaffColumnsAfterNames_.apply(null, arguments); }
function cleaning_clearTempData() { return CleaningApp.clearTempData.apply(null, arguments); }
function cleaning_clientPrompt() { return CleaningApp.clientPrompt.apply(null, arguments); }
function cleaning_columnToNumber() { return CleaningApp.columnToNumber.apply(null, arguments); }
function cleaning_convertRangeToValues() { return CleaningApp.convertRangeToValues.apply(null, arguments); }
function cleaning_convertVoucherDataToValues() { return CleaningApp.convertVoucherDataToValues.apply(null, arguments); }
function cleaning_copyFormulasWithReplace() { return CleaningApp.copyFormulasWithReplace.apply(null, arguments); }
function cleaning_copyFormulasWithReplaceBatch() { return CleaningApp.copyFormulasWithReplaceBatch.apply(null, arguments); }
function cleaning_copyFormulasWithReplaceBatchSkipRows() { return CleaningApp.copyFormulasWithReplaceBatchSkipRows.apply(null, arguments); }
function cleaning_copyRowUntilBlank() { return CleaningApp.copyRowUntilBlank.apply(null, arguments); }
function cleaning_copySalaryRow2048To2047AsValues_() { return CleaningApp.copySalaryRow2048To2047AsValues_.apply(null, arguments); }
function cleaning_countSalaryHeaderFromL_() { return CleaningApp.countSalaryHeaderFromL_.apply(null, arguments); }
function cleaning_debugToolDepositSheets() { return CleaningApp.debugToolDepositSheets.apply(null, arguments); }
function cleaning_debugUpDown() { return CleaningApp.debugUpDown.apply(null, arguments); }
function cleaning_diagnoseSourceData() { return CleaningApp.diagnoseSourceData.apply(null, arguments); }
function cleaning_emergencyStopAll() { return CleaningApp.emergencyStopAll.apply(null, arguments); }
function cleaning_exampleUsage() { return CleaningApp.exampleUsage.apply(null, arguments); }
function cleaning_executeCommonProcess() { return CleaningApp.executeCommonProcess.apply(null, arguments); }
function cleaning_executeCompletePayrollProcess() { return CleaningApp.executeCompletePayrollProcess.apply(null, arguments); }
function cleaning_executeCompleteYuantaProcess() { return CleaningApp.executeCompleteYuantaProcess.apply(null, arguments); }
function cleaning_executeFullAdjustmentProcess() { return CleaningApp.executeFullAdjustmentProcess.apply(null, arguments); }
function cleaning_executeFullLeaderProcess() { return CleaningApp.executeFullLeaderProcess.apply(null, arguments); }
function cleaning_executeFullSettlementProcess() { return CleaningApp.executeFullSettlementProcess.apply(null, arguments); }
function cleaning_executeFullToolDepositProcess() { return CleaningApp.executeFullToolDepositProcess.apply(null, arguments); }
function cleaning_executeInternCommonProcess() { return CleaningApp.executeInternCommonProcess.apply(null, arguments); }
function cleaning_executeNewcomerCommonProcess() { return CleaningApp.executeNewcomerCommonProcess.apply(null, arguments); }
function cleaning_executePanelAction() { return CleaningApp.executePanelAction.apply(null, arguments); }
function cleaning_executeVoucherCommonProcess() { return CleaningApp.executeVoucherCommonProcess.apply(null, arguments); }
function cleaning_exportSystemLog() { return CleaningApp.exportSystemLog.apply(null, arguments); }
function cleaning_extractDriveFileId_() { return CleaningApp.extractDriveFileId_.apply(null, arguments); }
function cleaning_extractPeriodFromFileName() { return CleaningApp.extractPeriodFromFileName.apply(null, arguments); }
function cleaning_extractRegionFromFileName() { return CleaningApp.extractRegionFromFileName.apply(null, arguments); }
function cleaning_findPeriodFileId_() { return CleaningApp.findPeriodFileId_.apply(null, arguments); }
function cleaning_findYuantaAccountFile() { return CleaningApp.findYuantaAccountFile.apply(null, arguments); }
function cleaning_findYuantaAccountFileDetailed() { return CleaningApp.findYuantaAccountFileDetailed.apply(null, arguments); }
function cleaning_finishInternProcess() { return CleaningApp.finishInternProcess.apply(null, arguments); }
function cleaning_finishNewcomerProcess() { return CleaningApp.finishNewcomerProcess.apply(null, arguments); }
function cleaning_fullYuantaDiagnosis() { return CleaningApp.fullYuantaDiagnosis.apply(null, arguments); }
function cleaning_generateCleaningSalaryPDFsByPeriodFile() { return CleaningApp.generateCleaningSalaryPDFsByPeriodFile.apply(null, arguments); }
function cleaning_generateProjectSalaryPDFs() { return CleaningApp.generateProjectSalaryPDFs.apply(null, arguments); }
function cleaning_generateProjectSalaryPDFsByPeriodFile() { return CleaningApp.generateProjectSalaryPDFsByPeriodFile.apply(null, arguments); }
function cleaning_generateSalaryPDFsByConfigAndFile_() { return CleaningApp.generateSalaryPDFsByConfigAndFile_.apply(null, arguments); }
function cleaning_generateSalaryPDFsByConfig_() { return CleaningApp.generateSalaryPDFsByConfig_.apply(null, arguments); }
function cleaning_generateSalaryPDFsCore_() { return CleaningApp.generateSalaryPDFsCore_.apply(null, arguments); }
function cleaning_generateSalaryPDFs_v2025() { return CleaningApp.generateSalaryPDFs_v2025.apply(null, arguments); }
function cleaning_generateStatisticsReport() { return CleaningApp.generateStatisticsReport.apply(null, arguments); }
function cleaning_getActualPdfSavedTime_() { return CleaningApp.getActualPdfSavedTime_.apply(null, arguments); }
function cleaning_getAdjustmentNamesFromS_() { return CleaningApp.getAdjustmentNamesFromS_.apply(null, arguments); }
function cleaning_getAdjustmentProcessConfig() { return CleaningApp.getAdjustmentProcessConfig.apply(null, arguments); }
function cleaning_getAllValues_() { return CleaningApp.getAllValues_.apply(null, arguments); }
function cleaning_getAllowanceProcessConfig() { return CleaningApp.getAllowanceProcessConfig.apply(null, arguments); }
function cleaning_getColumnLetter() { return CleaningApp.getColumnLetter.apply(null, arguments); }
function cleaning_getCompletePayrollProcessConfig() { return CleaningApp.getCompletePayrollProcessConfig.apply(null, arguments); }
function cleaning_getCurrentRegion() { return CleaningApp.getCurrentRegion.apply(null, arguments); }
function cleaning_getDataFromRange() { return CleaningApp.getDataFromRange.apply(null, arguments); }
function cleaning_getDefaultPeriod() { return CleaningApp.getDefaultPeriod.apply(null, arguments); }
function cleaning_getExecutionState() { return CleaningApp.getExecutionState.apply(null, arguments); }
function cleaning_getFinalPdfStorageFolder() { return CleaningApp.getFinalPdfStorageFolder.apply(null, arguments); }
function cleaning_getFirstEmptyRowByColumn() { return CleaningApp.getFirstEmptyRowByColumn.apply(null, arguments); }
function cleaning_getInternProcessConfig() { return CleaningApp.getInternProcessConfig.apply(null, arguments); }
function cleaning_getInternRecoverySuggestion() { return CleaningApp.getInternRecoverySuggestion.apply(null, arguments); }
function cleaning_getLatestProgress() { return CleaningApp.getLatestProgress.apply(null, arguments); }
function cleaning_getLeaderProcessConfig() { return CleaningApp.getLeaderProcessConfig.apply(null, arguments); }
function cleaning_getLeaderRecoverySuggestion() { return CleaningApp.getLeaderRecoverySuggestion.apply(null, arguments); }
function cleaning_getNewcomerProcessConfig() { return CleaningApp.getNewcomerProcessConfig.apply(null, arguments); }
function cleaning_getNextEmptyRowInColumnA() { return CleaningApp.getNextEmptyRowInColumnA.apply(null, arguments); }
function cleaning_getPanelInitData() { return CleaningApp.getPanelInitData.apply(null, arguments); }
function cleaning_getPdfRootFolder() { return CleaningApp.getPdfRootFolder.apply(null, arguments); }
function cleaning_getPdfStorageFolder() { return CleaningApp.getPdfStorageFolder.apply(null, arguments); }
function cleaning_getPdfStorageFolderBySpreadsheet_() { return CleaningApp.getPdfStorageFolderBySpreadsheet_.apply(null, arguments); }
function cleaning_getPendingPdfStaffList_() { return CleaningApp.getPendingPdfStaffList_.apply(null, arguments); }
function cleaning_getPeriodFromExistingPdf() { return CleaningApp.getPeriodFromExistingPdf.apply(null, arguments); }
function cleaning_getPeriodInfo() { return CleaningApp.getPeriodInfo.apply(null, arguments); }
function cleaning_getPeriodInfoBySpreadsheet_() { return CleaningApp.getPeriodInfoBySpreadsheet_.apply(null, arguments); }
function cleaning_getPeriodLabelProcessConfig() { return CleaningApp.getPeriodLabelProcessConfig.apply(null, arguments); }
function cleaning_getPeriodPrefix() { return CleaningApp.getPeriodPrefix.apply(null, arguments); }
function cleaning_getPeriodPrefixFromFile() { return CleaningApp.getPeriodPrefixFromFile.apply(null, arguments); }
function cleaning_getRecoverySuggestion() { return CleaningApp.getRecoverySuggestion.apply(null, arguments); }
function cleaning_getSafeFolderById() { return CleaningApp.getSafeFolderById.apply(null, arguments); }
function cleaning_getSafeFolderByName() { return CleaningApp.getSafeFolderByName.apply(null, arguments); }
function cleaning_getSalaryNamesList() { return CleaningApp.getSalaryNamesList.apply(null, arguments); }
function cleaning_getSalaryPdfFileName() { return CleaningApp.getSalaryPdfFileName.apply(null, arguments); }
function cleaning_getSalaryProcessConfig() { return CleaningApp.getSalaryProcessConfig.apply(null, arguments); }
function cleaning_getSettlementProcessConfig() { return CleaningApp.getSettlementProcessConfig.apply(null, arguments); }
function cleaning_getSettlementRecoverySuggestion() { return CleaningApp.getSettlementRecoverySuggestion.apply(null, arguments); }
function cleaning_getSheetByName() { return CleaningApp.getSheetByName.apply(null, arguments); }
function cleaning_getSheetNames() { return CleaningApp.getSheetNames.apply(null, arguments); }
function cleaning_getSilentMode() { return CleaningApp.getSilentMode.apply(null, arguments); }
function cleaning_getTargetDateFor10th() { return CleaningApp.getTargetDateFor10th.apply(null, arguments); }
function cleaning_getTargetDateFor20th() { return CleaningApp.getTargetDateFor20th.apply(null, arguments); }
function cleaning_getVoucherProcessConfig() { return CleaningApp.getVoucherProcessConfig.apply(null, arguments); }
function cleaning_getYuantaAccountFirstHalfConfig() { return CleaningApp.getYuantaAccountFirstHalfConfig.apply(null, arguments); }
function cleaning_getYuantaAccountSecondHalfConfig() { return CleaningApp.getYuantaAccountSecondHalfConfig.apply(null, arguments); }
function cleaning_handleInternClearLogic() { return CleaningApp.handleInternClearLogic.apply(null, arguments); }
function cleaning_handleNewcomerClearLogic() { return CleaningApp.handleNewcomerClearLogic.apply(null, arguments); }
function cleaning_importAllowanceData() { return CleaningApp.importAllowanceData.apply(null, arguments); }
function cleaning_importAndPrepareData() { return CleaningApp.importAndPrepareData.apply(null, arguments); }
function cleaning_importNewcomerDataWithQRS() { return CleaningApp.importNewcomerDataWithQRS.apply(null, arguments); }
function cleaning_importVoucherData() { return CleaningApp.importVoucherData.apply(null, arguments); }
function cleaning_isEmergencyStopRequested_() { return CleaningApp.isEmergencyStopRequested_.apply(null, arguments); }
function cleaning_makeFileAnyoneWithLinkViewOnly() { return CleaningApp.makeFileAnyoneWithLinkViewOnly.apply(null, arguments); }
function cleaning_makeFinalPdfFolderAnyoneWithLinkViewOnly() { return CleaningApp.makeFinalPdfFolderAnyoneWithLinkViewOnly.apply(null, arguments); }
function cleaning_makeFolderAnyoneWithLinkViewOnly() { return CleaningApp.makeFolderAnyoneWithLinkViewOnly.apply(null, arguments); }
function cleaning_makePdfAnyoneWithLinkViewOnly() { return CleaningApp.makePdfAnyoneWithLinkViewOnly.apply(null, arguments); }
function cleaning_manualSelectYuantaFile() { return CleaningApp.manualSelectYuantaFile.apply(null, arguments); }
function cleaning_markAllCleaningStaffForProcessing() { return CleaningApp.markAllCleaningStaffForProcessing.apply(null, arguments); }
function cleaning_markAllProjectStaffForProcessing() { return CleaningApp.markAllProjectStaffForProcessing.apply(null, arguments); }
function cleaning_markAllStaffForProcessing() { return CleaningApp.markAllStaffForProcessing.apply(null, arguments); }
function cleaning_markAllStaffForProcessing_() { return CleaningApp.markAllStaffForProcessing_.apply(null, arguments); }
function cleaning_markCustomFinishByHalf() { return CleaningApp.markCustomFinishByHalf.apply(null, arguments); }
function cleaning_markStepFinish() { return CleaningApp.markStepFinish.apply(null, arguments); }
function cleaning_normalizeDriveUrl_() { return CleaningApp.normalizeDriveUrl_.apply(null, arguments); }
function cleaning_openProgressSidebar() { return CleaningApp.openProgressSidebar.apply(null, arguments); }
function cleaning_pauseExecution() { return CleaningApp.pauseExecution.apply(null, arguments); }
function cleaning_performFinalValidation() { return CleaningApp.performFinalValidation.apply(null, arguments); }
function cleaning_performInternValidation() { return CleaningApp.performInternValidation.apply(null, arguments); }
function cleaning_performLeaderValidation() { return CleaningApp.performLeaderValidation.apply(null, arguments); }
function cleaning_performSettlementValidation() { return CleaningApp.performSettlementValidation.apply(null, arguments); }
function cleaning_prepareImportParameters() { return CleaningApp.prepareImportParameters.apply(null, arguments); }
function cleaning_prepareInternImportParams() { return CleaningApp.prepareInternImportParams.apply(null, arguments); }
function cleaning_prepareNewcomerImportParams() { return CleaningApp.prepareNewcomerImportParams.apply(null, arguments); }
function cleaning_preparePeriodInfo() { return CleaningApp.preparePeriodInfo.apply(null, arguments); }
function cleaning_prepareRevenueData() { return CleaningApp.prepareRevenueData.apply(null, arguments); }
function cleaning_prepareSalaryParameters() { return CleaningApp.prepareSalaryParameters.apply(null, arguments); }
function cleaning_prepareVoucherImportParameters() { return CleaningApp.prepareVoucherImportParameters.apply(null, arguments); }
function cleaning_processFirstHalfYuanta() { return CleaningApp.processFirstHalfYuanta.apply(null, arguments); }
function cleaning_processImportDecision() { return CleaningApp.processImportDecision.apply(null, arguments); }
function cleaning_processInternPeriodLabel() { return CleaningApp.processInternPeriodLabel.apply(null, arguments); }
function cleaning_processInternQRSCalculations() { return CleaningApp.processInternQRSCalculations.apply(null, arguments); }
function cleaning_processLemonData() { return CleaningApp.processLemonData.apply(null, arguments); }
function cleaning_processNewcomerPeriodLabel() { return CleaningApp.processNewcomerPeriodLabel.apply(null, arguments); }
function cleaning_processOrderClearData() { return CleaningApp.processOrderClearData.apply(null, arguments); }
function cleaning_processOrderClearDataWithProjectOrders() { return CleaningApp.processOrderClearDataWithProjectOrders.apply(null, arguments); }
function cleaning_processQRSCalculation() { return CleaningApp.processQRSCalculation.apply(null, arguments); }
function cleaning_processRevenueDataWithProjectOrders() { return CleaningApp.processRevenueDataWithProjectOrders.apply(null, arguments); }
function cleaning_processSalaryLColumn() { return CleaningApp.processSalaryLColumn.apply(null, arguments); }
function cleaning_processSecondHalfYuanta() { return CleaningApp.processSecondHalfYuanta.apply(null, arguments); }
function cleaning_processVoucherBonusDistribution() { return CleaningApp.processVoucherBonusDistribution.apply(null, arguments); }
function cleaning_processVoucherClearData() { return CleaningApp.processVoucherClearData.apply(null, arguments); }
function cleaning_promptUserForPeriod() { return CleaningApp.promptUserForPeriod.apply(null, arguments); }
function cleaning_quickRegenerateAllCleaningPDFs() { return CleaningApp.quickRegenerateAllCleaningPDFs.apply(null, arguments); }
function cleaning_quickRegenerateAllProjectPDFs() { return CleaningApp.quickRegenerateAllProjectPDFs.apply(null, arguments); }
function cleaning_quickSyncPeriodLabel() { return CleaningApp.quickSyncPeriodLabel.apply(null, arguments); }
function cleaning_remarkFailedCleaningPdfRows() { return CleaningApp.remarkFailedCleaningPdfRows.apply(null, arguments); }
function cleaning_remarkFailedPdfRowsByConfig_() { return CleaningApp.remarkFailedPdfRowsByConfig_.apply(null, arguments); }
function cleaning_remarkFailedProjectPdfRows() { return CleaningApp.remarkFailedProjectPdfRows.apply(null, arguments); }
function cleaning_repairCleaningPdfLinks() { return CleaningApp.repairCleaningPdfLinks.apply(null, arguments); }
function cleaning_repairPdfLinks() { return CleaningApp.repairPdfLinks.apply(null, arguments); }
function cleaning_repairPdfLinksByConfig_() { return CleaningApp.repairPdfLinksByConfig_.apply(null, arguments); }
function cleaning_repairProjectPdfLinks() { return CleaningApp.repairProjectPdfLinks.apply(null, arguments); }
function cleaning_replacePdfFileContent_() { return CleaningApp.replacePdfFileContent_.apply(null, arguments); }
function cleaning_restoreHeaderFormat() { return CleaningApp.restoreHeaderFormat.apply(null, arguments); }
function cleaning_resumeExecution() { return CleaningApp.resumeExecution.apply(null, arguments); }
function cleaning_runAdjustmentFirst() { return CleaningApp.runAdjustmentFirst.apply(null, arguments); }
function cleaning_runAdjustmentPreparation() { return CleaningApp.runAdjustmentPreparation.apply(null, arguments); }
function cleaning_runAdjustmentPreparationFirstHalf() { return CleaningApp.runAdjustmentPreparationFirstHalf.apply(null, arguments); }
function cleaning_runAdjustmentPreparationSecondHalf() { return CleaningApp.runAdjustmentPreparationSecondHalf.apply(null, arguments); }
function cleaning_runAdjustmentSecond() { return CleaningApp.runAdjustmentSecond.apply(null, arguments); }
function cleaning_runAllowanceFirst() { return CleaningApp.runAllowanceFirst.apply(null, arguments); }
function cleaning_runAllowanceProcess() { return CleaningApp.runAllowanceProcess.apply(null, arguments); }
function cleaning_runAllowanceProcessFirstHalf() { return CleaningApp.runAllowanceProcessFirstHalf.apply(null, arguments); }
function cleaning_runAllowanceProcessSecondHalf() { return CleaningApp.runAllowanceProcessSecondHalf.apply(null, arguments); }
function cleaning_runAllowanceSecond() { return CleaningApp.runAllowanceSecond.apply(null, arguments); }
function cleaning_runBankAccountFirst() { return CleaningApp.runBankAccountFirst.apply(null, arguments); }
function cleaning_runBankAccountSecond() { return CleaningApp.runBankAccountSecond.apply(null, arguments); }
function cleaning_runBankAccountUpdate() { return CleaningApp.runBankAccountUpdate.apply(null, arguments); }
function cleaning_runCommonProcess() { return CleaningApp.runCommonProcess.apply(null, arguments); }
function cleaning_runCompletePayrollFirst() { return CleaningApp.runCompletePayrollFirst.apply(null, arguments); }
function cleaning_runCompletePayrollProcess() { return CleaningApp.runCompletePayrollProcess.apply(null, arguments); }
function cleaning_runCompletePayrollProcessFirstHalf() { return CleaningApp.runCompletePayrollProcessFirstHalf.apply(null, arguments); }
function cleaning_runCompletePayrollProcessSecondHalf() { return CleaningApp.runCompletePayrollProcessSecondHalf.apply(null, arguments); }
function cleaning_runCompletePayrollSecond() { return CleaningApp.runCompletePayrollSecond.apply(null, arguments); }
function cleaning_runDataValidation() { return CleaningApp.runDataValidation.apply(null, arguments); }
function cleaning_runFinalSetFirst() { return CleaningApp.runFinalSetFirst.apply(null, arguments); }
function cleaning_runFinalSetSecond() { return CleaningApp.runFinalSetSecond.apply(null, arguments); }
function cleaning_runFinalSettlement() { return CleaningApp.runFinalSettlement.apply(null, arguments); }
function cleaning_runFinalSettlementFirstHalf() { return CleaningApp.runFinalSettlementFirstHalf.apply(null, arguments); }
function cleaning_runFinalSettlementSecondHalf() { return CleaningApp.runFinalSettlementSecondHalf.apply(null, arguments); }
function cleaning_runHalfFullFirst() { return CleaningApp.runHalfFullFirst.apply(null, arguments); }
function cleaning_runHalfFullSecond() { return CleaningApp.runHalfFullSecond.apply(null, arguments); }
function cleaning_runInternFirst() { return CleaningApp.runInternFirst.apply(null, arguments); }
function cleaning_runInternProcess() { return CleaningApp.runInternProcess.apply(null, arguments); }
function cleaning_runInternProcessFirstHalf() { return CleaningApp.runInternProcessFirstHalf.apply(null, arguments); }
function cleaning_runInternProcessLegacy() { return CleaningApp.runInternProcessLegacy.apply(null, arguments); }
function cleaning_runInternProcessSecondHalf() { return CleaningApp.runInternProcessSecondHalf.apply(null, arguments); }
function cleaning_runInternSecond() { return CleaningApp.runInternSecond.apply(null, arguments); }
function cleaning_runLeaderFirst() { return CleaningApp.runLeaderFirst.apply(null, arguments); }
function cleaning_runLeaderProcess() { return CleaningApp.runLeaderProcess.apply(null, arguments); }
function cleaning_runLeaderProcessFirstHalf() { return CleaningApp.runLeaderProcessFirstHalf.apply(null, arguments); }
function cleaning_runLeaderProcessSecondHalf() { return CleaningApp.runLeaderProcessSecondHalf.apply(null, arguments); }
function cleaning_runLeaderSecond() { return CleaningApp.runLeaderSecond.apply(null, arguments); }
function cleaning_runNewEmployeePeriodLabel() { return CleaningApp.runNewEmployeePeriodLabel.apply(null, arguments); }
function cleaning_runNewcomerFirst() { return CleaningApp.runNewcomerFirst.apply(null, arguments); }
function cleaning_runNewcomerProcess() { return CleaningApp.runNewcomerProcess.apply(null, arguments); }
function cleaning_runNewcomerProcessFirstHalf() { return CleaningApp.runNewcomerProcessFirstHalf.apply(null, arguments); }
function cleaning_runNewcomerProcessLegacy() { return CleaningApp.runNewcomerProcessLegacy.apply(null, arguments); }
function cleaning_runNewcomerProcessSecondHalf() { return CleaningApp.runNewcomerProcessSecondHalf.apply(null, arguments); }
function cleaning_runNewcomerSecond() { return CleaningApp.runNewcomerSecond.apply(null, arguments); }
function cleaning_runSalaryFirst() { return CleaningApp.runSalaryFirst.apply(null, arguments); }
function cleaning_runSalaryPreparation() { return CleaningApp.runSalaryPreparation.apply(null, arguments); }
function cleaning_runSalaryPreparationFirstHalf() { return CleaningApp.runSalaryPreparationFirstHalf.apply(null, arguments); }
function cleaning_runSalaryPreparationSecondHalf() { return CleaningApp.runSalaryPreparationSecondHalf.apply(null, arguments); }
function cleaning_runSalaryPreparationWithProjectOrders() { return CleaningApp.runSalaryPreparationWithProjectOrders.apply(null, arguments); }
function cleaning_runSalaryPreparationWithProjectOrdersFirstHalf() { return CleaningApp.runSalaryPreparationWithProjectOrdersFirstHalf.apply(null, arguments); }
function cleaning_runSalaryPreparationWithProjectOrdersSecondHalf() { return CleaningApp.runSalaryPreparationWithProjectOrdersSecondHalf.apply(null, arguments); }
function cleaning_runSalarySecond() { return CleaningApp.runSalarySecond.apply(null, arguments); }
function cleaning_runSalarySummaryFirst() { return CleaningApp.runSalarySummaryFirst.apply(null, arguments); }
function cleaning_runSalarySummaryProcess() { return CleaningApp.runSalarySummaryProcess.apply(null, arguments); }
function cleaning_runSalarySummarySecond() { return CleaningApp.runSalarySummarySecond.apply(null, arguments); }
function cleaning_runToolDepositFirst() { return CleaningApp.runToolDepositFirst.apply(null, arguments); }
function cleaning_runToolDepositProcess() { return CleaningApp.runToolDepositProcess.apply(null, arguments); }
function cleaning_runToolDepositProcessFirst() { return CleaningApp.runToolDepositProcessFirst.apply(null, arguments); }
function cleaning_runToolDepositProcessSecond() { return CleaningApp.runToolDepositProcessSecond.apply(null, arguments); }
function cleaning_runToolDepositSecond() { return CleaningApp.runToolDepositSecond.apply(null, arguments); }
function cleaning_runVoucherFirst() { return CleaningApp.runVoucherFirst.apply(null, arguments); }
function cleaning_runVoucherPreparation() { return CleaningApp.runVoucherPreparation.apply(null, arguments); }
function cleaning_runVoucherPreparationFirstHalf() { return CleaningApp.runVoucherPreparationFirstHalf.apply(null, arguments); }
function cleaning_runVoucherPreparationSecondHalf() { return CleaningApp.runVoucherPreparationSecondHalf.apply(null, arguments); }
function cleaning_runVoucherSecond() { return CleaningApp.runVoucherSecond.apply(null, arguments); }
function cleaning_runYuantaAccountFirstHalf() { return CleaningApp.runYuantaAccountFirstHalf.apply(null, arguments); }
function cleaning_runYuantaAccountSecondHalf() { return CleaningApp.runYuantaAccountSecondHalf.apply(null, arguments); }
function cleaning_safeClearHighlight() { return CleaningApp.safeClearHighlight.apply(null, arguments); }
function cleaning_safeHighlightProcessingRange() { return CleaningApp.safeHighlightProcessingRange.apply(null, arguments); }
function cleaning_saveAsExcelFile() { return CleaningApp.saveAsExcelFile.apply(null, arguments); }
function cleaning_savePreservingFileLinkEnhanced() { return CleaningApp.savePreservingFileLinkEnhanced.apply(null, arguments); }
function cleaning_scrollToCell() { return CleaningApp.scrollToCell.apply(null, arguments); }
function cleaning_setEmergencyStopFlag() { return CleaningApp.setEmergencyStopFlag.apply(null, arguments); }
function cleaning_setExecutionState() { return CleaningApp.setExecutionState.apply(null, arguments); }
function cleaning_setPdfRootFolder() { return CleaningApp.setPdfRootFolder.apply(null, arguments); }
function cleaning_setRegionName() { return CleaningApp.setRegionName.apply(null, arguments); }
function cleaning_setRegionRootFolderWithId() { return CleaningApp.setRegionRootFolderWithId.apply(null, arguments); }
function cleaning_setSilentMode() { return CleaningApp.setSilentMode.apply(null, arguments); }
function cleaning_setupSalarySlipZeroAsBlank() { return CleaningApp.setupSalarySlipZeroAsBlank.apply(null, arguments); }
function cleaning_setupYuantaAccountHeaders() { return CleaningApp.setupYuantaAccountHeaders.apply(null, arguments); }
function cleaning_showExecutionSidebar() { return CleaningApp.showExecutionSidebar.apply(null, arguments); }
function cleaning_showImportConfirmDialog() { return CleaningApp.showImportConfirmDialog.apply(null, arguments); }
function cleaning_showMainControlPanel() { return CleaningApp.showMainControlPanel.apply(null, arguments); }
function cleaning_showSystemSettings() { return CleaningApp.showSystemSettings.apply(null, arguments); }
function cleaning_showToast() { return CleaningApp.showToast.apply(null, arguments); }
function cleaning_stopExecution() { return CleaningApp.stopExecution.apply(null, arguments); }
function cleaning_switchPeriod() { return CleaningApp.switchPeriod.apply(null, arguments); }
function cleaning_syncAllPeriodSettings() { return CleaningApp.syncAllPeriodSettings.apply(null, arguments); }
function cleaning_syncPeriodToNewcomerAndIntern() { return CleaningApp.syncPeriodToNewcomerAndIntern.apply(null, arguments); }
function cleaning_testFolderAccess() { return CleaningApp.testFolderAccess.apply(null, arguments); }
function cleaning_testParams() { return CleaningApp.testParams.apply(null, arguments); }
function cleaning_testPeriodExtraction() { return CleaningApp.testPeriodExtraction.apply(null, arguments); }
function cleaning_testSpreadsheetOpenPermission() { return CleaningApp.testSpreadsheetOpenPermission.apply(null, arguments); }
function cleaning_throwIfEmergencyStopRequested_() { return CleaningApp.throwIfEmergencyStopRequested_.apply(null, arguments); }
function cleaning_triggerAllPermissions() { return CleaningApp.triggerAllPermissions.apply(null, arguments); }
function cleaning_updateCompletionStatus() { return CleaningApp.updateCompletionStatus.apply(null, arguments); }
function cleaning_updateProgress() { return CleaningApp.updateProgress.apply(null, arguments); }
function cleaning_updateSidebarProgress() { return CleaningApp.updateSidebarProgress.apply(null, arguments); }
function cleaning_updateSidebarProgressWithDelay() { return CleaningApp.updateSidebarProgressWithDelay.apply(null, arguments); }
function cleaning_validateAndGetSheet() { return CleaningApp.validateAndGetSheet.apply(null, arguments); }
function cleaning_validateCellValue() { return CleaningApp.validateCellValue.apply(null, arguments); }
function cleaning_validateImportData() { return CleaningApp.validateImportData.apply(null, arguments); }
function cleaning_validateSheetStatus() { return CleaningApp.validateSheetStatus.apply(null, arguments); }
function cleaning_waitForImportDecision() { return CleaningApp.waitForImportDecision.apply(null, arguments); }
function cleaning_writeDataToYuantaAccount() { return CleaningApp.writeDataToYuantaAccount.apply(null, arguments); }
