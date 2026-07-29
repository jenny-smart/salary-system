function mail_syncServiceFeeMail() {
  var region = CentralContext.getRegion();
  var period = CentralContext.getPeriod();
  var cfg = CentralMaster.getRegionConfig(region);
  if (!cfg.mail_id) throw new Error("「地區設定」缺少 mail_id：" + region);

  var cleaningId = mail_findPeriodFile_(cfg.root_folder_id, period, "清潔承攬", region);
  var otherId = mail_findPeriodFile_(cfg.root_folder_id, period, "其他承攬", region);
  var rows = [];
  if (cleaningId) {
    var cleaning = SpreadsheetApp.openById(cleaningId);
    rows = rows.concat(mail_readPdfPairs_(cleaning, "PDF產出"));
    rows = rows.concat(mail_readPdfPairs_(cleaning, "專案PDF產出"));
  }
  if (otherId) {
    rows = rows.concat(mail_readPdfPairs_(SpreadsheetApp.openById(otherId), "PDF產出"));
  }

  var mailBook = SpreadsheetApp.openById(cfg.mail_id);
  var periodSheet = mail_getOrCreateSheet_(mailBook, period);
  periodSheet.getRange("B2:C" + Math.max(periodSheet.getMaxRows(), 2)).clearContent();
  periodSheet.getRange("B1:C1").setValues([["專員", "PDF連結"]]);
  if (rows.length) periodSheet.getRange(2, 2, rows.length, 2).setValues(rows);

  var mailSheet = mail_getOrCreateSheet_(mailBook, "mail");
  mailSheet.getRange("A1").setValue(cfg.roster_id || "");
  mailSheet.getRange("A2").setFormula(
    '=CHOOSECOLS(IMPORTRANGE(A1,"' + period.slice(0, 6) + '專員名冊!A2:I120"),2,9)'
  );

  if (/-2$/.test(period) && cleaningId) {
    mail_syncDeposit_(mailBook, SpreadsheetApp.openById(cleaningId), period, region);
  }
  CentralMaster.recordExecution("承攬mail", rows.length, period);
  return { success: true, count: rows.length, message: "承攬服務費 mail 已同步 " + rows.length + " 筆" };
}

function mail_findPeriodFile_(rootId, period, label, region) {
  var root = DriveApp.getFolderById(rootId);
  var periodFolders = root.getFoldersByName(period);
  if (!periodFolders.hasNext()) return "";
  var folder = periodFolders.next();
  var name = period + label + "-" + String(region).replace(/區/g, "").trim();
  var files = folder.getFilesByName(name);
  return files.hasNext() ? files.next().getId() : "";
}

function mail_readPdfPairs_(book, title) {
  var sheet = book.getSheetByName(title);
  if (!sheet || sheet.getLastRow() < 2) return [];
  return sheet.getRange(2, 2, sheet.getLastRow() - 1, 4).getDisplayValues()
    .filter(function (row) { return String(row[0] || "").trim(); })
    .map(function (row) { return [String(row[0]).trim(), row[3] || ""]; });
}

function mail_getOrCreateSheet_(book, title) {
  return book.getSheetByName(title) || book.insertSheet(title);
}

function mail_syncDeposit_(mailBook, cleaningBook, period, region) {
  var summary = cleaningBook.getSheetByName("場次時數薪資總表");
  if (!summary) return;
  var values = summary.getRange("A4:AE120").getDisplayValues();
  var countMap = {};
  values.forEach(function (row) {
    if (String(row[0] || "").trim()) countMap[String(row[0]).trim()] = row[1] || "";
  });
  var names = values.map(function (row) { return String(row[30] || "").trim(); })
    .filter(function (name) { return name && name !== "0"; });
  if (!names.length) return;

  var year = Number(period.slice(0, 4));
  var month = Number(period.slice(4, 6));
  var due = new Date(year, month, 10);
  var dueText = Utilities.formatDate(due, "Asia/Taipei", "yyyy/MM/dd");
  var amount = String(region).indexOf("台中") >= 0 ? 1500 : 2000;
  var sheet = mail_getOrCreateSheet_(mailBook, period + "工具包押金");
  sheet.getRange("B2:E" + Math.max(sheet.getMaxRows(), 2)).clearContent();
  sheet.getRange("B1:E1").setValues([["專員", "場次數", "發放日", "金額"]]);
  sheet.getRange(2, 2, names.length, 4).setValues(names.map(function (name) {
    return [name, countMap[name] || "", dueText, amount];
  }));
}
