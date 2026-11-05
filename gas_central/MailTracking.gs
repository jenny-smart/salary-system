/**
 * 承攬費通知信「點連結」追蹤（salaryapp ④ 寄出承攬費通知信產生的連結）。
 *
 * 連結：/exec?action=click&m=承攬服務費mail試算表ID&s=期別&k=追蹤碼
 * 依 J 欄追蹤碼找到該列 → 第一次點擊寫 K 欄時間、狀態由「已寄出」改「已點連結」
 * → 轉到 C 欄原本的承攬服務費連結。只會轉到試算表裡的連結，不接受外部指定網址。
 */
var MAIL_TRACK_COLS = { link: 3, status: 7, statusAt: 8, token: 10, clickedAt: 11 };

function handleMailClick_(e) {
  var p = (e && e.parameter) || {};
  var target = "";
  try {
    var token = String(p.k || "").trim();
    if (!/^[0-9a-f]{16}$/.test(token)) throw new Error("追蹤碼格式錯誤");
    var sheet = SpreadsheetApp.openById(String(p.m || "")).getSheetByName(String(p.s || ""));
    if (!sheet) throw new Error("找不到期別工作表");
    var last = sheet.getLastRow();
    if (last < 2) throw new Error("工作表沒有資料");
    var tokens = sheet.getRange(2, MAIL_TRACK_COLS.token, last - 1, 1).getDisplayValues();
    var row = 0;
    for (var i = 0; i < tokens.length; i++) {
      if (String(tokens[i][0]).trim() === token) { row = i + 2; break; }
    }
    if (!row) throw new Error("找不到追蹤碼");
    target = String(sheet.getRange(row, MAIL_TRACK_COLS.link).getDisplayValue() || "").trim();

    var lock = LockService.getScriptLock();
    lock.waitLock(10000);
    try {
      var clicked = sheet.getRange(row, MAIL_TRACK_COLS.clickedAt);
      if (!String(clicked.getDisplayValue()).trim()) {
        var now = Utilities.formatDate(new Date(), "Asia/Taipei", "yyyy/MM/dd HH:mm:ss");
        clicked.setNumberFormat("@").setValue(now);
        var status = sheet.getRange(row, MAIL_TRACK_COLS.status);
        if (String(status.getDisplayValue()).trim() === "已寄出") {
          status.setValue("已點連結");
          sheet.getRange(row, MAIL_TRACK_COLS.statusAt).setNumberFormat("@").setValue(now);
        }
      }
    } finally {
      lock.releaseLock();
    }
  } catch (error) {
    console.warn("mail click tracking: " + (error && error.message || error));
  }
  return mailClickRedirectPage_(target);
}

function mailClickRedirectPage_(target) {
  var safe = /^https:\/\//.test(target) ? target : "";
  var html = safe
    ? '<base target="_top"><meta name="viewport" content="width=device-width,initial-scale=1">' +
      '<div style="font-family:sans-serif;text-align:center;margin-top:20vh">' +
      '<p>🍋 正在開啟承攬服務費…</p>' +
      '<p><a href="' + mailHtmlEscape_(safe) + '" style="font-size:18px">沒有自動開啟請點這裡</a></p></div>' +
      '<script>try{window.top.location.replace(' + JSON.stringify(safe) + ');}catch(e){}</script>'
    : '<div style="font-family:sans-serif;text-align:center;margin-top:20vh">' +
      '<p>連結已失效，請直接回信告知，我們會重新提供，謝謝。</p></div>';
  return HtmlService.createHtmlOutput(html).setTitle("檸檬家事｜承攬服務費");
}

function mailHtmlEscape_(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
