"""專案人員的前置檢查及結算名單，共用相同的姓名對應規則。"""
import re
import unicodedata


def _key(value):
    text = unicodedata.normalize('NFKC', str(value or ''))
    return ''.join(c for c in text if not c.isspace() and c not in '\u200b\u200c\u200d\ufeff')


def _present(value):
    text = _key(value)
    if not text:
        return False
    try:
        return float(text.replace(',', '')) != 0
    except ValueError:
        return True


def _is_one(value):
    try:
        number = float(str(value).replace(',', ''))
        return number == 1
    except (ValueError, TypeError):
        return False


def _col(number):
    result = ''
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def project_people(ws, log, *, fill_missing=False):
    """B2 有資料時，驗證 F 姓名對應同列人員欄必須等於 1；前置作業可補值，排除 2001 起統計列。"""
    b2 = ws.get('B2', value_render_option='UNFORMATTED_VALUE') or [[]]
    if not b2[0] or not _present(b2[0][0]):
        log.append('    專案薪資表 B2 為空或 0，無專案人員')
        return []
    headers = ws.row_values(1)[11:]
    columns = {}
    for idx, name in enumerate(headers, 12):
        if _present(name):
            key = _key(name)
            if key in columns:
                raise ValueError(f'專案薪資表人員欄姓名重複：{name}')
            columns[key] = (idx, str(name).strip())
    last_col = _col(max(11 + len(headers), 6))
    rows = ws.get(f'F2:{last_col}2000', value_render_option='UNFORMATTED_VALUE') or []
    eligible = set()
    errors = []
    updates = []
    for row_num, row in enumerate(rows, 2):
        if not row or not _present(row[0]):
            continue
        text = unicodedata.normalize('NFKC', str(row[0])).strip()
        # 單人先完整比對；多人名單接受常見的分隔符號。
        keys = [_key(text)] if _key(text) in columns else [
            _key(part) for part in re.split(r'[,、;/／\n\r\t ]+', text) if _present(part)
        ]
        for key in keys:
            if key not in columns:
                errors.append(f'F{row_num}「{key}」在 L1 起找不到姓名')
                continue
            col, name = columns[key]
            value = row[col - 6] if col - 6 < len(row) else 0
            if not _is_one(value) and fill_missing:
                updates.append({'range': f'{_col(col)}{row_num}', 'values': [[1]]})
            elif not _is_one(value):
                errors.append(f'F{row_num}「{name}」對應 {_col(col)}{row_num} 應為 1，實際值：{value!r}')
            if row_num <= 1000 and (_is_one(value) or fill_missing):
                eligible.add(key)
    if errors:
        raise ValueError('專案薪資表檢查失敗：' + '；'.join(errors[:10]) +
                         (f'（共 {len(errors)} 處）' if len(errors) > 10 else ''))
    if updates:
        for start in range(0, len(updates), 500):
            ws.batch_update(updates[start:start + 500], value_input_option='RAW')
        log.append(f'    專案姓名對應欄已填入 1：{len(updates)} 格')
        return project_people(ws, log)
    names = [name for key, (_, name) in columns.items() if key in eligible]
    log.append(f'    專案姓名對應檢查通過，第 2～1000 列有效人員：{len(names)} 人')
    return names
